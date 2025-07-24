// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cc/lookup_server/crypto_client/src/hpke_crypto_client.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"
#include "cc/public/cpio/proto/crypto_service/v1/crypto_service.pb.h"

#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/crypto_client/src/hpke_crypto_key.h"
#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::crypto_service::v1::HpkeDecryptRequest;
using ::google::cmrt::sdk::crypto_service::v1::HpkeDecryptResponse;
using ::google::cmrt::sdk::crypto_service::v1::HpkeEncryptRequest;
using ::google::cmrt::sdk::crypto_service::v1::HpkeEncryptResponse;
using ::google::cmrt::sdk::crypto_service::v1::HpkeParams;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::confidential_match::lookup_server::proto_backend::HybridKey;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::std::placeholders::_1;

constexpr absl::string_view kComponentName = "HpkeCryptoClient";

// Helper to build a hybrid key request.
GetHybridKeyRequest BuildGetHybridKeyRequest(
    const EncryptionKeyInfo::CoordinatorKeyInfo& key_info) noexcept {
  GetHybridKeyRequest request;
  request.set_key_id(key_info.key_id());
  for (const auto& info : key_info.coordinator_info()) {
    GetHybridKeyRequest::Coordinator* coordinator = request.add_coordinators();
    coordinator->set_key_service_endpoint(info.key_service_endpoint());
    coordinator->set_account_identity(info.kms_identity());
    coordinator->set_kms_wip_provider(info.kms_wip_provider());
    coordinator->set_key_service_audience_url(info.key_service_audience_url());
  }
  return request;
}

// Sets default values for HpkeParams, which is required for raw keys.
// TODO(b/364699897): Clean up after SCP adds support for returning the Tink
// binary key from PrivateKeyClient.
void SetDefaultHpkeParams(HpkeParams& hpke_params) {
  hpke_params.set_kem(
      cmrt::sdk::crypto_service::v1::HpkeKem::DHKEM_X25519_HKDF_SHA256);
  hpke_params.set_kdf(cmrt::sdk::crypto_service::v1::HpkeKdf::HKDF_SHA256);
  hpke_params.set_aead(
      cmrt::sdk::crypto_service::v1::HpkeAead::CHACHA20_POLY1305);
}

}  // namespace

std::shared_ptr<HpkeCryptoClient> HpkeCryptoClient::Create(
    std::shared_ptr<CoordinatorClientInterface> coordinator_client) noexcept {
  return std::shared_ptr<HpkeCryptoClient>(new HpkeCryptoClient(
      std::move(coordinator_client), scp::cpio::CryptoClientFactory::Create(
                                         scp::cpio::CryptoClientOptions())));
}

HpkeCryptoClient::HpkeCryptoClient(
    std::shared_ptr<CoordinatorClientInterface> coordinator_client,
    std::shared_ptr<scp::cpio::CryptoClientInterface> cpio_crypto_client)
    : coordinator_client_(std::move(coordinator_client)),
      cpio_crypto_client_(std::move(cpio_crypto_client)),
      is_running_(false) {}

ExecutionResult HpkeCryptoClient::Init() noexcept {
  return cpio_crypto_client_->Init();
}

ExecutionResult HpkeCryptoClient::Run() noexcept {
  RETURN_IF_FAILURE(cpio_crypto_client_->Run());
  is_running_ = true;
  return SuccessExecutionResult();
}

ExecutionResult HpkeCryptoClient::Stop() noexcept {
  RETURN_IF_FAILURE(cpio_crypto_client_->Stop());
  is_running_ = false;
  return SuccessExecutionResult();
}

void HpkeCryptoClient::GetCryptoKey(
    AsyncContext<proto_backend::EncryptionKeyInfo, CryptoKeyInterface>
        key_context) noexcept {
  if (!is_running_) {
    auto result = FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Unable to get crypto key when crypto client is not running.");
    key_context.result = result;
    key_context.Finish();
    return;
  }

  // Fetch the hybrid key from one or more coordinators.
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
      get_hybrid_key_context;
  get_hybrid_key_context.request = std::make_shared<GetHybridKeyRequest>(
      BuildGetHybridKeyRequest(key_context.request->coordinator_key_info()));
  get_hybrid_key_context.callback = std::bind(
      &HpkeCryptoClient::OnGetHybridKeyCallback, this, _1, key_context);
  coordinator_client_->GetHybridKey(get_hybrid_key_context);
}

void HpkeCryptoClient::OnGetHybridKeyCallback(
    const AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>&
        coordinator_context,
    AsyncContext<proto_backend::EncryptionKeyInfo, CryptoKeyInterface>
        key_context) noexcept {
  if (!coordinator_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, coordinator_context, coordinator_context.result,
        "Unable to fetch hybrid key with the provided encryption key info.");
    key_context.result = coordinator_context.result;
    key_context.Finish();
    return;
  }

  key_context.result = SuccessExecutionResult();
  key_context.response = std::make_shared<HpkeCryptoKey>(
      coordinator_context.response->hybrid_key().public_key(),
      coordinator_context.response->hybrid_key().private_key(),
      shared_from_this());
  key_context.Finish();
}

ExecutionResultOr<std::string> HpkeCryptoClient::Encrypt(
    absl::string_view plaintext, absl::string_view public_key) noexcept {
  if (!is_running_) {
    return FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR);
  }

  HpkeEncryptRequest request;
  request.mutable_raw_key_with_params()->set_raw_key(public_key);
  SetDefaultHpkeParams(
      *request.mutable_raw_key_with_params()->mutable_hpke_params());
  request.set_payload(plaintext);

  ExecutionResultOr<HpkeEncryptResponse> response_or =
      cpio_crypto_client_->HpkeEncryptSync(request);
  if (!response_or.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, response_or.result(),
              "Failed to perform CPIO HPKE encryption.");
    return FailureExecutionResult(CRYPTO_CLIENT_ENCRYPT_ERROR);
  }

  return response_or->encrypted_data().ciphertext();
}

ExecutionResultOr<std::string> HpkeCryptoClient::Decrypt(
    absl::string_view ciphertext, absl::string_view private_key) noexcept {
  if (!is_running_) {
    return FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR);
  }

  HpkeDecryptRequest request;
  request.set_tink_key_binary(private_key);
  request.mutable_encrypted_data()->set_ciphertext(ciphertext);

  ExecutionResultOr<HpkeDecryptResponse> response_or =
      cpio_crypto_client_->HpkeDecryptSync(request);
  if (!response_or.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, response_or.result(),
              "Failed to perform CPIO HPKE decryption.");
    return FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR);
  }

  return response_or->payload();
}

}  // namespace google::confidential_match::lookup_server
