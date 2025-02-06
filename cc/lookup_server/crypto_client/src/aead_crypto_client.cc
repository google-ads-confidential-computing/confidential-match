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

#include "cc/lookup_server/crypto_client/src/aead_crypto_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "tink/aead.h"
#include "tink/aead/aead_config.h"
#include "tink/binary_keyset_reader.h"
#include "tink/cleartext_keyset_handle.h"
#include "tink/keyset_handle.h"
#include "tink/util/status.h"
#include "tink/util/statusor.h"

#include "cc/lookup_server/crypto_client/src/aead_crypto_key.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::crypto::tink::Aead;
using ::crypto::tink::AeadConfig;
using ::crypto::tink::BinaryKeysetReader;
using ::crypto::tink::CleartextKeysetHandle;
using ::crypto::tink::KeysetHandle;
using ::crypto::tink::KeysetReader;
using ::crypto::tink::util::Status;
using ::crypto::tink::util::StatusOr;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::std::placeholders::_1;

constexpr absl::string_view kComponentName = "AeadCryptoClient";

// Helper to build a request for decrypting a wrapped KMS key.
DecryptRequest BuildKmsDecryptRequest(
    const EncryptionKeyInfo& encryption_key_info) noexcept {
  DecryptRequest decrypt_request;
  const EncryptionKeyInfo::WrappedKeyInfo& wrapped_key_info =
      encryption_key_info.wrapped_key_info();
  decrypt_request.set_ciphertext(wrapped_key_info.encrypted_dek());
  decrypt_request.set_key_resource_name(wrapped_key_info.kek_kms_resource_id());
  decrypt_request.set_gcp_wip_provider(
      wrapped_key_info.gcp_wrapped_key_info().wip_provider());
  decrypt_request.set_account_identity(
      wrapped_key_info.gcp_wrapped_key_info().service_account_to_impersonate());
  return decrypt_request;
}

}  // namespace

ExecutionResult AeadCryptoClient::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult AeadCryptoClient::Run() noexcept {
  Status register_result = AeadConfig::Register();
  if (!register_result.ok()) {
    auto result = FailureExecutionResult(AEAD_CONFIG_REGISTER_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrCat("Failed to initialize aead config: ",
                           register_result.ToString()));
    return result;
  }
  return SuccessExecutionResult();
}

ExecutionResult AeadCryptoClient::Stop() noexcept {
  return SuccessExecutionResult();
}

void AeadCryptoClient::GetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  AsyncContext<DecryptRequest, std::string> decrypt_context;
  decrypt_context.request = std::make_shared<DecryptRequest>(
      BuildKmsDecryptRequest(*key_context.request));
  decrypt_context.callback = std::bind(
      &AeadCryptoClient::OnDecryptWrappedKmsKeyCallback, this, _1, key_context);
  ExecutionResult start_decrypt_result = kms_client_->Decrypt(decrypt_context);
  if (!start_decrypt_result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, key_context, start_decrypt_result,
                      "Failed to start the KMS decryption process.");
    key_context.result = start_decrypt_result;
    key_context.Finish();
    return;
  }
}

void AeadCryptoClient::OnDecryptWrappedKmsKeyCallback(
    const AsyncContext<DecryptRequest, std::string>& kms_decrypt_context,
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  if (!kms_decrypt_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, kms_decrypt_context, kms_decrypt_context.result,
        "Unable to decrypt the wrapped DEK using the provided KMS.");
    key_context.result = kms_decrypt_context.result;
    key_context.Finish();
    return;
  }

  StatusOr<std::unique_ptr<KeysetReader>> keyset_reader_or =
      BinaryKeysetReader::New(*kms_decrypt_context.response);
  if (!keyset_reader_or.ok()) {
    auto result = FailureExecutionResult(CRYPTO_CLIENT_KEYSET_READ_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrCat("Failed to read keyset from binary: ",
                           keyset_reader_or.status().ToString()));
    key_context.result = result;
    key_context.Finish();
    return;
  }

  StatusOr<std::unique_ptr<KeysetHandle>> keyset_handle_or =
      CleartextKeysetHandle::Read(std::move(*keyset_reader_or));
  if (!keyset_handle_or.ok()) {
    auto result = FailureExecutionResult(CRYPTO_CLIENT_KEYSET_READ_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrCat("Failed to read keyset handler from keyset reader: ",
                           keyset_handle_or.status().ToString()));
    key_context.result = result;
    key_context.Finish();
    return;
  }

  // Get the primitive.
  StatusOr<std::unique_ptr<Aead>> aead_or =
      (*keyset_handle_or)->GetPrimitive<Aead>();
  if (!aead_or.ok()) {
    auto result = FailureExecutionResult(CRYPTO_CLIENT_GET_AEAD_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrCat("Failed to get AEAD from Keyset Handler: ",
                           aead_or.status().ToString()));
    key_context.result = result;
    key_context.Finish();
    return;
  }

  key_context.result = SuccessExecutionResult();
  key_context.response = std::make_shared<AeadCryptoKey>(std::move(*aead_or));
  key_context.Finish();
}

}  // namespace google::confidential_match::lookup_server
