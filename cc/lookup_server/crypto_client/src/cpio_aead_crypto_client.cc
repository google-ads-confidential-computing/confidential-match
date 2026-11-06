// Copyright 2026 Google LLC
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

#include "cc/lookup_server/crypto_client/src/cpio_aead_crypto_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/utils/key_fetching/proto/cloud_wrapped_key.pb.h"
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
using ::google::cmrt::sdk::v1::CloudWrappedKey;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncPriority;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "CpioAeadCryptoClient";

}  // namespace

ExecutionResult CpioAeadCryptoClient::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult CpioAeadCryptoClient::Run() noexcept {
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

ExecutionResult CpioAeadCryptoClient::Stop() noexcept {
  return SuccessExecutionResult();
}

CloudWrappedKey CpioAeadCryptoClient::BuildCloudWrappedKey(
    const EncryptionKeyInfo& encryption_key_info) noexcept {
  CloudWrappedKey cloud_wrapped_key;
  const EncryptionKeyInfo::WrappedKeyInfo& wrapped_key_info =
      encryption_key_info.wrapped_key_info();
  if (wrapped_key_info.has_gcp_wrapped_key_info()) {
    auto* gcp_key = cloud_wrapped_key.mutable_gcp_wrapped_key();
    gcp_key->set_encrypted_dek(wrapped_key_info.encrypted_dek());
    gcp_key->set_kek_uri(wrapped_key_info.kek_kms_resource_id());
    gcp_key->set_wip_provider(
        wrapped_key_info.gcp_wrapped_key_info().wip_provider());
  } else if (wrapped_key_info.has_aws_wrapped_key_info()) {
    auto* aws_key = cloud_wrapped_key.mutable_aws_wrapped_key();
    aws_key->set_encrypted_dek(wrapped_key_info.encrypted_dek());
    aws_key->set_kek_uri(wrapped_key_info.kek_kms_resource_id());
    aws_key->set_role_arn(wrapped_key_info.aws_wrapped_key_info().role_arn());
  }
  return cloud_wrapped_key;
}

void CpioAeadCryptoClient::GetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  ExecutionResult schedule_result = async_executor_->Schedule(
      [this, key_context]() noexcept { GetCryptoKeyAsync(key_context); },
      AsyncPriority::Normal);
  if (!schedule_result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, key_context, schedule_result,
        "Failed to schedule GetCryptoKey operation on AsyncExecutor.");
    key_context.result = schedule_result;
    key_context.Finish();
  }
}

void CpioAeadCryptoClient::GetCryptoKeyAsync(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  CloudWrappedKey cloud_wrapped_key =
      BuildCloudWrappedKey(*key_context.request);
  ExecutionResultOr<std::string> dek_or;
  if (key_context.request->wrapped_key_info().has_aws_wrapped_key_info()) {
    dek_or = aws_wrapped_key_handler_->GetKey(cloud_wrapped_key);
  } else if (key_context.request->wrapped_key_info()
                 .has_gcp_wrapped_key_info()) {
    dek_or = gcp_wrapped_key_handler_->GetKey(cloud_wrapped_key);
  } else {
    ExecutionResult result =
        FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR);
    SCP_ERROR_CONTEXT(
        kComponentName, key_context, result,
        "WrappedKeyInfo must have either AWS or GCP wrapped key info.");
    key_context.result = result;
    key_context.Finish();
    return;
  }

  if (!dek_or.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, key_context, dek_or.result(),
        "Unable to decrypt the wrapped DEK using the provided KMS.");
    key_context.result = FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR);
    key_context.Finish();
    return;
  }

  StatusOr<std::unique_ptr<KeysetReader>> keyset_reader_or =
      BinaryKeysetReader::New(*dek_or);
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

