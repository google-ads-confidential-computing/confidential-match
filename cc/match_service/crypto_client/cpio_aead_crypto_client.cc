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

#include "cc/match_service/crypto_client/cpio_aead_crypto_client.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "cc/core/interface/errors.h"
#include "cc/public/cpio/interface/error_codes.h"
#include "cc/public/cpio/utils/key_fetching/proto/cloud_wrapped_key.pb.h"
#include "cc/public/cpio/utils/key_fetching/src/error_codes.h"
#include "tink/aead.h"
#include "tink/aead/aead_config.h"
#include "tink/util/status.h"

#include "cc/core/async/async_context.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/crypto_client/aead_crypto_key.h"
#include "cc/match_service/crypto_client/tink_utils.h"
#include "cc/match_service/error/error.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::crypto::tink::Aead;
using ::crypto::tink::AeadConfig;
using ::google::cmrt::sdk::v1::CloudWrappedKey;
using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::errors::GetErrorMessage;
using TinkStatus = ::crypto::tink::util::Status;

// Helper to build a CloudWrappedKey proto from EncryptionKeyInfo.
CloudWrappedKey BuildCloudWrappedKey(
    const EncryptionKeyInfo& encryption_key_info) noexcept {
  CloudWrappedKey cloud_wrapped_key;
  const auto& wrapped_key_info = encryption_key_info.wrapped_key_info();
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

// Maps the CPIO error code to the backend error reason for wrapped keys.
Error::Reason MapWrappedKeyFetchingError(uint64_t cpio_error_code) {
  if (cpio_error_code == scp::core::errors::SC_CPIO_INVALID_CREDENTIALS) {
    return Error::CUSTOMER_KEY_PERMISSION_DENIED;
  }
  if (cpio_error_code == scp::core::errors::SC_CPIO_REQUEST_LIMIT_REACHED) {
    return Error::CUSTOMER_QUOTA_EXCEEDED;
  }
  if (cpio_error_code == scp::core::errors::SC_CPIO_INTERNAL_ERROR ||
      cpio_error_code == scp::core::errors::SC_CPIO_INVALID_ARGUMENT ||
      cpio_error_code == scp::core::errors::SC_CPIO_ENTITY_NOT_FOUND ||
      cpio_error_code == scp::core::errors::SC_CPIO_KEY_NOT_FOUND) {
    return Error::INVALID_DEK;
  }
  return Error::WRAPPED_KEY_FETCHING_ERROR;
}

}  // namespace

absl::Status CpioAeadCryptoClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status CpioAeadCryptoClient::Run() noexcept {
  TinkStatus register_result = AeadConfig::Register();
  if (!register_result.ok()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to initialize aead config: ",
                               register_result.ToString()));
  }
  return absl::OkStatus();
}

absl::Status CpioAeadCryptoClient::Stop() noexcept {
  return absl::OkStatus();
}

void CpioAeadCryptoClient::GetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  CloudWrappedKey cloud_wrapped_key =
      BuildCloudWrappedKey(*key_context.request);

  ExecutionResultOr<std::string> dek_or;
  if (key_context.request->wrapped_key_info().has_aws_wrapped_key_info()) {
    dek_or = aws_wrapped_key_handler_.GetKey(cloud_wrapped_key);
  } else {
    dek_or = gcp_wrapped_key_handler_.GetKey(cloud_wrapped_key);
  }

  if (!dek_or.Successful()) {
    key_context.status = Status(
        MapWrappedKeyFetchingError(dek_or.result().status_code),
        absl::StrCat(
            "Received error from wrapped key handler when trying to decrypt "
            "the wrapped DEK: ",
            GetErrorMessage(dek_or.result().status_code)));
    LOG_ERROR(*key_context.logger, "%v", key_context.status);
    key_context.Finish();
    return;
  }

  auto aead_or = GetTinkPrimitive<Aead>(*dek_or);
  if (!aead_or.ok()) {
    key_context.status = Status(Error::INVALID_DEK, aead_or.status().message());
    LOG_ERROR(*key_context.logger, "%v", key_context.status);
    key_context.Finish();
    return;
  }
  key_context.response = std::make_shared<AeadCryptoKey>(std::move(*aead_or));
  key_context.Finish();
}

}  // namespace google::confidential_match::match_service

