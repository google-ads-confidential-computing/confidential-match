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

#include "cc/match_service/crypto_client/aead_crypto_client.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "tink/aead.h"
#include "tink/aead/aead_config.h"
#include "tink/binary_keyset_reader.h"
#include "tink/cleartext_keyset_handle.h"
#include "tink/keyset_handle.h"
#include "tink/util/status.h"
#include "tink/util/statusor.h"

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
using ::crypto::tink::BinaryKeysetReader;
using ::crypto::tink::CleartextKeysetHandle;
using ::crypto::tink::KeysetHandle;
using ::crypto::tink::KeysetReader;
using ::google::cmrt::sdk::kms_service::v1::DecryptRequest;
using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::Error;
using TinkStatus = ::crypto::tink::util::Status;
template <typename T>
using TinkStatusOr = ::crypto::tink::util::StatusOr<T>;

constexpr absl::string_view kAwsKmsResourcePrefix = "aws-kms://";
constexpr absl::string_view kGcpKmsResourcePrefix = "gcp-kms://";
constexpr absl::string_view kAwsResourceDelimiter = ":";

}  // namespace

absl::Status AeadCryptoClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status AeadCryptoClient::Run() noexcept {
  TinkStatus register_result = AeadConfig::Register();
  if (!register_result.ok()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to initialize aead config: ",
                               register_result.ToString()));
  }
  return absl::OkStatus();
}

absl::Status AeadCryptoClient::Stop() noexcept {
  return absl::OkStatus();
}

DecryptRequest AeadCryptoClient::BuildKmsDecryptRequest(
    const EncryptionKeyInfo& encryption_key_info) noexcept {
  DecryptRequest decrypt_request;
  const auto& wrapped_key_info = encryption_key_info.wrapped_key_info();
  decrypt_request.set_ciphertext(wrapped_key_info.encrypted_dek());
  decrypt_request.set_key_resource_name(wrapped_key_info.kek_kms_resource_id());

  // Normalize prefix
  if (absl::StartsWith(wrapped_key_info.kek_kms_resource_id(),
                       kGcpKmsResourcePrefix)) {
    decrypt_request.set_key_resource_name(std::string(absl::StripPrefix(
        wrapped_key_info.kek_kms_resource_id(), kGcpKmsResourcePrefix)));
  } else if (absl::StartsWith(wrapped_key_info.kek_kms_resource_id(),
                              kAwsKmsResourcePrefix)) {
    decrypt_request.set_key_resource_name(std::string(absl::StripPrefix(
        wrapped_key_info.kek_kms_resource_id(), kAwsKmsResourcePrefix)));
  }

  if (wrapped_key_info.has_gcp_wrapped_key_info()) {
    decrypt_request.set_gcp_wip_provider(
        wrapped_key_info.gcp_wrapped_key_info().wip_provider());
  } else if (wrapped_key_info.has_aws_wrapped_key_info()) {
    const auto& aws_key_info = wrapped_key_info.aws_wrapped_key_info();
    decrypt_request.set_account_identity(aws_key_info.role_arn());
    decrypt_request.set_target_audience_for_web_identity(
        aws_kms_default_audience_);

    // Must include region
    std::vector<absl::string_view> resource_parts = absl::StrSplit(
        decrypt_request.key_resource_name(), kAwsResourceDelimiter);
    // Region is in the 3rd index. Eg: `arn:aws:kms:<region>:<###>`
    if (resource_parts.size() > 3) {
      decrypt_request.set_kms_region(std::string(resource_parts[3]));
    }

    const auto& decrypt_sigs = decrypt_request.mutable_key_ids();
    decrypt_sigs->Reserve(static_cast<int>(kms_default_signatures_.size()) +
                          aws_key_info.signatures_size());
    decrypt_sigs->Add(kms_default_signatures_.cbegin(),
                      kms_default_signatures_.cend());
    decrypt_sigs->Add(aws_key_info.signatures().cbegin(),
                      aws_key_info.signatures().cend());
  }
  return decrypt_request;
}

void AeadCryptoClient::GetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  auto decrypt_context =
      key_context.CreateAsyncContext<DecryptRequest, std::string>();
  decrypt_context.request = std::make_shared<DecryptRequest>(
      BuildKmsDecryptRequest(*key_context.request));
  decrypt_context.callback = absl::bind_front(
      &AeadCryptoClient::OnDecryptWrappedKmsKeyCallback, this, key_context);

  if (key_context.request->wrapped_key_info().has_aws_wrapped_key_info()) {
    aws_kms_client_.Decrypt(decrypt_context);
  } else {
    gcp_kms_client_.Decrypt(decrypt_context);
  }
}

void AeadCryptoClient::OnDecryptWrappedKmsKeyCallback(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context,
    AsyncContext<DecryptRequest, std::string> kms_decrypt_context) noexcept {
  if (!kms_decrypt_context.status.ok()) {
    key_context.status = Annotate(kms_decrypt_context.status,
                                  "Received error from KMS client when trying "
                                  "to decrypt the wrapped DEK.");
    LOG_ERROR(*key_context.logger, "%v", key_context.status);
    key_context.Finish();
    return;
  }

  auto aead_or = GetTinkPrimitive<Aead>(*kms_decrypt_context.response);
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
