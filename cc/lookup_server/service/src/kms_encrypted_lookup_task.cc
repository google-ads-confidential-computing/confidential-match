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

#include "cc/lookup_server/service/src/kms_encrypted_lookup_task.h"

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/converters/src/encryption_key_info_converter.h"
#include "cc/lookup_server/converters/src/matched_data_record_converter.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/service/src/error_codes.h"
#include "cc/lookup_server/service/src/public_error_response_functions.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::confidential_match::shared::api_errors::ErrorResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::std::placeholders::_1;

using WrappedKeyInfo = ::google::confidential_match::lookup_server::proto_api::
    EncryptionKeyInfo::WrappedKeyInfo;

constexpr absl::string_view kComponentName = "KmsEncryptedLookupTask";

// Helper to check if whether an encryption key type is supported.
bool IsValidWrappedKeyType(WrappedKeyInfo::KeyType key_type) {
  return key_type == WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305;
}

// Helper to decrypt an encrypted hash key.
ExecutionResultOr<std::string> DecryptHashKey(
    absl::string_view encrypted_hash_key,
    const CryptoKeyInterface& crypto_key) noexcept {
  std::string encrypted_key_bytes;
  if (!absl::Base64Unescape(encrypted_hash_key, &encrypted_key_bytes)) {
    ExecutionResult result =
        FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Failed to base-64 decode the encrypted hash key.");
    return result;
  }

  ExecutionResultOr<std::string> decrypted_key_or =
      crypto_key.Decrypt(encrypted_key_bytes);
  if (!decrypted_key_or.Successful()) {
    SCP_ERROR(
        kComponentName, kZeroUuid, decrypted_key_or.result(),
        "Failed to decrypt the encrypted hash key with the provided DEK.");
    return decrypted_key_or.result();
  }

  return decrypted_key_or;
}

}  // namespace

void KmsEncryptedLookupTask::HandleRequest(
    AsyncContext<LookupRequest, LookupResponse> context) noexcept {
  if (!IsValidWrappedKeyType(context.request->encryption_key_info()
                                 .wrapped_key_info()
                                 .key_type())) {
    context.result = FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST);
    SCP_ERROR_CONTEXT(
        kComponentName, context, context.result,
        "The encrypted lookup request contains an invalid key type.");
    context.Finish();
    return;
  }

  auto encryption_key_info = std::make_shared<EncryptionKeyInfo>();
  ExecutionResult convert_result = ConvertEncryptionKeyInfo(
      context.request->encryption_key_info(), *encryption_key_info);
  if (!convert_result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, context, convert_result,
                      "Failed to convert EncryptionKeyInfo to backend proto.");
    context.result = convert_result;
    context.Finish();
    return;
  }
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> decrypt_context;
  decrypt_context.request = encryption_key_info;
  decrypt_context.callback = std::bind(
      &KmsEncryptedLookupTask::OnGetCryptoKeyCallback, this, _1, context);
  aead_crypto_client_->GetCryptoKey(decrypt_context);
}

void KmsEncryptedLookupTask::OnGetCryptoKeyCallback(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> decrypt_context,
    AsyncContext<LookupRequest, LookupResponse> lookup_context) noexcept {
  if (!decrypt_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, decrypt_context, decrypt_context.result,
        "Unable to construct the crypto key from request parameters.");
    lookup_context.result = decrypt_context.result;
    lookup_context.Finish();
    return;
  }

  auto response = std::make_shared<LookupResponse>();
  for (const auto& record : lookup_context.request->data_records()) {
    LookupResult* lookup_result = response->add_lookup_results();
    *lookup_result->mutable_client_data_record() = record;

    absl::string_view encrypted_key = record.lookup_key().key();
    ExecutionResultOr<std::string> decrypted_key_or =
        DecryptHashKey(encrypted_key, *decrypt_context.response);
    if (!decrypted_key_or.Successful()) {
      ExecutionResultOr<ErrorResponse> public_error_response_or =
          BuildPublicErrorResponse(decrypted_key_or.result());
      if (!public_error_response_or.Successful()) {
        SCP_ERROR_CONTEXT(kComponentName, lookup_context,
                          public_error_response_or.result(),
                          "Failed to generate public-facing error response.");
        lookup_context.result = public_error_response_or.result();
        lookup_context.Finish();
        return;
      }
      lookup_result->set_status(LookupResult::STATUS_FAILED);
      *lookup_result->mutable_error_response() =
          std::move(*public_error_response_or);
      continue;
    }

    ExecutionResultOr<std::vector<MatchDataRow>> matches_or =
        match_data_storage_->Get(*decrypted_key_or);
    if (!matches_or.Successful()) {
      ExecutionResultOr<ErrorResponse> public_error_response_or =
          BuildPublicErrorResponse(matches_or.result());
      if (!public_error_response_or.Successful()) {
        SCP_ERROR_CONTEXT(kComponentName, lookup_context,
                          public_error_response_or.result(),
                          "Failed to generate public-facing error response.");
        lookup_context.result = public_error_response_or.result();
        lookup_context.Finish();
        return;
      }
      lookup_result->set_status(LookupResult::STATUS_FAILED);
      *lookup_result->mutable_error_response() =
          std::move(*public_error_response_or);
      continue;
    }

    lookup_result->set_status(LookupResult::STATUS_SUCCESS);
    for (MatchDataRow& match_data_row : *matches_or) {
      // Overwrite the decrypted key with the encrypted key in the response
      match_data_row.set_key(encrypted_key);
      ExecutionResult convert_result = ConvertToMatchedDataRecord(
          match_data_row, lookup_context.request->associated_data_keys(),
          *lookup_result->add_matched_data_records());
      if (!convert_result.Successful()) {
        SCP_ERROR_CONTEXT(
            kComponentName, lookup_context, convert_result,
            "Unable to convert matches to a matched data record response.");
        lookup_context.result = convert_result;
        lookup_context.Finish();
        return;
      }
    }
  }

  lookup_context.result = SuccessExecutionResult();
  lookup_context.response = response;
  lookup_context.Finish();
}

}  // namespace google::confidential_match::lookup_server
