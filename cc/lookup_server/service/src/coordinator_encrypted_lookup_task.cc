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

#include "cc/lookup_server/service/src/coordinator_encrypted_lookup_task.h"

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"

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

using SerializableDataRecords = ::google::confidential_match::lookup_server::
    proto_api::LookupRequest::SerializableDataRecords;

constexpr absl::string_view kComponentName = "CoordinatorEncryptedLookupTask";

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

ExecutionResultOr<SerializableDataRecords> DecryptSerializedDataRecords(
    absl::string_view serialized_records,
    const CryptoKeyInterface& crypto_key) {
  std::string encrypted_bytes;
  if (!absl::Base64Unescape(serialized_records, &encrypted_bytes)) {
    ExecutionResult result =
        FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Failed to base-64 decode the encrypted data records.");
    return result;
  }

  ExecutionResultOr<std::string> decrypted_bytes_or =
      crypto_key.Decrypt(encrypted_bytes);
  if (!decrypted_bytes_or.Successful()) {
    SCP_ERROR(
        kComponentName, kZeroUuid, decrypted_bytes_or.result(),
        "Failed to decrypt the encrypted data records with the provided DEK.");
    return decrypted_bytes_or.result();
  }

  SerializableDataRecords serializable_data_records;
  serializable_data_records.ParseFromString(*decrypted_bytes_or);
  return serializable_data_records;
}

}  // namespace

void CoordinatorEncryptedLookupTask::HandleRequest(
    AsyncContext<LookupRequest, LookupResponse> context) noexcept {
  // Fetch the crypto key.
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> get_crypto_key_context;
  get_crypto_key_context.request = std::make_shared<EncryptionKeyInfo>();
  ExecutionResult conversion_result = ConvertEncryptionKeyInfo(
      context.request->encryption_key_info(), *get_crypto_key_context.request);
  if (!conversion_result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, context, conversion_result,
                      "Unable to convert EncryptionKeyInfo to backend proto.");
    context.result = get_crypto_key_context.result;
    context.Finish();
    return;
  }
  get_crypto_key_context.callback =
      std::bind(&CoordinatorEncryptedLookupTask::OnGetCryptoKeyCallback, this,
                _1, context);
  hpke_crypto_client_->GetCryptoKey(get_crypto_key_context);
}

void CoordinatorEncryptedLookupTask::OnGetCryptoKeyCallback(
    const AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>&
        get_crypto_key_context,
    AsyncContext<LookupRequest, LookupResponse> lookup_context) noexcept {
  if (!get_crypto_key_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, lookup_context, get_crypto_key_context.result,
        "Unable to construct the crypto key from request parameters.");
    lookup_context.result = get_crypto_key_context.result;
    lookup_context.Finish();
    return;
  }
  const auto& crypto_key = get_crypto_key_context.response;

  ExecutionResultOr<LookupResponse> response_or;
  if (!lookup_context.request->encrypted_data_records().empty()) {
    response_or = Match(*lookup_context.request, *crypto_key);
  } else {
    // TODO(b/364942350): Remove support for individually encrypted keys after
    // MRP adds support for batch-encrypted requests.
    response_or =
        MatchIndividuallyEncryptedKeys(*lookup_context.request, *crypto_key);
  }
  if (!response_or.Successful()) {
    lookup_context.result = response_or.result();
    lookup_context.Finish();
    return;
  }

  lookup_context.response =
      std::make_shared<LookupResponse>(std::move(*response_or));
  lookup_context.result = SuccessExecutionResult();
  lookup_context.Finish();
}

ExecutionResultOr<LookupResponse> CoordinatorEncryptedLookupTask::Match(
    const LookupRequest& request,
    const CryptoKeyInterface& crypto_key) noexcept {
  LookupResponse response;

  ExecutionResultOr<SerializableDataRecords> decrypted_records_or =
      DecryptSerializedDataRecords(request.encrypted_data_records(),
                                   crypto_key);
  if (!decrypted_records_or.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, decrypted_records_or.result(),
              "Failed to parse encrypted data records in request.");
    return decrypted_records_or.result();
  }

  for (const auto& record : decrypted_records_or->data_records()) {
    ExecutionResultOr<std::vector<MatchDataRow>> matches_or =
        match_data_storage_->Get(record.lookup_key().key());
    if (!matches_or.Successful()) {
      SCP_ERROR(kComponentName, kZeroUuid, matches_or.result(),
                "Failed to look up key from match data storage.");
      return matches_or.result();
    }

    // Since we return unencrypted hashes, don't add a lookup result in
    // the response if there are no matches.
    if (matches_or->size() == 0) {
      continue;
    }

    LookupResult* lookup_result = response.add_lookup_results();
    *lookup_result->mutable_client_data_record() = record;
    lookup_result->set_status(LookupResult::STATUS_SUCCESS);
    for (const auto& match_data_row : *matches_or) {
      RETURN_IF_FAILURE(ConvertToMatchedDataRecord(
          match_data_row, request.associated_data_keys(),
          *lookup_result->add_matched_data_records()));
    }
  }

  return response;
}

ExecutionResultOr<LookupResponse>
CoordinatorEncryptedLookupTask::MatchIndividuallyEncryptedKeys(
    const LookupRequest& request,
    const CryptoKeyInterface& crypto_key) noexcept {
  LookupResponse response;

  for (const auto& record : request.data_records()) {
    LookupResult* lookup_result = response.add_lookup_results();
    *lookup_result->mutable_client_data_record() = record;

    absl::string_view encrypted_key = record.lookup_key().key();
    ExecutionResultOr<std::string> decrypted_key_or =
        DecryptHashKey(encrypted_key, crypto_key);
    if (!decrypted_key_or.Successful()) {
      lookup_result->set_status(LookupResult::STATUS_FAILED);
      ASSIGN_OR_LOG_AND_RETURN(
          ErrorResponse public_error_response,
          BuildPublicErrorResponse(decrypted_key_or.result()), kComponentName,
          kZeroUuid, "Failed to generate public-facing error response.");
      *lookup_result->mutable_error_response() =
          std::move(public_error_response);
      continue;
    }

    ExecutionResultOr<std::vector<MatchDataRow>> matches_or =
        match_data_storage_->Get(*decrypted_key_or);
    if (!matches_or.Successful()) {
      lookup_result->set_status(LookupResult::STATUS_FAILED);
      ASSIGN_OR_LOG_AND_RETURN(
          ErrorResponse public_error_response,
          BuildPublicErrorResponse(matches_or.result()), kComponentName,
          kZeroUuid, "Failed to generate public-facing error response.");
      *lookup_result->mutable_error_response() =
          std::move(public_error_response);
      continue;
    }

    lookup_result->set_status(LookupResult::STATUS_SUCCESS);
    for (MatchDataRow& match_data_row : *matches_or) {
      // Overwrite the decrypted key with the encrypted key in the response
      match_data_row.set_key(encrypted_key);
      ExecutionResult convert_result = ConvertToMatchedDataRecord(
          match_data_row, request.associated_data_keys(),
          *lookup_result->add_matched_data_records());
      if (!convert_result.Successful()) {
        SCP_ERROR(
            kComponentName, kZeroUuid, convert_result,
            "Unable to convert matches to a matched data record response.");
        return convert_result;
      }
    }
  }

  return response;
}

}  // namespace google::confidential_match::lookup_server
