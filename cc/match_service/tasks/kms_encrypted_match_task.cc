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

#include "cc/match_service/tasks/kms_encrypted_match_task.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/converters/encryption_key_converters.h"
#include "cc/match_service/converters/lookup_associated_data_converter.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/metrics/metrics_util.h"
#include "cc/match_service/tasks/associated_data_collector.h"
#include "cc/match_service/tasks/normalization_utils.h"
#include "cc/match_service/validators/match_key_encoding_validator.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

namespace {

using ::google::cmrt::sdk::metric_service::v1::Metric;
using ::google::cmrt::sdk::metric_service::v1::MetricType;
using ::google::cmrt::sdk::metric_service::v1::MetricUnit;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsRequest;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsResponse;
using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::AssociatedData;
using ::google::confidential_match::match_service::backend::AssociatedDataType;
using ::google::confidential_match::match_service::backend::CompositeField;
using ::google::confidential_match::match_service::backend::CompositeFieldType;
using ::google::confidential_match::match_service::backend::DataRecord;
using ::google::confidential_match::match_service::backend::EncryptionKey;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::ErrorReason;
using ::google::confidential_match::match_service::backend::FanOutField;
using ::google::confidential_match::match_service::backend::Field;
using ::google::confidential_match::match_service::backend::FieldType;
using ::google::confidential_match::match_service::backend::KeyValue;
using ::google::confidential_match::match_service::backend::LookupDataRecord;
using ::google::confidential_match::match_service::backend::LookupResult;
using ::google::confidential_match::match_service::backend::
    LookupServiceRequest;
using ::google::confidential_match::match_service::backend::
    LookupServiceResponse;
using ::google::confidential_match::match_service::backend::
    MatchedCompositeField;
using ::google::confidential_match::match_service::backend::MatchedDataRecord;
using ::google::confidential_match::match_service::backend::MatchedFanOutField;
using ::google::confidential_match::match_service::backend::MatchedField;
using ::google::confidential_match::match_service::backend::MatchedFieldInfo;
using ::google::confidential_match::match_service::backend::MatchedKey;
using ::google::confidential_match::match_service::backend::MatchKey;
using ::google::confidential_match::match_service::backend::MatchKeyEncoding;
using ::google::confidential_match::match_service::backend::MatchKeyFormat;
using ::google::confidential_match::match_service::backend::MatchRequest;
using ::google::confidential_match::match_service::backend::MatchResponse;
using ::google::confidential_match::match_service::
    encrypted_match_task_internal::EncryptedMatchKey;
using ::google::confidential_match::match_service::
    encrypted_match_task_internal::KeyGroup;
using ::google::confidential_match::match_service::
    encrypted_match_task_internal::KeyIndex;
using ::google::protobuf::RepeatedPtrField;

// This corresponds to PUBLIC_CRYPTO_ERROR error code returned by the Lookup
// Service.
constexpr absl::string_view kLookupServiceCryptoErrorCode = "2415853572";
constexpr absl::string_view kDataRecordIndexKey = "d";
constexpr absl::string_view kMatchKeyIndexKey = "m";
constexpr int64_t kAddressFieldSize = 4;
// Address composite fields must encrypt First & Last name
constexpr int kEncryptedAddressParts = 2;

// Converts an encrypted match key from the provided encoding to base-64.
absl::StatusOr<std::string> ConvertToBase64(absl::string_view match_key,
                                            MatchKeyEncoding key_encoding) {
  std::string decoded_match_key;
  switch (key_encoding) {
    case MatchKeyEncoding::MATCH_KEY_ENCODING_BASE64:
      return std::string(match_key);
    case MatchKeyEncoding::MATCH_KEY_ENCODING_BASE64_WEB_SAFE:
      if (!absl::WebSafeBase64Unescape(match_key, &decoded_match_key)) {
        return Status(Error::INVALID_MATCH_KEY_ENCODING,
                      "Unable to decode base-64 web safe match key.");
      }
      break;
    case MatchKeyEncoding::MATCH_KEY_ENCODING_HEX:
      if (!absl::HexStringToBytes(match_key, &decoded_match_key)) {
        return Status(Error::INVALID_MATCH_KEY_ENCODING,
                      "Unable to decode hexadecimal match key.");
      }
      break;
    default:
      return Status(Error::INVALID_MATCH_KEY_ENCODING,
                    "Match key encoding is not supported.");
  }

  return absl::Base64Escape(decoded_match_key);
}

// Sets the match key index on a LookupServiceRequest metadata.
void WriteMatchKeyIndexToMetadata(RepeatedPtrField<KeyValue>& mutable_metadata,
                                  int64_t data_record_index,
                                  int64_t match_key_index) {
  KeyValue* data_record_index_tracker = mutable_metadata.Add();
  data_record_index_tracker->set_key(kDataRecordIndexKey);
  data_record_index_tracker->set_int_value(data_record_index);
  KeyValue* match_key_index_tracker = mutable_metadata.Add();
  match_key_index_tracker->set_key(kMatchKeyIndexKey);
  match_key_index_tracker->set_int_value(match_key_index);
}

const EncryptionKey* ResolveEncryptionKey(const MatchRequest& request,
                                          const DataRecord& record,
                                          const MatchKey& match_key) {
  if (match_key.has_encryption_key()) return &match_key.encryption_key();
  if (record.has_encryption_key()) return &record.encryption_key();
  if (request.has_encryption_key()) return &request.encryption_key();
  return nullptr;
}

// Extracts the match key index from a LookupServiceResponse metadata.
absl::StatusOr<KeyIndex> ReadMatchKeyIndexFromMetadata(
    const RepeatedPtrField<KeyValue>& metadata) {
  KeyIndex match_key_index;
  match_key_index.data_records_index = -1;
  match_key_index.match_keys_index = -1;

  for (const auto& key_value : metadata) {
    if (key_value.key() == kDataRecordIndexKey) {
      if (!key_value.has_int_value()) {
        return Status(Error::LOOKUP_SERVICE_ERROR,
                      "Missing data record index in Lookup Service response.");
      }
      match_key_index.data_records_index = key_value.int_value();
    } else if (key_value.key() == kMatchKeyIndexKey) {
      if (!key_value.has_int_value()) {
        return Status(Error::LOOKUP_SERVICE_ERROR,
                      "Missing match key index in Lookup Service response.");
      }
      match_key_index.match_keys_index = key_value.int_value();
    } else {
      return Status(
          Error::LOOKUP_SERVICE_ERROR,
          absl::StrCat(
              "Got unexpected metadata key in Lookup Service response: ",
              key_value.key()));
    }
  }

  if (match_key_index.data_records_index == -1) {
    return Status(Error::LOOKUP_SERVICE_ERROR,
                  "Could not find data record index in Lookup Service response "
                  "metadata.");
  } else if (match_key_index.match_keys_index == -1) {
    return Status(Error::LOOKUP_SERVICE_ERROR,
                  "Could not find match key index in Lookup Service response "
                  "metadata.");
  }

  return match_key_index;
}

// Decrypts a base64 encoded string using the crypto key.
absl::StatusOr<std::string> DecryptString(
    const std::string& encrypted_base64, const CryptoKeyInterface& crypto_key) {
  std::string encrypted_bytes;
  if (!absl::Base64Unescape(encrypted_base64, &encrypted_bytes)) {
    return Status(Error::INVALID_MATCH_KEY_ENCODING,
                  "Failed to base64 decode encrypted match key.");
  }

  absl::StatusOr<std::string> decrypted_or =
      crypto_key.Decrypt(encrypted_bytes);
  if (!decrypted_or.ok()) {
    return Annotate(decrypted_or.status(),
                    "Encrypted task failed to decrypt match key.");
  }
  return *decrypted_or;
}

// Converts a Field to a LookupDataRecord using the decrypted value.
absl::Status ToLookupDataRecord(const Field& field,
                                absl::string_view base64_encrypted_value,
                                absl::string_view decrypted_value,
                                LookupDataRecord& lookup_data_record) {
  if (field.type() != FieldType::FIELD_TYPE_EMAIL &&
      field.type() != FieldType::FIELD_TYPE_PHONE) {
    return Status(Error::INVALID_MATCH_KEY_FIELD,
                  "The only supported types for fields are email and phone.");
  }

  lookup_data_record.Clear();
  *lookup_data_record.mutable_lookup_key()->mutable_key() =
      base64_encrypted_value;
  *lookup_data_record.mutable_lookup_key()->mutable_decrypted_key() =
      decrypted_value;
  return absl::OkStatus();
}

// Converts a CompositeField to a LookupDataRecord using the hashed values.
absl::Status ToLookupDataRecord(const CompositeField& composite_field,
                                const std::string& hashed_value,
                                const std::string& encrypted_hashed_value,
                                LookupDataRecord& lookup_data_record) {
  if (composite_field.type() !=
      CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS) {
    return Status(Error::INVALID_MATCH_KEY_FIELD,
                  "Only address composite fields are supported.");
  }
  lookup_data_record.Clear();
  // Key used for lookup is the Encrypted Hash
  *lookup_data_record.mutable_lookup_key()->mutable_key() =
      encrypted_hashed_value;
  // Decrypted key is the Hash itself (to verify match)
  *lookup_data_record.mutable_lookup_key()->mutable_decrypted_key() =
      hashed_value;
  return absl::OkStatus();
}

// Returns index [0, 4) for address fields.
absl::StatusOr<size_t> GetAddressFieldIndex(FieldType field_type) {
  switch (field_type) {
    case FieldType::FIELD_TYPE_FIRST_NAME:
      return 0;
    case FieldType::FIELD_TYPE_LAST_NAME:
      return 1;
    case FieldType::FIELD_TYPE_COUNTRY_CODE:
      return 2;
    case FieldType::FIELD_TYPE_ZIP_CODE:
      return 3;
    default:
      return Status(Error::INVALID_MATCH_KEY_FIELD,
                    "Field type is not supported for addresses.");
  }
}

// Reconstructs all parts of a composite address field.
absl::Status ReconstructAddressParts(
    absl::Span<const std::pair<backend::FieldType, absl::string_view>>
        decrypted_fields,
    const CompositeField& composite_field,
    std::vector<std::string>& decrypted_parts) {
  if (decrypted_fields.size() != 2) {
    return Status(Error::INTERNAL_ERROR,
                  "Expected two decrypted fields for composite fields: first "
                  "and last name");
  }
  if (composite_field.values_size() != kAddressFieldSize) {
    return Status(
        Error::INTERNAL_ERROR,
        "Match request composite field contains incorrect number of elements.");
  }

  std::vector<bool> is_index_used(kAddressFieldSize, false);

  // Map each decrypted value to its correct position
  for (const auto& [field_type, decrypted_value] : decrypted_fields) {
    size_t i;
    ASSIGN_OR_RETURN(i, GetAddressFieldIndex(field_type));
    if (is_index_used[i]) {
      return Status(Error::INTERNAL_ERROR,
                    "Composite address contains duplicate fields.");
    }
    decrypted_parts[i] = decrypted_value;
    is_index_used[i] = true;
  }

  // Map remaining fields which were received already decrypted in the request
  for (const auto& field : composite_field.values()) {
    if (field.type() == FieldType::FIELD_TYPE_FIRST_NAME ||
        field.type() == FieldType::FIELD_TYPE_LAST_NAME) {
      continue;
    }

    size_t i;
    ASSIGN_OR_RETURN(i, GetAddressFieldIndex(field.type()));
    if (is_index_used[i]) {
      return Status(Error::INTERNAL_ERROR,
                    "Composite address contains duplicate fields.");
    }
    decrypted_parts[i] = field.value();
    is_index_used[i] = true;
  }

  for (bool is_used : is_index_used) {
    if (!is_used) {
      return Status(Error::INTERNAL_ERROR,
                    "Composite field is missing required fields.");
    }
  }
  return absl::OkStatus();
}

// Normalizes, hashes, and encrypts the reconstructed address parts.
absl::Status HashAndEncryptAddress(
    const std::vector<std::string>& decrypted_parts,
    const CryptoKeyInterface& crypto_key, const HasherInterface& sha256_hasher,
    std::string& hashed_value, std::string& encrypted_hashed_value_b64) {
  std::vector<std::string> normalized_parts = decrypted_parts;
  size_t co_idx;
  ASSIGN_OR_RETURN(co_idx,
                   GetAddressFieldIndex(FieldType::FIELD_TYPE_COUNTRY_CODE));
  size_t pc_idx;
  ASSIGN_OR_RETURN(pc_idx,
                   GetAddressFieldIndex(FieldType::FIELD_TYPE_ZIP_CODE));

  std::string raw_country = normalized_parts[co_idx];
  normalized_parts[co_idx] = NormalizeCountryCode(raw_country);
  normalized_parts[pc_idx] =
      NormalizeZipCode(normalized_parts[pc_idx], raw_country);

  std::string concatenated_hash_input = absl::StrJoin(normalized_parts, "");
  ASSIGN_OR_RETURN(hashed_value,
                   sha256_hasher.Base64EncodedHash(concatenated_hash_input));

  // Encrypt and encode Hash
  std::string encrypted_hash;
  ASSIGN_OR_RETURN(encrypted_hash, crypto_key.Encrypt(hashed_value));
  encrypted_hashed_value_b64 = absl::Base64Escape(encrypted_hash);

  return absl::OkStatus();
}

// Copies over all shared information from a Field to a MatchedField.
absl::Status ToMatchedField(const Field& field,
                            MatchedField& matched_field) noexcept {
  matched_field.Clear();
  matched_field.mutable_matched_field_info()->set_field_type(field.type());
  return absl::OkStatus();
}

// Copies over all shared information from a Field to a MatchedCompositeField.
absl::Status ToMatchedCompositeField(
    const CompositeField& composite_field,
    MatchedCompositeField& matched_composite_field) noexcept {
  matched_composite_field.Clear();
  for (const auto& field : composite_field.values()) {
    matched_composite_field.add_matched_field_info()->set_field_type(
        field.type());
  }
  return absl::OkStatus();
}

// Copies over all shared information from a Field to a MatchedFanOutField.
absl::Status ToMatchedFanOutField(
    const FanOutField& fan_out_field,
    MatchedFanOutField& matched_fan_out_field) noexcept {
  return Status(Error::INTERNAL_ERROR, "Fan-out key fields are not supported.");
}

// Copies over all shared information from a MatchKey to a MatchedKey.
absl::Status ToMatchedKey(const MatchKey& match_key,
                          MatchedKey& matched_key) noexcept {
  *matched_key.mutable_metadata() = match_key.metadata();

  switch (match_key.field_info_case()) {
    case MatchKey::kField:
      RETURN_IF_ERROR(
          ToMatchedField(match_key.field(), *matched_key.mutable_field()));
      break;
    case MatchKey::kCompositeField:
      RETURN_IF_ERROR(ToMatchedCompositeField(
          match_key.composite_field(), *matched_key.mutable_composite_field()));
      break;
    case MatchKey::kFanOutKeyField:
      RETURN_IF_ERROR(
          ToMatchedFanOutField(match_key.fan_out_key_field(),
                               *matched_key.mutable_fan_out_key_field()));
      break;
    case MatchKey::FIELD_INFO_NOT_SET:
      return Status(Error::INVALID_MATCH_KEY_FIELD,
                    "Each match key must contain a field_info.");
  }

  return absl::OkStatus();
}

// Converts an absl::Status to a field-level match response error reason.
ErrorReason ToErrorReason(const absl::Status& status) {
  Error::Reason backend_error_reason =
      GetBackendErrorReason(status).value_or(Error::INTERNAL_ERROR);
  switch (backend_error_reason) {
    case Error::DECRYPTION_ERROR:
      return backend::ERROR_REASON_DECRYPTION_ERROR;
    case Error::DEK_DECRYPTION_ERROR:
      return backend::ERROR_REASON_DEK_DECRYPTION_ERROR;
    case Error::INVALID_DEK:
      return backend::ERROR_REASON_INVALID_DEK;
    case Error::CUSTOMER_KEY_PERMISSION_DENIED:
      return backend::ERROR_REASON_CUSTOMER_KEY_PERMISSION_DENIED;
    case Error::CUSTOMER_QUOTA_EXCEEDED:
      return backend::ERROR_REASON_CUSTOMER_QUOTA_EXCEEDED;
    case Error::WRAPPED_KEY_FETCHING_ERROR:
      return backend::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR;
    case Error::ENCRYPTION_ERROR:
      return backend::ERROR_REASON_ENCRYPTION_ERROR;
    case Error::INVALID_MATCH_KEY_ENCODING:
      return backend::ERROR_REASON_DECODING_ERROR;
    case Error::INVALID_MATCH_KEY_FIELD:
      return backend::ERROR_REASON_INVALID_MATCH_KEY_FIELD;
    case Error::INTERNAL_ERROR:
    default:
      return backend::ERROR_REASON_INTERNAL_ERROR;
  }
}

// Converts a Lookup Service result to a Match Service error reason.
std::optional<ErrorReason> MapLookupResultToErrorReason(
    const LookupResult& lookup_result) {
  switch (lookup_result.status()) {
    case LookupResult::STATUS_SUCCESS:
      return std::nullopt;
    case LookupResult::STATUS_FAILED:
      if (absl::StrContains(lookup_result.error_response(),
                            kLookupServiceCryptoErrorCode)) {
        return ErrorReason::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR;
      }
      return ErrorReason::ERROR_REASON_INTERNAL_ERROR;
    default:
      return ErrorReason::ERROR_REASON_INTERNAL_ERROR;
  }
}

// Adds results to a MatchedField, using the decrypted value for the response.
absl::Status AddLookupResultsToField(
    const LookupResult& lookup_result, absl::string_view decrypted_value,
    AssociatedDataCollector& associated_data_collector,
    MatchedField& matched_field) noexcept {
  if (matched_field.has_status()) return absl::OkStatus();

  std::optional<ErrorReason> error_reason_opt =
      MapLookupResultToErrorReason(lookup_result);

  if (error_reason_opt.has_value()) {
    matched_field.set_status(backend::STATUS_FAILED);
    matched_field.set_error_reason(error_reason_opt.value());
  } else {
    matched_field.set_status(lookup_result.matched_data_records_size() > 0
                                 ? backend::STATUS_SUCCESS_MATCHED
                                 : backend::STATUS_SUCCESS_UNMATCHED);
  }

  MatchedFieldInfo* matched_field_info =
      matched_field.mutable_matched_field_info();

  // Important: We set the field value to the decrypted value.
  if (matched_field.status() == backend::STATUS_SUCCESS_MATCHED) {
    matched_field_info->set_field_value(decrypted_value);
  } else {
    matched_field_info->clear_field_value();
  }

  for (const auto& lookup_rec : lookup_result.matched_data_records()) {
    if (lookup_rec.associated_data_size() > 0) {
      AssociatedData associated_data;
      RETURN_IF_ERROR(
          ToMatchService(lookup_rec.associated_data(), associated_data));
      int index = associated_data_collector.Add(associated_data);
      matched_field.add_matched_associated_data_indices(index);
    }
  }
  return absl::OkStatus();
}

// Copies over match results from a LookupResult to a MatchedCompositeField.
// decrypted_values contains all composite field match keys in decrypted form,
// in the concatenation ordering required by Lookup Service.
absl::Status AddLookupResultsToCompositeField(
    const LookupResult& lookup_result,
    absl::Span<const std::string> decrypted_values,
    AssociatedDataCollector& associated_data_collector,
    MatchedCompositeField& matched_composite_field) noexcept {
  if (matched_composite_field.has_status()) return absl::OkStatus();

  std::optional<ErrorReason> error_reason_opt =
      MapLookupResultToErrorReason(lookup_result);

  if (error_reason_opt.has_value()) {
    matched_composite_field.set_status(backend::STATUS_FAILED);
    matched_composite_field.set_error_reason(error_reason_opt.value());
  } else {
    matched_composite_field.set_status(
        lookup_result.matched_data_records_size() > 0
            ? backend::STATUS_SUCCESS_MATCHED
            : backend::STATUS_SUCCESS_UNMATCHED);
  }

  if (matched_composite_field.status() != backend::STATUS_SUCCESS_MATCHED) {
    for (int i = 0; i < matched_composite_field.matched_field_info_size();
         ++i) {
      matched_composite_field.mutable_matched_field_info(i)
          ->clear_field_value();
    }
  } else {
    // We expect exactly kAddressFieldSize parts.
    if (decrypted_values.size() != kAddressFieldSize) {
      return Status(Error::INTERNAL_ERROR,
                    "Stored decrypted values count mismatch.");
    }

    for (int i = 0; i < matched_composite_field.matched_field_info_size();
         ++i) {
      MatchedFieldInfo* info =
          matched_composite_field.mutable_matched_field_info(i);
      size_t idx;
      ASSIGN_OR_RETURN(idx, GetAddressFieldIndex(info->field_type()));
      if (idx >= decrypted_values.size()) {
        return Status(Error::INTERNAL_ERROR,
                      "Address field index exceeds size of decrypted values.");
      }
      info->set_field_value(decrypted_values[idx]);
    }
  }

  for (const auto& record : lookup_result.matched_data_records()) {
    if (record.associated_data_size() > 0) {
      AssociatedData data;
      RETURN_IF_ERROR(ToMatchService(record.associated_data(), data));
      matched_composite_field.add_matched_associated_data_indices(
          associated_data_collector.Add(data));
    }
  }
  return absl::OkStatus();
}

// Copies over match results from a LookupResult to a MatchedFanOutField.
absl::Status AddLookupResultsToFanOutField(
    const LookupResult& lookup_result,
    AssociatedDataCollector& associated_data_collector,
    MatchedFanOutField& matched_fan_out_field) noexcept {
  return Status(Error::INTERNAL_ERROR, "Fan-out key fields are not supported.");
}

// Copies over match results from a LookupResult to a MatchedKey.
absl::Status AddLookupResultsToMatchedKey(
    const LookupResult& lookup_result,
    absl::Span<const std::string> decrypted_values,
    AssociatedDataCollector& associated_data_collector,
    MatchedKey& matched_key) noexcept {
  switch (matched_key.field_info_case()) {
    case MatchedKey::kField:
      if (decrypted_values.size() != 1) {
        return Status(Error::INTERNAL_ERROR,
                      "Got unexpected number of decrypted values for field "
                      "(should be 1)");
      }
      RETURN_IF_ERROR(AddLookupResultsToField(
          lookup_result, decrypted_values[0], associated_data_collector,
          *matched_key.mutable_field()));
      break;
    case MatchedKey::kCompositeField:
      RETURN_IF_ERROR(AddLookupResultsToCompositeField(
          lookup_result, decrypted_values, associated_data_collector,
          *matched_key.mutable_composite_field()));
      break;
    case MatchedKey::kFanOutKeyField:
      RETURN_IF_ERROR(AddLookupResultsToFanOutField(
          lookup_result, associated_data_collector,
          *matched_key.mutable_fan_out_key_field()));
      break;
    case MatchedKey::FIELD_INFO_NOT_SET:
      return Status(
          Error::INTERNAL_ERROR,
          "Each matched key should have been initialized with field_info.");
  }
  return absl::OkStatus();
}

// Aggregates all lookup responses into a single MatchResponse.
//
// lookup_responses: A list containing all of the lookup responses for each
//                   encryption key group.
// match_request: The original match request.
// decrypted_values: A 3D vector keyed by:
//                   [data record index][match key index][composite field index]
//                   containing decrypted values to be set in the response.
//                   Required since LS responses merge composite fields.
absl::Status BuildMatchResponse(
    const std::vector<std::shared_ptr<LookupServiceResponse>>& lookup_responses,
    const std::vector<std::vector<std::vector<std::string>>>& decrypted_values,
    MatchResponse& match_response) noexcept {
  AssociatedDataCollector associated_data_collector;

  // Map Results from ALL lookup responses
  for (const auto& response : lookup_responses) {
    for (const auto& lookup_result : response->lookup_results()) {
      KeyIndex index;
      ASSIGN_OR_RETURN(index,
                       ReadMatchKeyIndexFromMetadata(
                           lookup_result.client_data_record().metadata()));

      if (index.data_records_index >=
              match_response.matched_data_records_size() ||
          index.match_keys_index >=
              match_response.matched_data_records(index.data_records_index)
                  .matched_keys_size()) {
        return Status(Error::LOOKUP_SERVICE_ERROR,
                      "Invalid indices in metadata.");
      }

      MatchedKey* matched_key =
          match_response.mutable_matched_data_records(index.data_records_index)
              ->mutable_matched_keys(index.match_keys_index);

      // Retrieve the decrypted value
      const std::vector<std::string>& decrypted_val =
          decrypted_values[index.data_records_index][index.match_keys_index];

      RETURN_IF_ERROR(AddLookupResultsToMatchedKey(lookup_result, decrypted_val,
                                                   associated_data_collector,
                                                   *matched_key));
    }
  }

  // Add Associated Data
  for (AssociatedData& ad : associated_data_collector.Get()) {
    *match_response.add_matched_associated_data() = std::move(ad);
  }

  return absl::OkStatus();
}

// Helper to check if a matched key has a failed status.
bool HasFailedStatus(const MatchedKey& matched_key) {
  return matched_key.field().status() == backend::STATUS_FAILED ||
         matched_key.composite_field().status() == backend::STATUS_FAILED ||
         matched_key.fan_out_key_field().status() == backend::STATUS_FAILED;
}

// Writes a failed status and error reason to a matched key.
void SetMatchedKeyAsFailed(ErrorReason error_reason, MatchedKey& matched_key) {
  if (matched_key.field_info_case() == MatchedKey::kField) {
    matched_key.mutable_field()->set_status(backend::STATUS_FAILED);
    matched_key.mutable_field()->set_error_reason(error_reason);
  } else if (matched_key.field_info_case() == MatchedKey::kCompositeField) {
    matched_key.mutable_composite_field()->set_status(backend::STATUS_FAILED);
    matched_key.mutable_composite_field()->set_error_reason(error_reason);
  }
}

// Sets field-level errors on a MatchResponse for all keys in the key group.
void SetFieldLevelErrorsForKeyGroup(const KeyGroup& key_group,
                                    ErrorReason error_reason,
                                    MatchResponse& match_response) {
  for (const auto& encrypted_match_key : key_group.keys) {
    const KeyIndex& key_index = encrypted_match_key.key_index;
    MatchedKey* matched_key =
        match_response
            .mutable_matched_data_records(key_index.data_records_index)
            ->mutable_matched_keys(key_index.match_keys_index);
    SetMatchedKeyAsFailed(error_reason, *matched_key);
  }
}

// Adds a match key to the key group.
absl::Status AddKeyToGroup(MatchKeyEncoding key_encoding, const MatchKey& key,
                           int record_index, int match_key_index,
                           KeyGroup& group, MatchResponse& match_response) {
  MatchedKey* matched_key =
      match_response.mutable_matched_data_records(record_index)
          ->mutable_matched_keys(match_key_index);

  if (key.field_info_case() == MatchKey::kField) {
    auto status = ValidateMatchKeyEncoding(key_encoding, key.field().value());
    if (!status.ok()) {
      SetMatchedKeyAsFailed(ToErrorReason(status), *matched_key);
      return absl::OkStatus();
    }
    auto base64_or = ConvertToBase64(key.field().value(), key_encoding);
    if (!base64_or.ok()) {
      SetMatchedKeyAsFailed(ToErrorReason(base64_or.status()), *matched_key);
      return absl::OkStatus();
    }
    group.keys.push_back({std::move(*base64_or),
                          key.field().type(),
                          {record_index, match_key_index}});
  } else if (key.field_info_case() == MatchKey::kCompositeField) {
    if (key.composite_field().type() !=
        CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS) {
      SetMatchedKeyAsFailed(backend::ERROR_REASON_INVALID_MATCH_KEY_FIELD,
                            *matched_key);
      return absl::OkStatus();
    }
    int encrypted_count = 0;
    std::vector<EncryptedMatchKey> temp_keys;
    for (const auto& sub_field : key.composite_field().values()) {
      if (sub_field.type() == FieldType::FIELD_TYPE_FIRST_NAME ||
          sub_field.type() == FieldType::FIELD_TYPE_LAST_NAME) {
        auto status = ValidateMatchKeyEncoding(key_encoding, sub_field.value());
        if (!status.ok()) {
          SetMatchedKeyAsFailed(ToErrorReason(status), *matched_key);
          return absl::OkStatus();
        }
        auto base64_or = ConvertToBase64(sub_field.value(), key_encoding);
        if (!base64_or.ok()) {
          SetMatchedKeyAsFailed(ToErrorReason(base64_or.status()),
                                *matched_key);
          return absl::OkStatus();
        }
        temp_keys.push_back({std::move(*base64_or),
                             sub_field.type(),
                             {record_index, match_key_index}});
        encrypted_count++;
      }
    }
    // Verify API Contract: Exactly 2 encrypted parts (First & Last)
    if (encrypted_count != kEncryptedAddressParts) {
      SetMatchedKeyAsFailed(backend::ERROR_REASON_INVALID_MATCH_KEY_FIELD,
                            *matched_key);
      return absl::OkStatus();
    }
    for (auto& k : temp_keys) {
      group.keys.push_back(std::move(k));
    }
  }
  return absl::OkStatus();
}

// Validates the match request.
absl::Status ValidateRequest(const MatchRequest& request) {
  if (request.match_key_format() !=
      MatchKeyFormat::MATCH_KEY_FORMAT_HASHED_ENCRYPTED) {
    return Status(Error::INVALID_MATCH_KEY_FORMAT,
                  "Match key format must be HASHED_ENCRYPTED.");
  }
  return absl::OkStatus();
}

// Pre-fills the match response and resizes the decrypted values grid.
absl::Status PreFillResponseAndResizeValues(
    const MatchRequest& request, MatchResponse& match_response,
    std::vector<std::vector<std::vector<std::string>>>& decrypted_values) {
  *match_response.mutable_metadata() = request.metadata();
  for (const auto& data_record : request.data_records()) {
    MatchedDataRecord* matched_data_record =
        match_response.add_matched_data_records();
    *matched_data_record->mutable_metadata() = data_record.metadata();
    for (const auto& match_key : data_record.match_keys()) {
      MatchedKey* matched_key = matched_data_record->add_matched_keys();
      // TODO(b/511279365): Convert these to field-level errors.
      RETURN_IF_ERROR(ToMatchedKey(match_key, *matched_key));
    }
  }

  // Resize decrypted values grid
  decrypted_values.resize(request.data_records_size());
  for (int r = 0; r < request.data_records_size(); ++r) {
    decrypted_values[r].resize(request.data_records(r).match_keys_size());
  }
  return absl::OkStatus();
}

absl::string_view GetEncryptionKeyType(const EncryptionKeyInfo& key_info) {
  if (key_info.has_coordinator_key_info()) {
    return metrics::kCoordinatorKeyType;
  }
  return metrics::kWrappedKeyType;
}

std::string GetApplication(const MatchRequest& request) {
  return Application_Name(request.application());
}

std::string GetMatchKeyFormat(const MatchRequest& request) {
  return MatchKeyFormat_Name(request.match_key_format());
}

Metric CreateKeyFetchingCountMetric(const MatchRequest& request,
                                    const EncryptionKeyInfo& key_info) {
  Metric m;
  m.set_name(metrics::kKeyFetchingRequestCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(key_info);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  return m;
}

Metric CreateKeyFetchingLatencyMetric(const MatchRequest& request,
                                      const EncryptionKeyInfo& key_info,
                                      absl::Duration latency) {
  Metric m;
  m.set_name(metrics::kKeyFetchingRequestLatencyMetricName);
  m.set_type(MetricType::METRIC_TYPE_HISTOGRAM);
  m.set_unit(MetricUnit::METRIC_UNIT_MILLISECONDS);
  m.set_value(absl::StrCat(absl::ToInt64Milliseconds(latency)));
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(key_info);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  return m;
}

Metric CreateKeyFetchingErrorCountMetric(const MatchRequest& request,
                                         const EncryptionKeyInfo& key_info,
                                         const absl::Status& status) {
  Metric m;
  m.set_name(metrics::kKeyFetchingRequestErrorCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(key_info);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  (*m.mutable_labels())[metrics::kBackendErrorLabel] =
      GetBackendErrorReasonString(status);
  return m;
}

}  // namespace

void KmsEncryptedMatchTask::Match(
    AsyncContext<MatchRequest, MatchResponse> context) noexcept {
  LOG_INFO(*context.logger, "Processing KMS encrypted match request...");

  if (auto status = ValidateRequest(*context.request); !status.ok()) {
    context.status = status;
    context.Finish();
    return;
  }

  // Initialize context tracking all necessary state for the match request
  auto state = std::make_shared<MatchRequestContext>(context);
  state->match_response = std::make_shared<MatchResponse>();

  if (auto status = PreFillResponseAndResizeValues(
          *context.request, *state->match_response, state->decrypted_values);
      !status.ok()) {
    context.status = status;
    context.Finish();
    return;
  }

  // Group Keys by their Encryption Configuration
  absl::StatusOr<std::vector<KeyGroup>> key_groups_or =
      GroupByEncryptionKey(*context.request, *state->match_response);
  if (!key_groups_or.ok()) {
    context.status = key_groups_or.status();
    context.Finish();
    return;
  }
  std::vector<KeyGroup> key_groups = std::move(*key_groups_or);

  if (key_groups.empty()) {
    LOG_WARNING(*context.logger, "No keys to process.");
    // Return an OK status if there are no keys to process. Field level errors
    // will be set in the pre-filled match response.
    context.response = state->match_response;
    context.Finish();
    return;
  }

  state->pending_groups = key_groups.size();

  if (metric_client_ != nullptr) {
    state->key_group_start_times.resize(key_groups.size());
  }

  // Asynchronously process each key group, starting first with key unwrapping
  for (size_t i = 0; i < key_groups.size(); ++i) {
    auto& key_group = key_groups[i];
    auto core_key_info = std::make_shared<EncryptionKeyInfo>();
    if (auto status = ToCore(*key_group.key_info, *core_key_info);
        !status.ok()) {
      context.status =
          Annotate(status, "Could not convert EncryptionKey to core format.");
      context.Finish();
      return;
    }

    auto key_context =
        context.CreateAsyncContext<EncryptionKeyInfo, CryptoKeyInterface>();
    key_context.request = core_key_info;
    key_context.callback =
        absl::bind_front(&KmsEncryptedMatchTask::OnGroupCryptoKeyReady, this,
                         state, key_group, i);

    // We default to using the AeadCryptoClient. If the key_group is using
    // coordinator keys, we will use the appropriate HybridCryptoClient.
    CryptoClientInterface* crypto_client =
        core_key_info->has_coordinator_key_info() ? hybrid_crypto_client_
                                                  : aead_crypto_client_;

    if (metric_client_ != nullptr) {
      state->key_group_start_times[i] = absl::Now();
      if (core_key_info->has_coordinator_key_info()) {
        LOG_INFO(*state->match_context.logger,
                 "key_group_start_times for group %d: %s", i,
                 absl::FormatTime(state->key_group_start_times[i]));
      }
      metrics::PutMetric(state->match_context.logger, metric_client_,
                         metric_namespace_,
                         CreateKeyFetchingCountMetric(
                             *state->match_context.request, *core_key_info));
    }

    crypto_client->GetCryptoKey(key_context);
  }
}

absl::StatusOr<std::vector<KeyGroup>>
KmsEncryptedMatchTask::GroupByEncryptionKey(const MatchRequest& request,
                                            MatchResponse& match_response) {
  // Maps a serialized EncryptionKeyInfo string to the key group for that key.
  std::map<std::string, KeyGroup> groups;

  int record_index = 0;
  for (const auto& record : request.data_records()) {
    int match_key_index = 0;
    for (const auto& key : record.match_keys()) {
      const EncryptionKey* effective_key =
          ResolveEncryptionKey(request, record, key);

      if (!effective_key) {
        return Status(Error::MISSING_ENCRYPTION_KEY_FIELD,
                      "Missing encryption key for match key.");
      }

      auto key_info = std::make_shared<EncryptionKeyInfo>();
      if (auto status = ToCore(*effective_key, *key_info); !status.ok()) {
        return Annotate(status,
                        "Could not convert EncryptionKey to core format.");
      }

      if (key_info->has_coordinator_key_info()) {
        PopulateCoordinatorInfo(key_info->mutable_coordinator_key_info());
      }

      // Serialize to use as map key for grouping
      std::string serialized_key = key_info->SerializeAsString();
      KeyGroup& group = groups[serialized_key];
      if (group.keys.empty()) {
        // Store backend proto in group for constructing LookupRequest later
        group.key_info =
            std::make_shared<backend::EncryptionKey>(*effective_key);

        if (group.key_info->has_coordinator_key()) {
          PopulateCoordinatorInfo(group.key_info->mutable_coordinator_key());
        }
      }

      if (auto status = AddKeyToGroup(request.key_encoding(), key, record_index,
                                      match_key_index, group, match_response);
          !status.ok()) {
        MatchedKey* matched_key =
            match_response.mutable_matched_data_records(record_index)
                ->mutable_matched_keys(match_key_index);
        SetMatchedKeyAsFailed(ToErrorReason(status), *matched_key);
      }
      match_key_index++;
    }
    record_index++;
  }

  // Return the individual key groups as a vector
  std::vector<KeyGroup> grouped_keys;
  grouped_keys.reserve(groups.size());
  for (auto& [key, group] : groups) {
    if (!group.keys.empty()) {
      grouped_keys.push_back(std::move(group));
    }
  }
  return grouped_keys;
}

std::vector<absl::StatusOr<std::string>>
KmsEncryptedMatchTask::DecryptKeysInGroup(
    const KeyGroup& group, const CryptoKeyInterface& crypto_key) {
  std::vector<absl::StatusOr<std::string>> decrypted_strings;
  decrypted_strings.reserve(group.keys.size());
  for (const auto& key : group.keys) {
    decrypted_strings.push_back(
        DecryptString(key.encrypted_match_key, crypto_key));
  }
  return decrypted_strings;
}

absl::Status KmsEncryptedMatchTask::ProcessSingleField(
    const EncryptedMatchKey& key, absl::string_view decrypted_val,
    MatchRequestContext& state, backend::LookupServiceRequest& lookup_request) {
  const KeyIndex& idx = key.key_index;

  // Build Lookup Record
  const MatchKey& original_key =
      state.match_context.request->data_records(idx.data_records_index)
          .match_keys(idx.match_keys_index);

  LookupDataRecord lookup_record;
  RETURN_IF_ERROR(ToLookupDataRecord(original_key.field(),
                                     key.encrypted_match_key, decrypted_val,
                                     lookup_record));
  WriteMatchKeyIndexToMetadata(*lookup_record.mutable_metadata(),
                               idx.data_records_index, idx.match_keys_index);

  // Store plain decrypted value for matching
  state.decrypted_values[idx.data_records_index][idx.match_keys_index] = {
      std::string(decrypted_val)};

  lookup_request.add_data_records()->Swap(&lookup_record);
  return absl::OkStatus();
}

absl::Status KmsEncryptedMatchTask::ProcessCompositeField(
    absl::Span<const std::pair<backend::FieldType, absl::string_view>>
        decrypted_fields,
    const KeyIndex& key_index, const CryptoKeyInterface& crypto_key,
    MatchRequestContext& state, backend::LookupServiceRequest& lookup_request) {
  const CompositeField& composite_field =
      state.match_context.request->data_records(key_index.data_records_index)
          .match_keys(key_index.match_keys_index)
          .composite_field();

  std::vector<std::string> decrypted_parts(kAddressFieldSize);
  RETURN_IF_ERROR(ReconstructAddressParts(decrypted_fields, composite_field,
                                          decrypted_parts));

  // Save decrypted values to generate the decrypted match response later
  std::vector<std::string>& decrypted_address =
      state.decrypted_values[key_index.data_records_index]
                            [key_index.match_keys_index];
  decrypted_address.assign(decrypted_parts.begin(), decrypted_parts.end());

  std::string hashed_value;
  std::string encrypted_hashed_value_b64;
  RETURN_IF_ERROR(HashAndEncryptAddress(decrypted_parts, crypto_key,
                                        *sha256_hasher_, hashed_value,
                                        encrypted_hashed_value_b64));

  // Add hashed data to LookupRequest safely
  LookupDataRecord lookup_data_record;
  RETURN_IF_ERROR(ToLookupDataRecord(composite_field, hashed_value,
                                     encrypted_hashed_value_b64,
                                     lookup_data_record));
  WriteMatchKeyIndexToMetadata(*lookup_data_record.mutable_metadata(),
                               key_index.data_records_index,
                               key_index.match_keys_index);
  lookup_request.add_data_records()->Swap(&lookup_data_record);

  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<LookupServiceRequest>>
KmsEncryptedMatchTask::CreateLookupRequest(
    MatchRequestContext& state, const KeyGroup& group,
    const CryptoKeyInterface& crypto_key) {
  auto lookup_request = std::make_shared<LookupServiceRequest>();
  lookup_request->set_key_format(
      LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED);
  lookup_request->mutable_hash_info()->set_hash_type(
      LookupServiceRequest::HashInfo::HASH_TYPE_SHA_256);

  *lookup_request->mutable_encryption_key() = *group.key_info;

  // Batch Decryption
  // Each decrypted string corresponds to the encrypted value in group.keys
  // at the same index.
  std::vector<absl::StatusOr<std::string>> decrypted_strings_or =
      DecryptKeysInGroup(group, crypto_key);

  // Acquire the lock since the MatchResponse may be updated for errors.
  absl::ReleasableMutexLock lock(&state.mutex);

  // Iterate through keys and build request
  size_t i = 0;
  while (i < group.keys.size()) {
    const EncryptedMatchKey& key = group.keys[i];

    const KeyIndex& idx = key.key_index;
    const MatchKey& original_key =
        state.match_context.request->data_records(idx.data_records_index)
            .match_keys(idx.match_keys_index);

    MatchedKey* matched_key =
        state.match_response
            ->mutable_matched_data_records(idx.data_records_index)
            ->mutable_matched_keys(idx.match_keys_index);

    const int step =
        (original_key.field_info_case() == MatchKey::kCompositeField) ? 2 : 1;

    if (HasFailedStatus(*matched_key)) {
      i += step;
      continue;
    }

    // Write any decryption failures as field-level errors on the response.
    const absl::StatusOr<std::string>& decrypted_val_or =
        decrypted_strings_or[i];
    if (!decrypted_val_or.ok()) {
      SetMatchedKeyAsFailed(ToErrorReason(decrypted_val_or.status()),
                            *matched_key);
      i += step;
      continue;
    }
    absl::string_view decrypted_val = *decrypted_val_or;

    if (original_key.field_info_case() == MatchKey::kField) {
      if (absl::Status status =
              ProcessSingleField(key, decrypted_val, state, *lookup_request);
          !status.ok()) {
        SetMatchedKeyAsFailed(ToErrorReason(status), *matched_key);
      }
    } else if (original_key.field_info_case() == MatchKey::kCompositeField) {
      // Process Composite Field in chunks of 2 since first name + last name
      // will always be stored one after the other
      if (i + 1 >= group.keys.size()) {
        return Status(Error::INTERNAL_ERROR,
                      "Unexpected end of keys for composite field.");
      }

      const EncryptedMatchKey& next_key = group.keys[i + 1];
      const absl::StatusOr<std::string>& next_decrypted_val_or =
          decrypted_strings_or[i + 1];
      if (!next_decrypted_val_or.ok()) {
        SetMatchedKeyAsFailed(ToErrorReason(next_decrypted_val_or.status()),
                              *matched_key);
        i += step;
        continue;
      }
      absl::string_view next_decrypted_val = *next_decrypted_val_or;

      if (key.key_index.data_records_index !=
              next_key.key_index.data_records_index ||
          key.key_index.match_keys_index !=
              next_key.key_index.match_keys_index) {
        return Status(Error::INTERNAL_ERROR, "Composite indices mismatch.");
      }

      std::vector<std::pair<backend::FieldType, absl::string_view>>
          decrypted_fields = {
              {key.field_type, decrypted_val},
              {next_key.field_type, next_decrypted_val},
          };
      absl::Status status = ProcessCompositeField(
          decrypted_fields, key.key_index, crypto_key, state, *lookup_request);
      if (!status.ok()) {
        SetMatchedKeyAsFailed(ToErrorReason(status), *matched_key);
      }

    } else {
      // Shouldn't happen.
      SetMatchedKeyAsFailed(ErrorReason::ERROR_REASON_INVALID_MATCH_KEY_FIELD,
                            *matched_key);
    }
    // Move to the next key (or next two keys for composite fields)
    i += step;
  }

  lock.Release();

  // Add Associated Data Types
  if (state.match_context.request->associated_data_types_size() > 0) {
    for (const int type :
         state.match_context.request->associated_data_types()) {
      RETURN_IF_ERROR(ToLookup(static_cast<AssociatedDataType>(type),
                               *lookup_request->add_associated_data_keys()));
    }
  }

  lookup_request->set_application(state.match_context.request->application());

  return lookup_request;
}

void KmsEncryptedMatchTask::OnGroupCryptoKeyReady(
    std::shared_ptr<MatchRequestContext> state, KeyGroup group,
    size_t group_index,
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context) noexcept {
  // Record metrics for key fetching latency and errors.
  if (metric_client_ != nullptr) {
    const absl::Duration latency =
        absl::Now() - state->key_group_start_times[group_index];
    if (key_context.request->has_coordinator_key_info()) {
      LOG_INFO(*state->match_context.logger,
               "key latency: %s with start time: %s",
               absl::FormatDuration(latency),
               absl::FormatTime(state->key_group_start_times[group_index]));
    }
    metrics::PutMetric(
        state->match_context.logger, metric_client_, metric_namespace_,
        CreateKeyFetchingLatencyMetric(*state->match_context.request,
                                       *key_context.request, latency));

    if (!key_context.status.ok()) {
      metrics::PutMetric(
          state->match_context.logger, metric_client_, metric_namespace_,
          CreateKeyFetchingErrorCountMetric(*state->match_context.request,
                                            *key_context.request,
                                            key_context.status));
    }
  }

  // If a request-level failure occurred in another thread, stop work on the
  // current thread.
  bool has_previous_failure = false;
  {
    absl::MutexLock lock(&state->mutex);
    has_previous_failure = !state->status.ok();
  }
  if (has_previous_failure) {
    // Wind down this group without recording a new failure.
    auto empty_context =
        state->match_context
            .CreateAsyncContext<LookupServiceRequest, LookupServiceResponse>();
    empty_context.response = std::make_shared<LookupServiceResponse>();
    OnLookupCallback(state, empty_context);
    return;
  }

  if (key_context.request->has_coordinator_key_info()) {
    LOG_INFO(*state->match_context.logger, "finished getting coord key: %d",
             key_context.status.ok());
  }

  // For decryption errors, set field-level errors on the response, instead of
  // setting an error to state->status (used for request-level gRPC errors).
  if (!key_context.status.ok()) {
    LOG_ERROR(*state->match_context.logger,
              "Failed to retrieve crypto key for group: %v",
              key_context.status);
    {
      absl::MutexLock lock(&state->mutex);
      SetFieldLevelErrorsForKeyGroup(group, ToErrorReason(key_context.status),
                                     *state->match_response);
    }
    // Run the same callbacks using a placeholder empty lookup response,
    // to update state and assemble the final response on the last thread.
    auto empty_context =
        state->match_context
            .CreateAsyncContext<LookupServiceRequest, LookupServiceResponse>();
    empty_context.response = std::make_shared<LookupServiceResponse>();
    OnLookupCallback(state, empty_context);
    return;
  }

  // Build the lookup request.
  absl::StatusOr<std::shared_ptr<LookupServiceRequest>> request_or =
      CreateLookupRequest(*state, group, *key_context.response);

  if (!request_or.ok()) {
    LOG_ERROR(*state->match_context.logger,
              "Failed to create lookup request: %v", request_or.status());
    // Record request-level failure and wind down.
    auto failed_context =
        state->match_context
            .CreateAsyncContext<LookupServiceRequest, LookupServiceResponse>();
    failed_context.status = request_or.status();
    OnLookupCallback(state, failed_context);
    return;
  }
  std::shared_ptr<LookupServiceRequest> lookup_request = std::move(*request_or);

  // Optimization: Skip empty calls.
  if (lookup_request->data_records().empty()) {
    auto empty_context =
        state->match_context
            .CreateAsyncContext<LookupServiceRequest, LookupServiceResponse>();
    empty_context.response = std::make_shared<LookupServiceResponse>();
    OnLookupCallback(state, empty_context);
    return;
  }

  auto lookup_context =
      state->match_context
          .CreateAsyncContext<LookupServiceRequest, LookupServiceResponse>();
  lookup_context.request = std::move(lookup_request);
  lookup_context.callback =
      absl::bind_front(&KmsEncryptedMatchTask::OnLookupCallback, this, state);
  lookup_service_client_->Lookup(lookup_context);
}

void KmsEncryptedMatchTask::OnLookupCallback(
    std::shared_ptr<MatchRequestContext> state,
    AsyncContext<LookupServiceRequest, LookupServiceResponse>&
        lookup_context) noexcept {
  bool is_last = false;

  {
    absl::MutexLock lock(&state->mutex);
    if (!lookup_context.status.ok()) {
      state->RecordFailureHoldingLock(lookup_context.status);
      LOG_ERROR(*state->match_context.logger, "Lookup failed: %v",
                lookup_context.status);
    } else {
      state->lookup_responses.push_back(lookup_context.response);
    }

    state->pending_groups--;
    if (state->pending_groups == 0) is_last = true;
  }

  if (is_last) {
    absl::Status final_status;
    {
      absl::MutexLock lock(&state->mutex);
      final_status = state->status;
    }
    if (!final_status.ok()) {
      state->match_context.status = final_status;
      state->match_context.Finish();
      return;
    }

    // Merge everything
    if (auto status =
            BuildMatchResponse(state->lookup_responses, state->decrypted_values,
                               *state->match_response);
        !status.ok()) {
      state->match_context.status = status;
      state->match_context.Finish();
      return;
    }

    state->match_context.response = state->match_response;
    state->match_context.Finish();
  }
}

void KmsEncryptedMatchTask::MatchRequestContext::RecordFailureHoldingLock(
    absl::Status failed_status) noexcept {
  if (failed_status == absl::OkStatus()) {
    failed_status =
        Status(Error::INTERNAL_ERROR,
               "Expected failure to write to match context, but got ok.");
    LOG_ERROR(*match_context.logger,
              "RecordFailure provided with invalid argument, should not "
              "happen. Falling back to internal error: %v",
              failed_status);
  }

  if (this->status.ok()) {
    // Assign the first failure status directly.
    this->status = failed_status;
  } else {
    // Append the message, preserving the status code from the first error.
    this->status = Annotate(
        this->status, absl::StrFormat("Additional failure: %v", failed_status));
  }
}

}  // namespace google::confidential_match::match_service
