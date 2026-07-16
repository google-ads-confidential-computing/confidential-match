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

#include "cc/match_service/tasks/hashed_match_task.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "cc/common/proto_utils/proto_utils.h"
#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/converters/lookup_associated_data_converter.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/tasks/associated_data_collector.h"
#include "cc/match_service/tasks/normalization_utils.h"
#include "cc/match_service/validators/match_key_encoding_validator.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::AssociatedData;
using ::google::confidential_match::match_service::backend::AssociatedDataType;
using ::google::confidential_match::match_service::backend::CompositeField;
using ::google::confidential_match::match_service::backend::CompositeFieldType;
using ::google::confidential_match::match_service::backend::
    CompositeFieldType_Name;
using ::google::confidential_match::match_service::backend::DataRecord;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::ErrorReason;
using ::google::confidential_match::match_service::backend::FanOutField;
using ::google::confidential_match::match_service::backend::Field;
using ::google::confidential_match::match_service::backend::FieldType;
using ::google::confidential_match::match_service::backend::FieldType_Name;
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
using ::google::protobuf::RepeatedPtrField;

// The number of expected fields for an address composite field.
constexpr int64_t kAddressFieldSize = 4;
// The key within LookupDataRecord metadata tracking the index of the
// data record from the original match service request.
constexpr absl::string_view kDataRecordIndexKey = "d";
// The key within LookupDataRecord metadata tracking the index of the
// match key from the original match service request.
constexpr absl::string_view kMatchKeyIndexKey = "m";

// Converts a match key from the provided encoding to base-64 encoding.
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
      decoded_match_key = absl::HexStringToBytes(match_key);
      break;
    default:
      return Status(Error::INVALID_MATCH_KEY_ENCODING,
                    "Match key encoding is not supported.");
  }

  return absl::Base64Escape(decoded_match_key);
}

// Returns an index within [0, 4) providing the order in which address fields
// are concatenated, or std::nullopt if the field is not an address field.
absl::StatusOr<int> GetAddressFieldIndex(FieldType field_type) {
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
      return Status(
          Error::INVALID_MATCH_KEY_FIELD,
          absl::StrFormat("Field type '%s' is not supported for addresses.",
                          FieldType_Name(field_type)));
  }
}

// Computes the combined lookup key for an address composite field.
absl::StatusOr<std::string> GetAddressLookupKey(
    const MatchedCompositeField& matched_composite_field,
    const HasherInterface& sha256_hasher) {
  std::vector<std::string> address_parts(kAddressFieldSize);
  for (const auto& field_info : matched_composite_field.matched_field_info()) {
    int i;
    ASSIGN_OR_RETURN(i, GetAddressFieldIndex(field_info.field_type()));
    address_parts[i] = field_info.field_value();
  }

  int country_idx;
  ASSIGN_OR_RETURN(country_idx,
                   GetAddressFieldIndex(FieldType::FIELD_TYPE_COUNTRY_CODE));
  int zip_idx;
  ASSIGN_OR_RETURN(zip_idx,
                   GetAddressFieldIndex(FieldType::FIELD_TYPE_ZIP_CODE));

  // Normalize country and zip code using MRP-aligned logic.
  // We use the original country code for zip code normalization to match MRP.
  std::string raw_country = address_parts[country_idx];
  address_parts[country_idx] = NormalizeCountryCode(raw_country);
  address_parts[zip_idx] =
      NormalizeZipCode(address_parts[zip_idx], raw_country);

  std::string concatenated_address = absl::StrJoin(address_parts, "");
  return sha256_hasher.Base64EncodedHash(concatenated_address);
}

// Tracks the location of a MatchKey within a MatchRequest.
struct MatchKeyIndex {
  // The index of the data record containing the match key.
  int data_records_index;
  // The index of the match key within the data record's match keys.
  int match_keys_index;
};

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

// Extracts the match key index from a LookupServiceResponse metadata.
absl::StatusOr<MatchKeyIndex> ReadMatchKeyIndexFromMetadata(
    const RepeatedPtrField<KeyValue>& metadata) {
  MatchKeyIndex match_key_index;
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

// Converts a field to a LookupDataRecord.
absl::Status ToDataRecord(const MatchedField& matched_field,
                          LookupDataRecord& lookup_data_record) {
  lookup_data_record.Clear();
  *lookup_data_record.mutable_lookup_key()->mutable_key() =
      matched_field.matched_field_info().field_value();
  return absl::OkStatus();
}

// Converts a composite field into a LookupDataRecord.
absl::Status ToDataRecord(const CompositeField& composite_field,
                          const MatchedCompositeField& matched_composite_field,
                          const HasherInterface& sha256_hasher,
                          LookupDataRecord& lookup_data_record) {
  lookup_data_record.Clear();

  if (composite_field.type() ==
      CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS) {
    ASSIGN_OR_RETURN(
        *lookup_data_record.mutable_lookup_key()->mutable_key(),
        GetAddressLookupKey(matched_composite_field, sha256_hasher));
  } else {
    // This branch should be unreachable. Validation in
    // ToMatchedCompositeField() guarantees that only supported composite fields
    // (i.e. addresses) reach this conversion step.
    return Status(Error::INTERNAL_ERROR,
                  "Unexpected composite field type in ToDataRecord.");
  }

  return absl::OkStatus();
}

// Copies over all shared information from a Field to a MatchedField.
// MatchedFieldInfo.field_value will be converted to base64 from the request
// field value.
absl::Status ToMatchedField(const Field& field, MatchKeyEncoding key_encoding,
                            MatchedField& matched_field) noexcept {
  matched_field.Clear();
  matched_field.mutable_matched_field_info()->set_field_type(field.type());

  switch (field.type()) {
    case FieldType::FIELD_TYPE_EMAIL:
    case FieldType::FIELD_TYPE_PHONE: {
      RETURN_IF_ERROR(ValidateMatchKeyEncoding(key_encoding, field.value()));
      ASSIGN_OR_RETURN(
          *matched_field.mutable_matched_field_info()->mutable_field_value(),
          ConvertToBase64(field.value(), key_encoding));
      break;
    }
    default:
      return Status(Error::INVALID_MATCH_KEY_FIELD,
                    "The only supported types for fields are email and phone.");
  }

  return absl::OkStatus();
}

// Copies over all shared information from a Field to a MatchedCompositeField.
// MatchedFieldInfo.field_value will be converted to base64 from the request
// field value.
absl::Status ToMatchedCompositeField(
    const CompositeField& composite_field, MatchKeyEncoding key_encoding,
    MatchedCompositeField& matched_composite_field) noexcept {
  matched_composite_field.Clear();
  switch (composite_field.type()) {
    case CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS: {
      if (composite_field.values_size() != kAddressFieldSize) {
        return Status(
            Error::INVALID_MATCH_KEY_FIELD,
            absl::StrFormat("Addresses must contain exactly %d fields.",
                            kAddressFieldSize));
      }

      std::vector<bool> is_index_used(kAddressFieldSize, false);
      for (const auto& field : composite_field.values()) {
        int i;
        ASSIGN_OR_RETURN(i, GetAddressFieldIndex(field.type()));
        if (is_index_used[i]) {
          return Status(Error::INVALID_MATCH_KEY_FIELD,
                        absl::StrFormat(
                            "Field type '%s' can't be provided more than once.",
                            FieldType_Name(field.type())));
        }
        is_index_used[i] = true;
      }
      break;
    }
    case CompositeFieldType::COMPOSITE_FIELD_TYPE_UNSPECIFIED:
      return Status(Error::INVALID_MATCH_KEY_FIELD,
                    "The composite field type must be specified.");
    default:
      return Status(
          Error::INVALID_MATCH_KEY_FIELD,
          absl::StrCat("Got an unsupported composite field type: ",
                       CompositeFieldType_Name(composite_field.type())));
  }

  // Initialize all matched field info entries to preserve structure.
  for (const auto& field : composite_field.values()) {
    MatchedFieldInfo* matched_field_info =
        matched_composite_field.add_matched_field_info();
    matched_field_info->set_field_type(field.type());
  }

  // Convert values.
  for (int i = 0; i < composite_field.values_size(); ++i) {
    const auto& field = composite_field.values(i);
    MatchedFieldInfo* matched_field_info =
        matched_composite_field.mutable_matched_field_info(i);
    switch (field.type()) {
      case FieldType::FIELD_TYPE_FIRST_NAME:
      case FieldType::FIELD_TYPE_LAST_NAME: {
        RETURN_IF_ERROR(ValidateMatchKeyEncoding(key_encoding, field.value()));
        ASSIGN_OR_RETURN(*matched_field_info->mutable_field_value(),
                         ConvertToBase64(field.value(), key_encoding));
        break;
      }
      default:
        matched_field_info->set_field_value(field.value());
    }
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
                          MatchKeyEncoding key_encoding,
                          MatchedKey& matched_key) noexcept {
  *matched_key.mutable_metadata() = match_key.metadata();

  switch (match_key.field_info_case()) {
    case MatchKey::kField:
      RETURN_IF_ERROR(ToMatchedField(match_key.field(), key_encoding,
                                     *matched_key.mutable_field()));
      break;
    case MatchKey::kCompositeField:
      RETURN_IF_ERROR(
          ToMatchedCompositeField(match_key.composite_field(), key_encoding,
                                  *matched_key.mutable_composite_field()));
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

// Converts a Lookup Service result status to a Match Service error reason
// if an error occurred. Otherwise returns std::nullopt.
std::optional<ErrorReason> ToErrorReason(LookupResult::Status status) {
  switch (status) {
    case LookupResult::STATUS_SUCCESS:
      return std::nullopt;
    case LookupResult::STATUS_FAILED:
      return ErrorReason::ERROR_REASON_INTERNAL_ERROR;
    default:
      LOG_ERROR(GlobalLogger(), "Got unexpected status from Lookup Service: %s",
                LookupResult::Status_Name(status));
      return ErrorReason::ERROR_REASON_INTERNAL_ERROR;
  }
}

// Copies over match results from a LookupResult to a MatchedField.
// Assumes the matched field has had relevant fields copied from the request.
absl::Status AddLookupResultsToField(
    const LookupResult& lookup_result,
    AssociatedDataCollector& associated_data_collector,
    MatchedField& matched_field) noexcept {
  if (matched_field.has_status()) {
    // Shouldn't happen, we should only write to each matched field once.
    return Status(Error::INTERNAL_ERROR,
                  absl::StrFormat(
                      "Lookup results have already been written to "
                      "this matched field, not overwriting. Lookup result: %s",
                      common::ProtoUtils::TextProtoString(lookup_result)));
  }

  // Set the status and error reason.
  std::optional<ErrorReason> error_reason_opt =
      ToErrorReason(lookup_result.status());
  if (error_reason_opt.has_value()) {
    LOG_WARNING(GlobalLogger(), "Lookup result failed with error response: %s",
                lookup_result.error_response());
    matched_field.set_status(backend::STATUS_FAILED);
    matched_field.set_error_reason(error_reason_opt.value());
  } else {
    if (lookup_result.matched_data_records_size() > 0) {
      matched_field.set_status(backend::STATUS_SUCCESS_MATCHED);
    } else {
      matched_field.set_status(backend::STATUS_SUCCESS_UNMATCHED);
    }
  }

  MatchedFieldInfo* matched_field_info =
      matched_field.mutable_matched_field_info();
  // Redaction for non-matches.
  if (matched_field.status() != backend::STATUS_SUCCESS_MATCHED) {
    matched_field_info->clear_field_value();
  }

  // Add all associated data.
  for (const auto& lookup_matched_data_record :
       lookup_result.matched_data_records()) {
    if (lookup_matched_data_record.associated_data_size() > 0) {
      AssociatedData associated_data;
      RETURN_IF_ERROR(ToMatchService(
          lookup_matched_data_record.associated_data(), associated_data));
      int index = associated_data_collector.Add(associated_data);
      matched_field.add_matched_associated_data_indices(index);
    }
  }

  return absl::OkStatus();
}

// Copies over match results from a LookupResult to a MatchedCompositeField.
// Assumes the matched field has had relevant fields copied from the request.
absl::Status AddLookupResultsToCompositeField(
    const LookupResult& lookup_result,
    AssociatedDataCollector& associated_data_collector,
    MatchedCompositeField& matched_composite_field) noexcept {
  if (matched_composite_field.has_status()) {
    // Shouldn't happen, we should only write to each matched field once.
    return Status(
        Error::INTERNAL_ERROR,
        absl::StrFormat(
            "Lookup results have already been written to "
            "this matched composite field, not overwriting. Lookup result: %s",
            common::ProtoUtils::TextProtoString(lookup_result)));
  }

  // Set the status and error reason.
  std::optional<ErrorReason> error_reason_opt =
      ToErrorReason(lookup_result.status());
  if (error_reason_opt.has_value()) {
    LOG_WARNING(GlobalLogger(), "Lookup result failed with error response: %s",
                lookup_result.error_response());
    matched_composite_field.set_status(backend::STATUS_FAILED);
    matched_composite_field.set_error_reason(error_reason_opt.value());
  } else {
    if (lookup_result.matched_data_records_size() > 0) {
      matched_composite_field.set_status(backend::STATUS_SUCCESS_MATCHED);
    } else {
      matched_composite_field.set_status(backend::STATUS_SUCCESS_UNMATCHED);
    }
  }

  if (matched_composite_field.status() != backend::STATUS_SUCCESS_MATCHED) {
    for (int i = 0; i < matched_composite_field.matched_field_info_size();
         ++i) {
      MatchedFieldInfo* matched_field_info =
          matched_composite_field.mutable_matched_field_info(i);
      // Redaction for non-matches.
      matched_field_info->clear_field_value();
    }
  }

  // Add all associated data.
  for (const auto& lookup_matched_data_record :
       lookup_result.matched_data_records()) {
    if (lookup_matched_data_record.associated_data_size() > 0) {
      AssociatedData associated_data;
      RETURN_IF_ERROR(ToMatchService(
          lookup_matched_data_record.associated_data(), associated_data));
      int index = associated_data_collector.Add(associated_data);
      matched_composite_field.add_matched_associated_data_indices(index);
    }
  }

  return absl::OkStatus();
}

// Copies over match results from a LookupResult to a MatchedFanOutField.
// Assumes the matched field has had relevant fields copied from the request.
absl::Status AddLookupResultsToFanOutField(
    const LookupResult& lookup_result,
    AssociatedDataCollector& associated_data_collector,
    MatchedFanOutField& matched_fan_out_field) noexcept {
  return Status(Error::INTERNAL_ERROR, "Fan-out key fields are not supported.");
}

// Copies over match results from a LookupResult to a MatchedKey.
// Assumes the matched field has had relevant fields copied from the request.
absl::Status AddLookupResultsToMatchedKey(
    const LookupResult& lookup_result,
    AssociatedDataCollector& associated_data_collector,
    MatchedKey& matched_key) noexcept {
  switch (matched_key.field_info_case()) {
    case MatchedKey::kField:
      RETURN_IF_ERROR(AddLookupResultsToField(lookup_result,
                                              associated_data_collector,
                                              *matched_key.mutable_field()));
      break;
    case MatchedKey::kCompositeField:
      RETURN_IF_ERROR(AddLookupResultsToCompositeField(
          lookup_result, associated_data_collector,
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

// Writes a failed status and error reason to a matched key, and clears PII
// values in the composite field.
void SetMatchedKeyAsFailed(ErrorReason error_reason, MatchedKey& matched_key) {
  switch (matched_key.field_info_case()) {
    case MatchedKey::kField:
      matched_key.mutable_field()->set_status(backend::STATUS_FAILED);
      matched_key.mutable_field()->set_error_reason(error_reason);
      matched_key.mutable_field()
          ->mutable_matched_field_info()
          ->clear_field_value();
      break;
    case MatchedKey::kCompositeField:
      matched_key.mutable_composite_field()->set_status(backend::STATUS_FAILED);
      matched_key.mutable_composite_field()->set_error_reason(error_reason);
      // Clear all field values to avoid PII leak
      for (auto& info : *matched_key.mutable_composite_field()
                             ->mutable_matched_field_info()) {
        info.clear_field_value();
      }
      break;
    case MatchedKey::kFanOutKeyField:
      matched_key.mutable_fan_out_key_field()->set_status(
          backend::STATUS_FAILED);
      matched_key.mutable_fan_out_key_field()->set_error_reason(error_reason);
      break;
    case MatchedKey::FIELD_INFO_NOT_SET:
      matched_key.mutable_field()->set_status(backend::STATUS_FAILED);
      matched_key.mutable_field()->set_error_reason(error_reason);
      // No value to clear, so we don't instantiate matched_field_info
      break;
  }
}

// Helper to check if a matched key has a failed status.
bool HasFailedStatus(const MatchedKey& matched_key) {
  return (matched_key.has_field() &&
          matched_key.field().status() == backend::STATUS_FAILED) ||
         (matched_key.has_composite_field() &&
          matched_key.composite_field().status() == backend::STATUS_FAILED) ||
         (matched_key.has_fan_out_key_field() &&
          matched_key.fan_out_key_field().status() == backend::STATUS_FAILED);
}

// Hydrates the MatchResponse with results from the Lookup Service.
absl::Status HydrateMatchResponse(const LookupServiceResponse& lookup_response,
                                  MatchResponse& match_response) noexcept {
  AssociatedDataCollector associated_data_collector;

  // Sanity check that the size matches, failing means an error in the system.
  int64_t expected_lookup_record_count = 0;
  for (const auto& data_record : match_response.matched_data_records()) {
    for (const auto& match_key : data_record.matched_keys()) {
      if (!HasFailedStatus(match_key)) {
        ++expected_lookup_record_count;
      }
    }
  }

  // Sanity check that the size matches, failing means an error in the system.
  if (expected_lookup_record_count != lookup_response.lookup_results_size()) {
    return Status(
        Error::LOOKUP_SERVICE_ERROR,
        absl::StrFormat("The number of lookup service results (%d) did not "
                        "match the number of match keys sent (%d).",
                        lookup_response.lookup_results_size(),
                        expected_lookup_record_count));
  }

  // Stitch the match results from the Lookup Service response into the match
  // response using the indices set in the LS response metadata.
  for (const auto& lookup_result : lookup_response.lookup_results()) {
    MatchKeyIndex index;
    ASSIGN_OR_RETURN(index, ReadMatchKeyIndexFromMetadata(
                                lookup_result.client_data_record().metadata()));

    if (index.data_records_index < 0 ||
        index.data_records_index >=
            match_response.matched_data_records_size()) {
      return Status(
          Error::LOOKUP_SERVICE_ERROR,
          absl::StrFormat("Found invalid data records index (%d) in metadata, "
                          "max size is %d.",
                          index.data_records_index,
                          match_response.matched_data_records_size()));
    }
    MatchedDataRecord* matched_data_record =
        match_response.mutable_matched_data_records(index.data_records_index);

    if (index.match_keys_index < 0 ||
        index.match_keys_index >= matched_data_record->matched_keys_size()) {
      return Status(
          Error::LOOKUP_SERVICE_ERROR,
          absl::StrFormat("Found invalid matched keys index (%d) in metadata, "
                          "max size is %d.",
                          index.match_keys_index,
                          matched_data_record->matched_keys_size()));
    }
    MatchedKey* matched_key =
        matched_data_record->mutable_matched_keys(index.match_keys_index);

    RETURN_IF_ERROR(AddLookupResultsToMatchedKey(
        lookup_result, associated_data_collector, *matched_key));
  }

  // Add the top-level associated data items.
  for (AssociatedData& associated_data : associated_data_collector.Get()) {
    *match_response.add_matched_associated_data() = std::move(associated_data);
  }

  return absl::OkStatus();
}

absl::Status ValidateRequest(const MatchRequest& request) {
  if (request.match_key_format() != MatchKeyFormat::MATCH_KEY_FORMAT_HASHED) {
    return Status(Error::INVALID_MATCH_KEY_FORMAT,
                  "Match key format must be hashed.");
  }
  if (request.has_encryption_key()) {
    return Status(Error::INVALID_MATCH_KEY_FORMAT,
                  "Encryption keys must not be set for hashed requests.");
  }
  if (request.key_encoding() ==
      MatchKeyEncoding::MATCH_KEY_ENCODING_UNSPECIFIED) {
    return Status(Error::INVALID_MATCH_KEY_ENCODING,
                  "Match key encoding must be specified.");
  }
  return absl::OkStatus();
}

bool ProcessMatchKey(const MatchKey& match_key, MatchKeyEncoding key_encoding,
                     const HasherInterface& sha256_hasher,
                     MatchedKey& matched_key,
                     LookupDataRecord& lookup_data_record,
                     LoggerInterface& logger) {
  absl::Status status = ToMatchedKey(match_key, key_encoding, matched_key);

  if (!status.ok()) {
    auto backend_error = GetBackendErrorReason(status);
    if (backend_error == Error::INVALID_MATCH_KEY_ENCODING) {
      SetMatchedKeyAsFailed(ErrorReason::ERROR_REASON_DECODING_ERROR,
                            matched_key);
    } else if (backend_error == Error::INVALID_MATCH_KEY_FIELD) {
      SetMatchedKeyAsFailed(ErrorReason::ERROR_REASON_INVALID_MATCH_KEY_FIELD,
                            matched_key);
    } else {  // Catch all for other errors.
      SetMatchedKeyAsFailed(ErrorReason::ERROR_REASON_INTERNAL_ERROR,
                            matched_key);
    }
    return false;
  }

  absl::Status data_record_status;
  if (match_key.has_field()) {
    data_record_status = ToDataRecord(matched_key.field(), lookup_data_record);
  } else if (match_key.has_composite_field()) {
    data_record_status =
        ToDataRecord(match_key.composite_field(), matched_key.composite_field(),
                     sha256_hasher, lookup_data_record);
  } else {
    // This is not expected to happen here, since ToMatchedKey already
    // validated these cases.
    LOG_ERROR(logger, "Unexpected match key field type: %d",
              match_key.field_info_case());
    SetMatchedKeyAsFailed(ErrorReason::ERROR_REASON_INTERNAL_ERROR,
                          matched_key);
    return false;
  }

  if (!data_record_status.ok()) {
    LOG_ERROR(logger, "Failed to convert to backend data record: %v",
              data_record_status);
    SetMatchedKeyAsFailed(ErrorReason::ERROR_REASON_INTERNAL_ERROR,
                          matched_key);
    return false;
  }

  return true;
}

absl::Status AddAssociatedDataTypesToLookupRequest(
    const MatchRequest& request, LookupServiceRequest& lookup_request) {
  if (request.associated_data_types_size() > 0) {
    for (const int associated_data_type : request.associated_data_types()) {
      RETURN_IF_ERROR(
          ToLookup(static_cast<AssociatedDataType>(associated_data_type),
                   *lookup_request.add_associated_data_keys()));
    }
  }
  return absl::OkStatus();
}

absl::Status BuildLookupRequestAndPreFillResponse(
    const MatchRequest& request, const HasherInterface& sha256_hasher,
    MatchResponse& match_response, LookupServiceRequest& lookup_request,
    LoggerInterface& logger) {
  lookup_request.set_key_format(LookupServiceRequest::KEY_FORMAT_HASHED);
  lookup_request.mutable_hash_info()->set_hash_type(
      LookupServiceRequest::HashInfo::HASH_TYPE_SHA_256);
  lookup_request.set_application(request.application());

  int64_t data_record_idx = 0;
  for (const auto& data_record : request.data_records()) {
    MatchedDataRecord* matched_data_record =
        match_response.add_matched_data_records();
    *matched_data_record->mutable_metadata() = data_record.metadata();

    int64_t match_key_idx = 0;
    for (const auto& match_key : data_record.match_keys()) {
      MatchedKey* matched_key = matched_data_record->add_matched_keys();
      LookupDataRecord temp_lookup_data_record;

      if (ProcessMatchKey(match_key, request.key_encoding(), sha256_hasher,
                          *matched_key, temp_lookup_data_record, logger)) {
        LookupDataRecord* lookup_data_record =
            lookup_request.add_data_records();
        *lookup_data_record = std::move(temp_lookup_data_record);
        WriteMatchKeyIndexToMetadata(*lookup_data_record->mutable_metadata(),
                                     data_record_idx, match_key_idx);
      }
      ++match_key_idx;
    }
    ++data_record_idx;
  }

  return AddAssociatedDataTypesToLookupRequest(request, lookup_request);
}

}  // namespace

void HashedMatchTask::Match(
    AsyncContext<MatchRequest, MatchResponse> context) noexcept {
  LOG_INFO(*context.logger, "Processing match request...");

  absl::Status validation_status = ValidateRequest(*context.request);
  if (!validation_status.ok()) {
    context.status = validation_status;
    context.Finish();
    return;
  }

  auto match_response = std::make_shared<MatchResponse>();
  *match_response->mutable_metadata() = context.request->metadata();

  auto lookup_request = std::make_shared<LookupServiceRequest>();
  absl::Status build_status = BuildLookupRequestAndPreFillResponse(
      *context.request, *sha256_hasher_, *match_response, *lookup_request,
      *context.logger);
  if (!build_status.ok()) {
    context.status = build_status;
    context.Finish();
    return;
  }

  context.response = match_response;  // Pre-fill!

  // If there are no valid data records to lookup, we can short-circuit the
  // request.
  if (lookup_request->data_records_size() == 0) {
    LOG_WARNING(*context.logger,
                "No valid data records to lookup. Short-circuiting request.");
    context.Finish();
    return;
  }

  auto lookup_context =
      context.CreateAsyncContext<LookupServiceRequest, LookupServiceResponse>();
  lookup_context.request = std::move(lookup_request);
  lookup_context.callback =
      absl::bind_front(&HashedMatchTask::OnLookupCallback, this, context);
  lookup_service_client_->Lookup(lookup_context);
}

void HashedMatchTask::OnLookupCallback(
    AsyncContext<MatchRequest, MatchResponse> match_context,
    AsyncContext<LookupServiceRequest, LookupServiceResponse>&
        lookup_context) noexcept {
  LOG_INFO(*match_context.logger, "Processing match request callback...");
  if (!lookup_context.status.ok()) {
    LOG_ERROR(*match_context.logger,
              "Lookup Service matching failed with an error: %v",
              lookup_context.status);
    match_context.status = lookup_context.status;
    match_context.Finish();
    return;
  }

  absl::Status status =
      HydrateMatchResponse(*lookup_context.response, *match_context.response);
  if (!status.ok()) {
    LOG_ERROR(*match_context.logger,
              "Failed to construct the Match Service response: %v", status);
    match_context.status = status;
  }
  match_context.Finish();
}

}  // namespace google::confidential_match::match_service
