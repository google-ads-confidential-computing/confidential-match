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

#include "cc/match_service/converters/lookup_request_converter.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "cc/common/proto_utils/proto_utils.h"
#include "cc/core/error/status_macros.h"
#include "cc/match_service/error/error.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::EncryptionKey;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::KeyType;
using ::google::confidential_match::match_service::backend::
    LookupServiceRequest;
using ::google::confidential_match::match_service::backend::
    LookupServiceResponse;

using ApiLookupRequest =
    ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ApiEncryptionKey =
    ::google::confidential_match::lookup_server::proto_api::EncryptionKeyInfo;
using ApiLookupResult =
    ::google::confidential_match::lookup_server::proto_api::LookupResult;

absl::StatusOr<ApiLookupRequest::KeyFormat> ToLookupApi(
    const LookupServiceRequest::KeyFormat input) noexcept {
  switch (input) {
    case LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED:
      return ApiLookupRequest::KEY_FORMAT_HASHED_ENCRYPTED;
    case LookupServiceRequest::KEY_FORMAT_HASHED:
      return ApiLookupRequest::KEY_FORMAT_HASHED;
    default: {
      return Status(Error::CONVERTER_PARSE_ERROR,
                    "Invalid backend key format.");
    }
  }
}

absl::StatusOr<ApiLookupRequest::HashInfo::HashType> ToLookupApi(
    const LookupServiceRequest::HashInfo::HashType input) noexcept {
  switch (input) {
    case LookupServiceRequest::HashInfo::HASH_TYPE_SHA_256:
      return ApiLookupRequest::HashInfo::HASH_TYPE_SHA_256;
    default:
      return Status(Error::CONVERTER_PARSE_ERROR, "Invalid backend hash type.");
  }
}

absl::StatusOr<ApiEncryptionKey::WrappedKeyInfo::KeyType> ToLookupApi(
    const KeyType input) noexcept {
  switch (input) {
    case KeyType::KEY_TYPE_XCHACHA20_POLY1305:
      return ApiEncryptionKey::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305;
    default:
      return Status(Error::CONVERTER_PARSE_ERROR, "Invalid backend key type.");
  }
}

absl::Status ToLookupApi(const backend::KeyValue& input,
                         lookup_server::proto_api::KeyValue& output) noexcept {
  if (!input.has_key() || input.key().empty()) {
    return Status(Error::CONVERTER_PARSE_ERROR,
                  "Backend keyValue does not contain key.");
  }
  switch (input.value_case()) {
    case backend::KeyValue::ValueCase::kStringValue:
      output.set_string_value(input.string_value());
      break;
    case backend::KeyValue::ValueCase::kIntValue:
      output.set_int_value(input.int_value());
      break;
    case backend::KeyValue::ValueCase::kDoubleValue:
      output.set_double_value(input.double_value());
      break;
    case backend::KeyValue::ValueCase::kBoolValue:
      output.set_bool_value(input.bool_value());
      break;
    case backend::KeyValue::ValueCase::kBytesValue:
      output.set_bytes_value(input.bytes_value());
      break;
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Invalid keyValue value_case: ",
                                 static_cast<int>(input.value_case())));
  }
  output.set_key(input.key());
  return absl::OkStatus();
}

absl::Status ToLookupApi(const EncryptionKey& input,
                         ApiEncryptionKey& output) noexcept {
  if (input.has_wrapped_key()) {
    auto* out_wrapped = output.mutable_wrapped_key_info();
    const auto& in_wrapped = input.wrapped_key();

    auto key_type_or = ToLookupApi(in_wrapped.key_type());
    if (!key_type_or.ok()) return key_type_or.status();
    out_wrapped->set_key_type(*key_type_or);

    if (in_wrapped.encrypted_dek().empty()) {
      return Status(Error::CONVERTER_PARSE_ERROR,
                    "Backend WrappedKey missing encrypted dek");
    }
    out_wrapped->set_encrypted_dek(in_wrapped.encrypted_dek());

    if (in_wrapped.kek_kms_resource_id().empty()) {
      return Status(Error::CONVERTER_PARSE_ERROR,
                    "Backend WrappedKey missing kek");
    }
    out_wrapped->set_kek_kms_resource_id(in_wrapped.kek_kms_resource_id());

    switch (in_wrapped.cloud_wrapped_key_info_case()) {
      case backend::WrappedKey::CloudWrappedKeyInfoCase::kAwsWrappedKeyInfo: {
        auto* out_aws = out_wrapped->mutable_aws_wrapped_key_info();
        const auto& in_aws = in_wrapped.aws_wrapped_key_info();
        if (in_aws.role_arn().empty()) {
          return Status(Error::CONVERTER_PARSE_ERROR,
                        "Backend AwsWrappedKeyInfo missing role arn");
        }
        out_aws->set_role_arn(in_aws.role_arn());
        *out_aws->mutable_signatures() = in_aws.signatures();
        break;
      }
      case backend::WrappedKey::CloudWrappedKeyInfoCase::kGcpWrappedKeyInfo: {
        auto* out_gcp = out_wrapped->mutable_gcp_wrapped_key_info();
        const auto& in_gcp = in_wrapped.gcp_wrapped_key_info();
        if (in_gcp.wip_provider().empty()) {
          return Status(Error::CONVERTER_PARSE_ERROR,
                        "Backend GcpWrappedKeyInfo missing wip provider");
        }
        out_gcp->set_wip_provider(in_gcp.wip_provider());
        break;
      }
      case backend::WrappedKey::CloudWrappedKeyInfoCase::
          CLOUD_WRAPPED_KEY_INFO_NOT_SET:
        break;
      default:
        return Status(Error::CONVERTER_PARSE_ERROR,
                      "Invalid CloudWrappedKeyInfo case");
    }
  } else if (input.has_coordinator_key()) {
    auto* out_coord_key = output.mutable_coordinator_key_info();
    const auto& in_coord_key = input.coordinator_key();
    if (in_coord_key.key_id().empty()) {
      return Status(Error::CONVERTER_PARSE_ERROR,
                    "Backend CoordinatorKey missing key_id.");
    }
    if (in_coord_key.coordinator_info().empty()) {
      return Status(Error::CONVERTER_PARSE_ERROR,
                    "Backend CoordinatorKey missing coordinator_info.");
    }
    for (const auto& info : in_coord_key.coordinator_info()) {
      auto& out_info = *out_coord_key->add_coordinator_info();
      if (info.key_service_endpoint().empty()) {
        return Status(Error::CONVERTER_PARSE_ERROR,
                      "Backend CoordinatorKey missing key_service_endpoint.");
      }
      out_info.set_key_service_endpoint(info.key_service_endpoint());
      if (info.kms_wip_provider().empty()) {
        return Status(Error::CONVERTER_PARSE_ERROR,
                      "Backend CoordinatorKey missing kms_wip_provider.");
      }
      out_info.set_kms_wip_provider(info.kms_wip_provider());
      if (info.key_service_audience_url().empty()) {
        return Status(
            Error::CONVERTER_PARSE_ERROR,
            "Backend CoordinatorKey missing key_service_audience_url.");
      }
      out_info.set_key_service_audience_url(info.key_service_audience_url());
    }
    out_coord_key->set_key_id(in_coord_key.key_id());
  }
  return absl::OkStatus();
}

absl::Status ToLookupApi(
    const backend::LookupDataRecord& input,
    lookup_server::proto_api::DataRecord& output) noexcept {
  if (!input.has_lookup_key() || input.lookup_key().key().empty()) {
    return Status(Error::CONVERTER_PARSE_ERROR,
                  "Backend input does not have lookup key.");
  }
  output.mutable_lookup_key()->set_key(input.lookup_key().key());

  if (input.metadata_size() > 0) {
    output.mutable_metadata()->Reserve(input.metadata_size());
    for (const auto& kv : input.metadata()) {
      RETURN_IF_ERROR(ToLookupApi(kv, *output.add_metadata()));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<backend::LookupResult::Status> ToBackend(
    const ApiLookupResult::Status input) noexcept {
  switch (input) {
    case ApiLookupResult::STATUS_SUCCESS:
      return backend::LookupResult::STATUS_SUCCESS;
    case ApiLookupResult::STATUS_FAILED:
      return backend::LookupResult::STATUS_FAILED;
    default: {
      return Status(
          Error::INTERNAL_ERROR,
          absl::StrCat("Failed to parse LookupService response status: ",
                       input));
    }
  }
}

absl::Status ToBackend(const lookup_server::proto_api::KeyValue& input,
                       backend::KeyValue& output) noexcept {
  if (input.key().empty()) {
    return Status(Error::INTERNAL_ERROR,
                  "LookupService response missing key_value_pair key.");
  }

  switch (input.value_case()) {
    case lookup_server::proto_api::KeyValue::ValueCase::kStringValue:
      output.set_string_value(input.string_value());
      break;
    case lookup_server::proto_api::KeyValue::ValueCase::kIntValue:
      output.set_int_value(input.int_value());
      break;
    case lookup_server::proto_api::KeyValue::ValueCase::kDoubleValue:
      output.set_double_value(input.double_value());
      break;
    case lookup_server::proto_api::KeyValue::ValueCase::kBoolValue:
      output.set_bool_value(input.bool_value());
      break;
    case lookup_server::proto_api::KeyValue::ValueCase::kBytesValue:
      output.set_bytes_value(input.bytes_value());
      break;
    case lookup_server::proto_api::KeyValue::ValueCase::VALUE_NOT_SET:
      break;
    default:
      return Status(
          Error::INTERNAL_ERROR,
          absl::StrCat(
              "LookupService response returned invalid keyValue value_case: ",
              static_cast<int>(input.value_case())));
  }
  output.set_key(input.key());

  return absl::OkStatus();
}

absl::Status ToBackend(const lookup_server::proto_api::DataRecord& input,
                       backend::LookupDataRecord& output) noexcept {
  if (!input.has_lookup_key() || input.lookup_key().key().empty()) {
    return Status(
        Error::INTERNAL_ERROR,
        "LookupService response client data record missing lookup key.");
  }
  output.mutable_lookup_key()->set_key(input.lookup_key().key());

  if (input.metadata_size() > 0) {
    output.mutable_metadata()->Reserve(input.metadata_size());
    for (const auto& kv : input.metadata()) {
      RETURN_IF_ERROR(ToBackend(kv, *output.add_metadata()));
    }
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const lookup_server::proto_api::MatchedDataRecord& input,
                       backend::LookupMatchedDataRecord& output) noexcept {
  if (!input.has_lookup_key() || input.lookup_key().key().empty()) {
    return Status(
        Error::INTERNAL_ERROR,
        "LookupService InternalError MatchedDataRecord missing lookup key.");
  }
  output.mutable_lookup_key()->set_key(input.lookup_key().key());

  if (input.associated_data_size() > 0) {
    output.mutable_associated_data()->Reserve(input.associated_data_size());
    for (const auto& kv : input.associated_data()) {
      RETURN_IF_ERROR(ToBackend(kv, *output.add_associated_data()));
    }
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const ApiLookupResult& input,
                       backend::LookupResult& output) noexcept {
  auto status_or = ToBackend(input.status());
  if (!status_or.ok()) return status_or.status();
  output.set_status(*status_or);

  if (input.has_client_data_record()) {
    RETURN_IF_ERROR(ToBackend(input.client_data_record(),
                              *output.mutable_client_data_record()));
  }

  if (input.matched_data_records_size() > 0) {
    output.mutable_matched_data_records()->Reserve(
        input.matched_data_records_size());
    for (const auto& match : input.matched_data_records()) {
      RETURN_IF_ERROR(ToBackend(match, *output.add_matched_data_records()));
    }
  }

  if (input.has_error_response()) {
    output.set_error_response(
        common::ProtoUtils::ShortTextProtoString(input.error_response()));
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ToLookupApi(
    const backend::LookupServiceRequest& input,
    lookup_server::proto_api::LookupRequest& output) noexcept {
  if (input.data_records_size() <= 0) {
    return absl::OkStatus();
  }

  output.mutable_data_records()->Reserve(input.data_records_size());
  for (const auto& record : input.data_records()) {
    RETURN_IF_ERROR(ToLookupApi(record, *output.add_data_records()));
  }

  auto key_format_or = ToLookupApi(input.key_format());
  if (!key_format_or.ok()) {
    return key_format_or.status();
  }
  output.set_key_format(*key_format_or);

  if (!input.has_sharding_scheme()) {
    return Status(
        Error::CONVERTER_PARSE_ERROR,
        absl::StrCat("Backend lookup request missing sharding scheme."));
  }
  output.mutable_sharding_scheme()->set_type(input.sharding_scheme().type());
  output.mutable_sharding_scheme()->set_num_shards(
      input.sharding_scheme().num_shards());

  if (!input.has_hash_info()) {
    return Status(Error::CONVERTER_PARSE_ERROR,
                  absl::StrCat("Backend lookup request missing hash info."));
  }
  auto hash_type_or = ToLookupApi(input.hash_info().hash_type());
  if (!hash_type_or.ok()) {
    return hash_type_or.status();
  }
  output.mutable_hash_info()->set_hash_type(*hash_type_or);

  if (input.key_format() == LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED) {
    if (!input.has_encryption_key()) {
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          "Backend lookup request missing encryption_key for HASHED_ENCRYPTED "
          "format");
    }
    RETURN_IF_ERROR(ToLookupApi(input.encryption_key(),
                                *output.mutable_encryption_key_info()));
  }
  *output.mutable_associated_data_keys() = input.associated_data_keys();
  return absl::OkStatus();
}

absl::Status ToBackend(const lookup_server::proto_api::LookupResponse& input,
                       backend::LookupServiceResponse& output) noexcept {
  if (input.lookup_results_size() > 0) {
    output.mutable_lookup_results()->Reserve(input.lookup_results_size());
    for (const auto& result : input.lookup_results()) {
      RETURN_IF_ERROR(ToBackend(result, *output.add_lookup_results()));
    }
  }
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
