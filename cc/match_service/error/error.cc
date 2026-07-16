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

#include "cc/match_service/error/error.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "google/rpc/code.pb.h"
#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"
#include "grpcpp/grpcpp.h"

#include "cc/core/logger/log.h"
#include "protos/match_service/api/v1/error.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::api::v1::ExternalError;
using ::google::confidential_match::match_service::backend::Error;
using ::google::protobuf::EnumValueDescriptor;
using ::google::protobuf::GetEnumDescriptor;

// absl::Status payload key for the backend error reason enum.
constexpr absl::string_view kBackendErrorReasonPayloadKey =
    "BackendErrorReason";

}  // namespace

absl::Status Status(Error::Reason backend_error_reason,
                    absl::string_view backend_message) noexcept {
  const EnumValueDescriptor* backend_error_reason_descriptor =
      GetEnumDescriptor<Error::Reason>()->FindValueByNumber(
          backend_error_reason);
  if (backend_error_reason_descriptor == nullptr) {
    LOG_ERROR(GlobalLogger(),
              "Unable to find EnumValueDescriptor for Reason: %d",
              backend_error_reason);
    return Status(Error::UNKNOWN_ERROR, "Unknown backend reason.");
  }

  rpc::Code code = backend_error_reason_descriptor->options().GetExtension(
      backend::backend_rpc_code);
  if (code == rpc::Code::OK) {
    LOG_ERROR(GlobalLogger(),
              "backend_rpc_code option is unset or set to OK for Reason: %d",
              backend_error_reason);
    return Status(Error::UNKNOWN_ERROR, "Unknown backend reason.");
  }

  auto status =
      absl::Status(static_cast<absl::StatusCode>(code), backend_message);
  status.SetPayload(kBackendErrorReasonPayloadKey,
                    absl::Cord(Error::Reason_Name(backend_error_reason)));
  return status;
}

absl::Status Annotate(const absl::Status& status,
                      absl::string_view message) noexcept {
  if (message.empty()) {
    return status;
  }
  if (status.ok()) {
    // Only failed statuses can contain messages.
    return status;
  }

  std::string new_message;
  if (status.message().empty()) {
    new_message = message;
  } else {
    new_message = absl::StrCat(status.message(), "; ", message);
  }
  absl::Status new_status(status.code(), new_message);

  status.ForEachPayload(
      [&new_status](absl::string_view type_url, const absl::Cord& payload) {
        new_status.SetPayload(type_url, payload);
      });

  return new_status;
}

std::optional<Error::Reason> GetBackendErrorReason(
    const absl::Status& status) noexcept {
  std::optional<absl::Cord> backend_error_reason_cord =
      status.GetPayload(kBackendErrorReasonPayloadKey);
  if (!backend_error_reason_cord.has_value()) {
    return std::nullopt;
  }

  Error::Reason result;
  if (!Error::Reason_Parse(backend_error_reason_cord->Flatten(), &result)) {
    LOG_ERROR(GlobalLogger(),
              "Failed to parse backend error reason payload: %s",
              backend_error_reason_cord->Flatten());
    return std::nullopt;
  }
  return result;
}

std::string GetBackendErrorReasonString(const absl::Status& status) noexcept {
  auto error_reason =
      GetBackendErrorReason(status).value_or(Error::UNKNOWN_ERROR);
  return Error::Reason_Name(error_reason);
}

std::optional<std::string> GetErrorReason(const grpc::Status& status) noexcept {
  std::string error_details = status.error_details();
  if (error_details.empty()) {
    return std::nullopt;
  }
  rpc::Status rpc_status;
  if (!rpc_status.ParseFromString(error_details)) {
    return std::nullopt;
  }
  rpc::ErrorInfo error_info;
  for (const auto& detail : rpc_status.details()) {
    if (detail.UnpackTo(&error_info)) {
      return error_info.reason();
    }
  }
  return std::nullopt;
}

grpc::Status GrpcStatus(grpc::StatusCode code, absl::string_view message,
                        absl::string_view reason) noexcept {
  rpc::Status rpc_status =
      RpcStatus(static_cast<rpc::Code>(code), message, reason);
  return grpc::Status(code, std::string(message),
                      rpc_status.SerializeAsString());
}

rpc::Status RpcStatus(rpc::Code code, absl::string_view message,
                      absl::string_view reason) noexcept {
  rpc::Status status;
  status.set_code(code);
  status.set_message(message);
  rpc::ErrorInfo error_info;
  error_info.set_reason(reason);
  status.add_details()->PackFrom(error_info);
  return status;
}

namespace {

// Returns a generic INTERNAL status, to be used when a more specific status
// can't be returned.
grpc::Status InternalErrorStatus() noexcept {
  return GrpcStatus(grpc::StatusCode::INTERNAL, "An internal error occurred",
                    ExternalError::Reason_Name(ExternalError::INTERNAL_ERROR));
}

// Creates an external-facing grpc::Status message based on `error_reason` enum.
// This extracts an RPC code and message from the proto file where the enum is
// defined.
grpc::Status CreateApiStatus(ExternalError::Reason error_reason) noexcept {
  const protobuf::EnumValueDescriptor* error_reason_descriptor =
      protobuf::GetEnumDescriptor<ExternalError::Reason>()->FindValueByNumber(
          error_reason);
  if (error_reason_descriptor == nullptr) {
    LOG_ERROR(GlobalLogger(),
              "Unable to find EnumValueDescriptor for ExternalErrorReason: %d",
              error_reason);
    return InternalErrorStatus();
  }

  if (!error_reason_descriptor->options().HasExtension(
          api::v1::external_rpc_code)) {
    LOG_ERROR(GlobalLogger(),
              "external_rpc_code option is missing for ExternalErrorReason: %d",
              error_reason);
    return InternalErrorStatus();
  }
  rpc::Code external_rpc_code = error_reason_descriptor->options().GetExtension(
      api::v1::external_rpc_code);

  if (!error_reason_descriptor->options().HasExtension(
          api::v1::external_message)) {
    LOG_ERROR(GlobalLogger(),
              "external_message option is missing for ExternalErrorReason: %d",
              error_reason);
    return InternalErrorStatus();
  }
  std::string external_message =
      error_reason_descriptor->options().GetExtension(
          api::v1::external_message);

  return GrpcStatus(static_cast<grpc::StatusCode>(external_rpc_code),
                    external_message, error_reason_descriptor->name());
}

// Maps from backend error reason to external-facing API error reason.
ExternalError::Reason ToExternalErrorReason(
    const backend::Error::Reason backend_error_reason) {
  switch (backend_error_reason) {
    case backend::Error::UNKNOWN_ERROR:
    case backend::Error::INTERNAL_ERROR:
    case backend::Error::AUTH_TOKEN_CLIENT_ERROR:
    case backend::Error::LOOKUP_SERVICE_SHARD_CLIENT_ERROR:
    case backend::Error::LOOKUP_REQUEST_SERIALIZATION_ERROR:
    case backend::Error::LOOKUP_RESPONSE_DESERIALIZATION_ERROR:
    case backend::Error::ORCHESTRATOR_RESPONSE_DESERIALIZATION_ERROR:
    case backend::Error::DELETION_IN_PROGRESS:
    case backend::Error::WRONG_NUMBER_OF_KEYS:
      return ExternalError::INTERNAL_ERROR;
    case backend::Error::KEY_FETCHING_ERROR:
      return ExternalError::KEY_FETCHING_ERROR;
    case backend::Error::BASE64_DECODING_ERROR:
      return ExternalError::BASE64_DECODING_ERROR;
    case backend::Error::DECODING_ERROR:
      return ExternalError::DECODING_ERROR;
    case backend::Error::CANCELLED:
      return ExternalError::CANCELLED;
    case backend::Error::NOT_FOUND:
      return ExternalError::NOT_FOUND;
    case backend::Error::ALREADY_EXISTS:
      return ExternalError::ALREADY_EXISTS;
    case backend::Error::CONVERTER_PARSE_ERROR:
      return ExternalError::INTERNAL_ERROR;
    case backend::Error::INVALID_MATCH_KEY_FORMAT:
      return ExternalError::INVALID_MATCH_KEY_FORMAT;
    case backend::Error::ORCHESTRATOR_SERVICE_ERROR:
      return ExternalError::INTERNAL_ERROR;
    case backend::Error::LOOKUP_SERVICE_ERROR:
      return ExternalError::INTERNAL_ERROR;
    case backend::Error::INVALID_MATCH_KEY_ENCODING:
      return ExternalError::INVALID_MATCH_KEY_ENCODING;
    case backend::Error::INVALID_MATCH_KEY_FIELD:
      return ExternalError::INVALID_MATCH_KEY_FIELD;
    case backend::Error::DECRYPTION_ERROR:
    case backend::Error::KMS_CLIENT_ERROR:
    case backend::Error::CACHED_KMS_CLIENT_ERROR:
      return ExternalError::DECRYPTION_ERROR;
    case backend::Error::ENCRYPTION_ERROR:
      return ExternalError::ENCRYPTION_ERROR;
    case backend::Error::MISSING_ENCRYPTION_KEY_FIELD:
      return ExternalError::MISSING_ENCRYPTION_KEY_FIELD;
    case backend::Error::DEK_DECRYPTION_ERROR:
      return ExternalError::DEK_DECRYPTION_ERROR;
    case backend::Error::INVALID_DEK:
      return ExternalError::INVALID_DEK;
    case backend::Error::WRAPPED_KEY_FETCHING_ERROR:
      return ExternalError::WRAPPED_KEY_FETCHING_ERROR;
    case backend::Error::CUSTOMER_QUOTA_EXCEEDED:
      return ExternalError::CUSTOMER_QUOTA_EXCEEDED;
    case backend::Error::CUSTOMER_KEY_PERMISSION_DENIED:
      return ExternalError::CUSTOMER_KEY_PERMISSION_DENIED;
  }
  // Exhaustive switch, should never reach here.
  LOG_ERROR(GlobalLogger(),
            "No external error reason specified for backend error reason: %d",
            backend_error_reason);
  return ExternalError::UNKNOWN_ERROR;
}

}  // namespace

grpc::Status ToApiStatus(const absl::Status& status) noexcept {
  if (status.ok()) {
    return grpc::Status::OK;
  }

  std::optional<Error::Reason> backend_error_reason =
      GetBackendErrorReason(status);
  if (!backend_error_reason.has_value()) {
    LOG_ERROR(GlobalLogger(),
              "absl::Status missing backend error reason payload: %v", status);
    return CreateApiStatus(ExternalError::INTERNAL_ERROR);
  }
  return CreateApiStatus(ToExternalErrorReason(*backend_error_reason));
}

}  // namespace google::confidential_match::match_service
