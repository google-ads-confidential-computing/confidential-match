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

#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/error.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::api::v1::ExternalError;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::test::EqualsProto;

// Helper function to get the error message from an ExternalErrorReason enum.
std::string ErrorReasonMessage(ExternalError::Reason error_reason) noexcept {
  return protobuf::GetEnumDescriptor<ExternalError::Reason>()
      ->FindValueByNumber(error_reason)
      ->options()
      .GetExtension(api::v1::external_message);
}

TEST(ErrorsTest, StatusWithBackendReason) {
  std::string backend_message = "Test status message";
  Error::Reason backend_reason = Error::INTERNAL_ERROR;

  absl::Status result = Status(backend_reason, backend_message);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal, backend_message));
  EXPECT_EQ(*GetBackendErrorReason(result), backend_reason);
}

TEST(ErrorsTest, GetBackendErrorReasonMissingPayload) {
  absl::Status status = absl::InternalError("Test status message");

  std::optional<Error::Reason> result = GetBackendErrorReason(status);

  EXPECT_FALSE(result.has_value()) << *result;
}

TEST(ErrorsTest, BackendReasonString) {
  std::string backend_message = "Test status message";
  Error::Reason backend_reason = Error::INTERNAL_ERROR;

  absl::Status result = Status(backend_reason, backend_message);

  EXPECT_EQ(GetBackendErrorReasonString(result), "INTERNAL_ERROR");
}

TEST(ErrorsTest, BackendReasonStringNotFound) {
  absl::Status result = absl::InternalError("Bad error");

  EXPECT_EQ(GetBackendErrorReasonString(result), "UNKNOWN_ERROR");
}

TEST(ErrorsTest, AnnotateEmptyMessage) {
  absl::Status status = absl::InternalError("Error message");
  absl::Status result = Annotate(status, "");
  EXPECT_EQ(result, status);
}

TEST(ErrorsTest, AnnotateOkStatus) {
  absl::Status status = absl::OkStatus();
  absl::Status result = Annotate(status, "Should not annotate");
  EXPECT_EQ(result, status);
}

TEST(ErrorsTest, AnnotateStatusWithMessage) {
  absl::Status status = absl::InternalError("Original message");
  absl::Status result = Annotate(status, "New message");
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal,
                               "Original message; New message"));
}

TEST(ErrorsTest, AnnotateStatusWithoutMessage) {
  absl::Status status = absl::InternalError("");
  absl::Status result = Annotate(status, "Error message");
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal, "Error message"));
}

TEST(ErrorsTest, AnnotatePreservesPayload) {
  std::string backend_message = "Original message";
  Error::Reason backend_reason = Error::INTERNAL_ERROR;
  absl::Status status = Status(backend_reason, backend_message);

  absl::Status result = Annotate(status, "New message");

  EXPECT_EQ(*GetBackendErrorReason(result), backend_reason);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal,
                               "Original message; New message"));
}

TEST(ErrorsTest, GetErrorReasonSuccess) {
  std::string reason = "INVALID_ARGUMENT_ERROR";
  grpc::Status status =
      GrpcStatus(grpc::INVALID_ARGUMENT, "Invalid argument", reason);

  std::optional<std::string> result = GetErrorReason(status);

  EXPECT_EQ(*result, reason);
}

TEST(ErrorsTest, GetErrorReasonMissingReason) {
  grpc::Status status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid argument");

  std::optional<std::string> result = GetErrorReason(status);

  EXPECT_FALSE(result.has_value());
}

TEST(ErrorsTest, GrpcStatusSuccess) {
  grpc::StatusCode code = grpc::INVALID_ARGUMENT;
  std::string message = "The argument is invalid";
  std::string reason = "INVALID_ARGUMENT_ERROR";

  grpc::Status result = GrpcStatus(code, message, reason);

  EXPECT_EQ(result.error_code(), code);
  EXPECT_EQ(result.error_message(), message);
  google::rpc::Status rpc_status;
  EXPECT_TRUE(rpc_status.ParseFromString(result.error_details()));
  EXPECT_THAT(rpc_status, EqualsProto(RpcStatus(rpc::Code::INVALID_ARGUMENT,
                                                message, reason)));
}

TEST(ErrorsTest, RpcStatusSuccess) {
  auto code = rpc::Code::DEADLINE_EXCEEDED;
  auto message = "The deadline was exceeded";
  auto reason = "DEADLINE_EXCEEDED_ERROR";

  auto status = RpcStatus(code, message, reason);

  EXPECT_EQ(status.code(), code);
  EXPECT_EQ(status.message(), message);
  EXPECT_EQ(status.details_size(), 1);
  rpc::ErrorInfo error_info;
  status.details(0).UnpackTo(&error_info);
  EXPECT_EQ(error_info.reason(), reason);
}

TEST(StatusConverterTest, ToApiStatusSuccess) {
  absl::Status status = Status(Error::CANCELLED, "Backend error message");

  grpc::Status result = ToApiStatus(status);

  ExternalError::Reason expected_api_reason = ExternalError::CANCELLED;
  std::string expected_message = ErrorReasonMessage(expected_api_reason);
  EXPECT_EQ(result.error_code(), grpc::StatusCode::CANCELLED);
  EXPECT_EQ(result.error_message(), expected_message);
  rpc::Status rpc_status;
  EXPECT_TRUE(rpc_status.ParseFromString(result.error_details()));
  EXPECT_THAT(
      rpc_status,
      EqualsProto(RpcStatus(rpc::Code::CANCELLED, expected_message,
                            ExternalError::Reason_Name(expected_api_reason))));
}

TEST(StatusConverterTest, ToApiStatusWithoutBackendErrorReason) {
  // Expect an absl::Status without a backend reason payload is converted to a
  // generic internal error status.
  absl::Status status = absl::PermissionDeniedError("Permission denied");

  grpc::Status result = ToApiStatus(status);

  ExternalError::Reason expected_reason = ExternalError::INTERNAL_ERROR;
  std::string expected_message = ErrorReasonMessage(expected_reason);
  EXPECT_EQ(result.error_code(), grpc::StatusCode::INTERNAL);
  EXPECT_EQ(result.error_message(), expected_message);
  rpc::Status rpc_status;
  EXPECT_TRUE(rpc_status.ParseFromString(result.error_details()));
  EXPECT_THAT(rpc_status, EqualsProto(RpcStatus(
                              rpc::Code::INTERNAL, expected_message,
                              ExternalError::Reason_Name(expected_reason))));
}

}  // namespace
}  // namespace google::confidential_match::match_service
