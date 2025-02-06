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

#include "cc/lookup_server/service/src/public_error_response_functions.h"

#include <string>

#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_server_interface.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "googlemock/include/gmock/gmock.h"
#include "googletest/include/gtest/gtest.h"

#include "cc/core/test/src/parse_text_proto.h"
#include "cc/lookup_server/auth/src/error_codes.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/public/src/error_codes.h"
#include "protos/shared/api/errors/code.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::shared::api_errors::Code;
using ::google::confidential_match::shared::api_errors::ErrorResponse;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPublicErrorHasCorrectDomain) {
  ExecutionResultOr<ErrorResponse> error_response_or = BuildPublicErrorResponse(
      FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->details_size(), 1);
  EXPECT_EQ(error_response_or->details(0).domain(), "LookupService");
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPrivateErrorHasCorrectDomain) {
  ExecutionResultOr<ErrorResponse> error_response_or =
      BuildPublicErrorResponse(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->details_size(), 1);
  EXPECT_EQ(error_response_or->details(0).domain(), "LookupService");
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPublicErrorHasCorrectReason) {
  ExecutionResultOr<ErrorResponse> error_response_or = BuildPublicErrorResponse(
      FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->details_size(), 1);
  // The expected error reason code for an encryption/decryption related error.
  EXPECT_EQ(error_response_or->details(0).reason(), "2415853572");
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPrivateErrorHasCorrectReason) {
  ExecutionResultOr<ErrorResponse> error_response_or =
      BuildPublicErrorResponse(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->details_size(), 1);
  EXPECT_EQ(error_response_or->details(0).reason(),
            std::to_string(
                FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED).status_code));
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPublicErrorHasCorrectCode) {
  ExecutionResultOr<ErrorResponse> error_response_or = BuildPublicErrorResponse(
      FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->code(), Code::INVALID_ARGUMENT);
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPrivateErrorHasCorrectCode) {
  ExecutionResultOr<ErrorResponse> error_response_or =
      BuildPublicErrorResponse(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->details_size(), 1);
  EXPECT_EQ(error_response_or->code(), Code::UNKNOWN);
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPublicErrorHasPublicErrorMessage) {
  ExecutionResultOr<ErrorResponse> error_response_or = BuildPublicErrorResponse(
      FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(
      error_response_or->message(),
      GetErrorMessage(FailureExecutionResult(PUBLIC_CRYPTO_ERROR).status_code));
}

TEST(PublicErrorResponseFunctionsTest,
     BuildPublicErrorResponseWithPrivateErrorHasGenericErrorMessage) {
  ExecutionResultOr<ErrorResponse> error_response_or =
      BuildPublicErrorResponse(FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED));

  EXPECT_SUCCESS(error_response_or);
  EXPECT_EQ(error_response_or->message(),
            GetErrorMessage(
                FailureExecutionResult(PUBLIC_GENERIC_ERROR).status_code));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
