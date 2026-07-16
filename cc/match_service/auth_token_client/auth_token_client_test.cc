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

#include "cc/match_service/auth_token_client/auth_token_client.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/http2_client/mock/mock_http_client.h"
#include "cc/core/interface/http_types.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/match_service/error/error.h"
#include "protos/core/auth_token.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::http2_client::mock::MockHttpClient;
using ::google::scp::core::test::EqualsProto;
using ::std::placeholders::_1;
using ::testing::Eq;
using ::testing::Pointee;

constexpr absl::string_view kAudience = "audience";
constexpr absl::string_view kAuthenticationToken = "token";
constexpr absl::string_view kEmail = "test@example.com";

class AuthTokenClientTest : public testing::Test {
 protected:
  AuthTokenClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        auth_token_client_(
            std::make_unique<AuthTokenClient>(&mock_http1_client_)) {}

  std::shared_ptr<LoggerInterface> logger_;
  MockHttpClient mock_http1_client_;
  std::unique_ptr<AuthTokenClientInterface> auth_token_client_;
};

TEST_F(AuthTokenClientTest, StartStop) {
  AuthTokenClient auth_token_client(&mock_http1_client_);
  EXPECT_TRUE(auth_token_client.Init().ok());
  EXPECT_TRUE(auth_token_client.Run().ok());
  EXPECT_TRUE(auth_token_client.Stop().ok());
}

// Helper to simulate a response with a given response body
ExecutionResult MockSuccessResponse(
    absl::string_view body,
    scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer(std::string(body));
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

TEST_F(AuthTokenClientTest, GetAuthTokenIsSuccessful) {
  mock_http1_client_.perform_request_mock =
      std::bind(MockSuccessResponse, kAuthenticationToken, _1);

  GetAuthTokenRequest request;
  request.set_audience(std::string(kAudience));
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context(
      std::make_shared<GetAuthTokenRequest>(request),
      [](AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        GetAuthTokenResponse expected;
        expected.set_token(kAuthenticationToken);
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected));
      },
      logger_);

  auth_token_client_->GetAuthToken(context);
}

TEST_F(AuthTokenClientTest, GetAuthTokenFullFormatIsSuccessful) {
  // Verify specific query parameters
  mock_http1_client_.perform_request_mock =
      [](scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
        EXPECT_EQ(*context.request->query, "audience=audience&format=full");
        return MockSuccessResponse(kAuthenticationToken, context);
      };
  GetAuthTokenRequest request;
  request.set_audience(std::string(kAudience));
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context(
      std::make_shared<GetAuthTokenRequest>(request),
      [](AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        GetAuthTokenResponse expected;
        expected.set_token(kAuthenticationToken);
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected));
      },
      logger_);

  auth_token_client_->GetAuthTokenFullFormat(context);
}

TEST_F(AuthTokenClientTest, GetAuthTokenFailure) {
  mock_http1_client_.http_get_result_mock = FailureExecutionResult(1);
  GetAuthTokenRequest request;
  request.set_audience(std::string(kAudience));
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context(
      std::make_shared<GetAuthTokenRequest>(request),
      [](auto& context) {
        EXPECT_THAT(GetBackendErrorReason(context.status),
                    Eq(Error::AUTH_TOKEN_CLIENT_ERROR));
        EXPECT_EQ(context.response, nullptr);
      },
      logger_);

  auth_token_client_->GetAuthToken(context);
}

TEST_F(AuthTokenClientTest, GetServiceAccountEmailIsSuccessful) {
  GetServiceAccountEmailResponse expected_response;
  expected_response.set_email(kEmail);
  mock_http1_client_.perform_request_mock =
      std::bind(MockSuccessResponse, kEmail, _1);
  AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
      context(
          std::make_shared<GetServiceAccountEmailRequest>(),
          [&expected_response](
              AsyncContext<GetServiceAccountEmailRequest,
                           GetServiceAccountEmailResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response));
          },
          logger_);

  auth_token_client_->GetServiceAccountEmail(context);
}

TEST_F(AuthTokenClientTest, GetServiceAccountEmailFailsOnHttpError) {
  mock_http1_client_.perform_request_mock =
      [](scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
        return FailureExecutionResult(1);
      };
  AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
      context(
          std::make_shared<GetServiceAccountEmailRequest>(),
          [](AsyncContext<GetServiceAccountEmailRequest,
                          GetServiceAccountEmailResponse>& context) {
            EXPECT_THAT(GetBackendErrorReason(context.status),
                        Eq(Error::AUTH_TOKEN_CLIENT_ERROR));
          },
          logger_);

  auth_token_client_->GetServiceAccountEmail(context);
}

}  // namespace
}  // namespace google::confidential_match::match_service
