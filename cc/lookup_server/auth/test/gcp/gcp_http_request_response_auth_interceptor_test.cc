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

#include "cc/lookup_server/auth/src/gcp/gcp_http_request_response_auth_interceptor.h"

#include "absl/strings/str_cat.h"
#include "cc/core/interface/http_types.h"
#include "cc/public/core/interface/execution_result.h"
#include "gtest/gtest.h"
#include "public/core/test/interface/execution_result_matchers.h"

#include "cc/lookup_server/auth/mock/mock_jwt_validator.h"
#include "cc/lookup_server/auth/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::AuthorizationMetadata;
using ::google::scp::core::AuthorizedMetadata;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::IsSuccessfulAndHolds;
using ::google::scp::core::test::ResultIs;
using testing::Eq;
using testing::FieldsAre;
using testing::Pointee;
using ::testing::Return;

constexpr absl::string_view kJwt = "json.web.token";
constexpr absl::string_view kJwkSet = "json-web-key-set";

}  // namespace

class GcpHttpRequestResponseAuthInterceptorTest : public testing::Test {
 protected:
  GcpHttpRequestResponseAuthInterceptorTest()
      : mock_jwt_validator_(std::make_shared<MockJwtValidator>()),
        auth_interceptor_(mock_jwt_validator_) {}

  std::shared_ptr<MockJwtValidator> mock_jwt_validator_;
  GcpHttpRequestResponseAuthInterceptor auth_interceptor_;
};

TEST_F(GcpHttpRequestResponseAuthInterceptorTest, PrepareRequestYieldsSuccess) {
  AuthorizationMetadata authorization_metadata;
  authorization_metadata.authorization_token = kJwt;
  HttpRequest http_request;

  ExecutionResult result =
      auth_interceptor_.PrepareRequest(authorization_metadata, http_request);

  EXPECT_THAT(result, IsSuccessful());
  EXPECT_EQ(http_request.method, HttpMethod::GET);
  EXPECT_EQ(*http_request.query, "");
}

TEST_F(GcpHttpRequestResponseAuthInterceptorTest,
       ObtainAuthorizedMetadataFromResponseWithValidAuthYieldsSuccess) {
  AuthorizationMetadata authorization_metadata;
  authorization_metadata.authorization_token = kJwt;
  HttpResponse http_response;
  http_response.body = BytesBuffer(std::string(kJwkSet));
  EXPECT_CALL(*mock_jwt_validator_, Validate(kJwkSet, kJwt))
      .WillOnce(Return(SuccessExecutionResult()));

  ExecutionResultOr<AuthorizedMetadata> result_or =
      auth_interceptor_.ObtainAuthorizedMetadataFromResponse(
          authorization_metadata, http_response);

  EXPECT_THAT(result_or, IsSuccessfulAndHolds(FieldsAre(Pointee(Eq("")))));
}

TEST_F(GcpHttpRequestResponseAuthInterceptorTest,
       ObtainAuthorizedMetadataFromResponseWithInvalidAuthYieldsFailure) {
  AuthorizationMetadata authorization_metadata;
  authorization_metadata.authorization_token = kJwt;
  HttpResponse http_response;
  http_response.body = BytesBuffer(std::string(kJwkSet));
  EXPECT_CALL(*mock_jwt_validator_, Validate(kJwkSet, kJwt))
      .WillOnce(Return(FailureExecutionResult(AUTH_INVALID_JWT)));

  ExecutionResultOr<AuthorizedMetadata> result_or =
      auth_interceptor_.ObtainAuthorizedMetadataFromResponse(
          authorization_metadata, http_response);

  EXPECT_THAT(result_or, ResultIs(FailureExecutionResult(AUTH_INVALID_JWT)));
}

}  // namespace google::confidential_match::lookup_server
