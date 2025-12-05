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

#include "cc/core/auth_token_client/auth_token_client.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "cc/core/http2_client/mock/mock_http_client.h"
#include "cc/core/interface/http_types.h"
#include "cc/core/interface/type_def.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

namespace google::confidential_match {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::http2_client::mock::MockHttpClient;
using ::google::scp::core::test::EqualsProto;
using ::std::placeholders::_1;

constexpr absl::string_view kAudience = "audience";
constexpr absl::string_view kAuthenticationToken = "token";

class AuthTokenClientTest : public testing::Test {
 protected:
  AuthTokenClientTest()
      : auth_token_client_(
            std::make_unique<AuthTokenClient>(&mock_http1_client_)) {}

  MockHttpClient mock_http1_client_;
  std::unique_ptr<AuthTokenClientInterface> auth_token_client_;
};

TEST_F(AuthTokenClientTest, StartStop) {
  AuthTokenClient auth_token_client(&mock_http1_client_);
  EXPECT_TRUE(auth_token_client.Init().ok());
  EXPECT_TRUE(auth_token_client.Run().ok());
  EXPECT_TRUE(auth_token_client.Stop().ok());
}

// Helper to simulate a response from the VM instance metadata endpoint.
ExecutionResult MockPerformRequestToInstanceMetadataEndpoint(
    AsyncContext<HttpRequest, HttpResponse>& context) {
  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer(std::string(kAuthenticationToken));
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

TEST_F(AuthTokenClientTest, GetAuthTokenIsSuccessful) {
  mock_http1_client_.perform_request_mock =
      MockPerformRequestToInstanceMetadataEndpoint;
  GetAuthTokenRequest request;
  request.set_audience(std::string(kAudience));
  AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>
      context;
  context.request = std::make_shared<GetAuthTokenRequest>(request);
  context.callback = [](auto& context) {
    GetAuthTokenResponse expected;
    expected.set_token(kAuthenticationToken);
    EXPECT_SUCCESS(context.result);
    EXPECT_THAT(*context.response, IsOkAndHolds(EqualsProto(expected)));
  };

  absl::Status status = auth_token_client_->GetAuthToken(context);

  ASSERT_THAT(status, IsOk());
}

TEST_F(AuthTokenClientTest, GetAuthTokenFailure) {
  mock_http1_client_.http_get_result_mock = FailureExecutionResult(1);
  GetAuthTokenRequest request;
  request.set_audience(std::string(kAudience));
  AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>
      context;
  context.request = std::make_shared<GetAuthTokenRequest>(request);
  context.callback = [](auto& context) {
    GetAuthTokenResponse expected;
    expected.set_token(kAuthenticationToken);
    EXPECT_SUCCESS(context.result);
    EXPECT_THAT(*context.response, StatusIs(absl::StatusCode::kInternal));
  };

  absl::Status status = auth_token_client_->GetAuthToken(context);

  ASSERT_THAT(status, IsOk());
}

}  // namespace
}  // namespace google::confidential_match
