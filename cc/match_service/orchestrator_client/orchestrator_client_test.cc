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

#include "cc/match_service/orchestrator_client/orchestrator_client.h"

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
#include "gmock/gmock.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "cc/match_service/auth_token_client/mock_auth_token_client.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::MockAuthTokenClient;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpClientInterface;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::http2_client::mock::MockHttpClient;
using ::google::scp::core::test::EqualsProto;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;

constexpr absl::string_view kOrchestratorHostAddress =
    "orchestrator.test.google.com";
constexpr absl::string_view kClusterGroupId = "cluster-group-id";
constexpr absl::string_view kAuthenticationToken = "token";
constexpr absl::string_view kShardingSchemeJson = R"({
    "shardingSchemeId": "123",
    "type": "jch",
    "shards": [
        {
            "shardNumber": "0",
            "serverAddressUri": "https://lookup-server-0"
        },
        {
            "shardNumber": "1",
            "serverAddressUri": "https://lookup-server-1"
        }
    ]
})";

class OrchestratorClientTest : public testing::Test {
 protected:
  OrchestratorClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        orchestrator_client_(std::make_unique<OrchestratorClient>(
            &mock_auth_token_client_, &mock_http2_client_,
            kOrchestratorHostAddress)) {}

  std::shared_ptr<LoggerInterface> logger_;
  MockAuthTokenClient mock_auth_token_client_;
  MockHttpClient mock_http2_client_;
  std::unique_ptr<OrchestratorClientInterface> orchestrator_client_;
};

TEST_F(OrchestratorClientTest, StartStop) {
  OrchestratorClient orchestrator_client(
      &mock_auth_token_client_, &mock_http2_client_, kOrchestratorHostAddress);
  EXPECT_TRUE(orchestrator_client.Init().ok());
  EXPECT_TRUE(orchestrator_client.Run().ok());
  EXPECT_TRUE(orchestrator_client.Stop().ok());
}

// Helper to mock auth token
void MockGetAuthToken(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context) {
  GetAuthTokenResponse response;
  response.set_token(kAuthenticationToken);
  context.response = std::make_shared<GetAuthTokenResponse>(response);
  context.status = absl::OkStatus();
  context.Finish();
}

// Helper to mock auth token failure on status
void MockGetAuthTokenFailure(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context) {
  context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, "error");
  context.Finish();
}

// Helper to simulate an orchestrator response with valid data.
// Captures and returns the request query parameters.
ExecutionResult MockPerformRequestToOrchestrator(
    std::string* request_query,
    scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
  *request_query = *context.request->query;

  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer(std::string(kShardingSchemeJson));
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper to simulate an orchestrator response with invalid data.
ExecutionResult MockPerformRequestToOrchestratorWithInvalidResponse(
    scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer("invalid-json");
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

TEST_F(OrchestratorClientTest, GetShardingSchemeIsSuccessful) {
  GetCurrentShardingSchemeResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kShardingSchemeJson, &expected_response).ok());
  std::string captured_request_query;
  mock_http2_client_.perform_request_mock =
      std::bind(MockPerformRequestToOrchestrator, &captured_request_query, _1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthToken).WillOnce(MockGetAuthToken);
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      get_sharding_scheme_context(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [&expected_response](
              AsyncContext<GetCurrentShardingSchemeRequest,
                           GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response));

            EXPECT_EQ(context.response->shards_size(), 2);
            EXPECT_EQ(context.response->type(), "jch");
          },
          logger_);

  orchestrator_client_->GetCurrentShardingScheme(get_sharding_scheme_context);

  EXPECT_THAT(captured_request_query,
              HasSubstr(absl::StrFormat("clusterGroupId=%s", kClusterGroupId)));
}

TEST_F(OrchestratorClientTest,
       GetShardingSchemeWithAuthStatusErrorYieldsFailure) {
  GetCurrentShardingSchemeResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kShardingSchemeJson, &expected_response).ok());
  std::string captured_request_query;
  mock_http2_client_.perform_request_mock =
      std::bind(MockPerformRequestToOrchestrator, &captured_request_query, _1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthToken)
      .WillOnce(MockGetAuthTokenFailure);
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      get_sharding_scheme_context(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [](AsyncContext<GetCurrentShardingSchemeRequest,
                          GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, StatusIs(absl::StatusCode::kInternal));
            EXPECT_EQ(GetBackendErrorReason(context.status),
                      Error::AUTH_TOKEN_CLIENT_ERROR);
            EXPECT_EQ(context.response, nullptr);
          },
          logger_);

  orchestrator_client_->GetCurrentShardingScheme(get_sharding_scheme_context);
}

TEST_F(OrchestratorClientTest, GetShardingSchemeWithFetchErrorYieldsFailure) {
  mock_http2_client_.http_get_result_mock = FailureExecutionResult(1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthToken).WillOnce(MockGetAuthToken);
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      get_sharding_scheme_context(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [](AsyncContext<GetCurrentShardingSchemeRequest,
                          GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, StatusIs(absl::StatusCode::kInternal));
            EXPECT_EQ(GetBackendErrorReason(context.status),
                      Error::ORCHESTRATOR_SERVICE_ERROR);
            EXPECT_EQ(context.response, nullptr);
          },
          logger_);

  orchestrator_client_->GetCurrentShardingScheme(get_sharding_scheme_context);
}

TEST_F(OrchestratorClientTest,
       GetShardingSchemeWithInvalidResponseYieldsFailure) {
  mock_http2_client_.perform_request_mock =
      std::bind(MockPerformRequestToOrchestratorWithInvalidResponse, _1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthToken).WillOnce(MockGetAuthToken);
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      get_sharding_scheme_context(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [](AsyncContext<GetCurrentShardingSchemeRequest,
                          GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, StatusIs(absl::StatusCode::kInternal));
            EXPECT_EQ(GetBackendErrorReason(context.status),
                      Error::ORCHESTRATOR_RESPONSE_DESERIALIZATION_ERROR);
            EXPECT_EQ(context.response, nullptr);
          },
          logger_);

  orchestrator_client_->GetCurrentShardingScheme(get_sharding_scheme_context);
}

}  // namespace
}  // namespace google::confidential_match::match_service
