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

#include "cc/core/orchestrator_client/orchestrator_client.h"

#include <memory>
#include <string>

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

#include "absl/status/status_matchers.h"
#include "cc/core/auth_token_client/mock_auth_token_client.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::AsyncContext;
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
      : orchestrator_client_(std::make_unique<OrchestratorClient>(
            &mock_auth_token_client_, &mock_http2_client_,
            kOrchestratorHostAddress)) {}

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
absl::Status MockGetAuthToken(
    AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>
        context) {
  GetAuthTokenResponse response;
  response.set_token(kAuthenticationToken);
  context.response =
      std::make_shared<absl::StatusOr<GetAuthTokenResponse>>(response);
  context.result = SuccessExecutionResult();
  context.Finish();
  return absl::OkStatus();
}

// Helper to mock auth token failure on status
absl::Status MockGetAuthTokenFailure(
    AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>
        context) {
  context.response = std::make_shared<absl::StatusOr<GetAuthTokenResponse>>(
      absl::InternalError("error"));
  context.result = SuccessExecutionResult();
  context.Finish();
  return absl::InternalError("error");
}

// Helper to simulate an orchestrator response with valid data.
// Captures and returns the request query parameters.
ExecutionResult MockPerformRequestToOrchestrator(
    std::string* request_query,
    AsyncContext<HttpRequest, HttpResponse>& context) {
  *request_query = *context.request->query;

  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer(std::string(kShardingSchemeJson));
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
  AsyncContext<GetCurrentShardingSchemeRequest,
               absl::StatusOr<GetCurrentShardingSchemeResponse>>
      context;
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  context.request = std::make_shared<GetCurrentShardingSchemeRequest>(request);
  context.callback = [&expected_response](auto& context) {
    EXPECT_SUCCESS(context.result);
    ASSERT_THAT(*context.response,
                IsOkAndHolds(EqualsProto(expected_response)));

    GetCurrentShardingSchemeResponse& sharding_scheme =
        context.response->value();
    EXPECT_EQ(sharding_scheme.shards_size(), 2);
    EXPECT_EQ(sharding_scheme.type(), "jch");
  };

  absl::Status status = orchestrator_client_->GetCurrentShardingScheme(context);

  ASSERT_THAT(status, IsOk());
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
  AsyncContext<GetCurrentShardingSchemeRequest,
               absl::StatusOr<GetCurrentShardingSchemeResponse>>
      context;
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  context.request = std::make_shared<GetCurrentShardingSchemeRequest>(request);

  absl::Status status = orchestrator_client_->GetCurrentShardingScheme(context);

  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal));
}

TEST_F(OrchestratorClientTest,
       GetShardingSchemeWithAuthExecutionErrorYieldsFailure) {
  GetCurrentShardingSchemeResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kShardingSchemeJson, &expected_response).ok());
  std::string captured_request_query;
  mock_http2_client_.perform_request_mock =
      std::bind(MockPerformRequestToOrchestrator, &captured_request_query, _1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthToken)
      .WillOnce(Return(absl::InternalError("test")));
  AsyncContext<GetCurrentShardingSchemeRequest,
               absl::StatusOr<GetCurrentShardingSchemeResponse>>
      context;
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  context.request = std::make_shared<GetCurrentShardingSchemeRequest>(request);

  absl::Status status = orchestrator_client_->GetCurrentShardingScheme(context);

  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal));
}

TEST_F(OrchestratorClientTest, GetShardingSchemeWithFetchErrorYieldsFailure) {
  mock_http2_client_.http_get_result_mock = FailureExecutionResult(1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthToken).WillOnce(MockGetAuthToken);
  AsyncContext<GetCurrentShardingSchemeRequest,
               absl::StatusOr<GetCurrentShardingSchemeResponse>>
      context;
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  context.request = std::make_shared<GetCurrentShardingSchemeRequest>(request);
  context.callback = [](auto& context) {
    EXPECT_SUCCESS(context.result);
    EXPECT_THAT(*context.response, StatusIs(absl::StatusCode::kInternal));
  };

  absl::Status status = orchestrator_client_->GetCurrentShardingScheme(context);

  ASSERT_THAT(status, IsOk());
}

}  // namespace
}  // namespace google::confidential_match
