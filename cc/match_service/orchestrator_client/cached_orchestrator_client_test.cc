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

#include "cc/match_service/orchestrator_client/cached_orchestrator_client.h"

#include <memory>
#include <string>

#include "absl/strings/str_format.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/interface/http_types.h"
#include "cc/core/interface/type_def.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "absl/status/status_matchers.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/orchestrator_client/mock_orchestrator_client.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::Timestamp;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::Eq;
using ::testing::Field;
using ::testing::HasSubstr;
using ::testing::Pointee;
using ::testing::Property;
using ::testing::Return;

constexpr absl::string_view kClusterGroupId = "cluster-group-id";
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

class CachedOrchestratorClientTest : public testing::Test {
 protected:
  CachedOrchestratorClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_orchestrator_client_(std::make_shared<MockOrchestratorClient>()),
        cached_client_(std::make_unique<CachedOrchestratorClient>(
            mock_async_executor_, mock_orchestrator_client_)) {
    mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
      return SuccessExecutionResult();
    };
    mock_async_executor_->schedule_for_mock =
        [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
          return SuccessExecutionResult();
        };
  }

  void SetUp() override {
    ASSERT_THAT(cached_client_->Init(), IsOk());
    ASSERT_THAT(cached_client_->Run(), IsOk());
  }

  std::shared_ptr<LoggerInterface> logger_;
  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::shared_ptr<MockOrchestratorClient> mock_orchestrator_client_;
  std::unique_ptr<OrchestratorClientInterface> cached_client_;
};

// Helper to simulate a successful response from the uncached client.
absl::Status MockGetShardingSchemeResponse(
    const GetCurrentShardingSchemeResponse& response,
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>
        context) noexcept {
  context.response =
      std::make_shared<GetCurrentShardingSchemeResponse>(response);
  context.status = absl::OkStatus();
  context.Finish();
  return absl::OkStatus();
}

// Helper to simulate an error response from the uncached client.
void MockGetShardingSchemeError(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& context) noexcept {
  context.status = Status(Error::INTERNAL_ERROR, "Underlying client error");
  context.Finish();
}

TEST_F(CachedOrchestratorClientTest, GetSchemeUsesCache) {
  GetCurrentShardingSchemeResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kShardingSchemeJson, &expected_response).ok());
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(
          std::bind(MockGetShardingSchemeResponse, expected_response, _1));
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  std::atomic<bool> is_complete = false;
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      request_context(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [&expected_response, &is_complete](
              AsyncContext<GetCurrentShardingSchemeRequest,
                           GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response));

            EXPECT_EQ(context.response->shards_size(), 2);
            EXPECT_EQ(context.response->type(), "jch");
            is_complete = true;
          },
          logger_);
  std::atomic<bool> is_complete1 = false;
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      request_context1(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [&expected_response, &is_complete1](
              AsyncContext<GetCurrentShardingSchemeRequest,
                           GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response));

            EXPECT_EQ(context.response->shards_size(), 2);
            EXPECT_EQ(context.response->type(), "jch");
            is_complete1 = true;
          },
          logger_);

  cached_client_->GetCurrentShardingScheme(request_context);
  cached_client_->GetCurrentShardingScheme(request_context1);

  WaitUntil([&]() { return is_complete.load(); });
  WaitUntil([&]() { return is_complete1.load(); });
}

TEST_F(CachedOrchestratorClientTest,
       GetSchemeWithDifferentClusterGroupDoesNotUseCache) {
  GetCurrentShardingSchemeResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kShardingSchemeJson, &expected_response).ok());
  GetCurrentShardingSchemeResponse expected_response1;
  EXPECT_TRUE(
      JsonStringToMessage(kShardingSchemeJson, &expected_response1).ok());
  expected_response1.set_type("test");
  EXPECT_CALL(
      *mock_orchestrator_client_,
      GetCurrentShardingScheme(Field(
          &AsyncContext<GetCurrentShardingSchemeRequest,
                        GetCurrentShardingSchemeResponse>::request,
          Pointee(Property(&GetCurrentShardingSchemeRequest::cluster_group_id,
                           Eq(kClusterGroupId))))))
      .WillOnce(
          std::bind(MockGetShardingSchemeResponse, expected_response, _1));
  EXPECT_CALL(
      *mock_orchestrator_client_,
      GetCurrentShardingScheme(Field(
          &AsyncContext<GetCurrentShardingSchemeRequest,
                        GetCurrentShardingSchemeResponse>::request,
          Pointee(Property(&GetCurrentShardingSchemeRequest::cluster_group_id,
                           Eq("test"))))))
      .WillOnce(
          std::bind(MockGetShardingSchemeResponse, expected_response1, _1));
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      request_context(
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
  GetCurrentShardingSchemeRequest request1;
  request1.set_cluster_group_id("test");
  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      request_context1(
          std::make_shared<GetCurrentShardingSchemeRequest>(request1),
          [&expected_response1](
              AsyncContext<GetCurrentShardingSchemeRequest,
                           GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response1));

            EXPECT_EQ(context.response->shards_size(), 2);
            EXPECT_EQ(context.response->type(), "test");
          },
          logger_);

  cached_client_->GetCurrentShardingScheme(request_context);
  cached_client_->GetCurrentShardingScheme(request_context1);
}

TEST_F(CachedOrchestratorClientTest, GetSchemeWithErrorPropagatesError) {
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(MockGetShardingSchemeError);
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(kClusterGroupId);
  std::atomic<bool> is_complete = false;

  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      request_context(
          std::make_shared<GetCurrentShardingSchemeRequest>(request),
          [&is_complete](
              AsyncContext<GetCurrentShardingSchemeRequest,
                           GetCurrentShardingSchemeResponse>& context) {
            EXPECT_THAT(context.status,
                        StatusIs(absl::StatusCode::kInternal,
                                 HasSubstr("Underlying client error")));
            EXPECT_EQ(GetBackendErrorReason(context.status),
                      Error::INTERNAL_ERROR);
            is_complete = true;
          },
          logger_);

  cached_client_->GetCurrentShardingScheme(request_context);
  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::match_service
