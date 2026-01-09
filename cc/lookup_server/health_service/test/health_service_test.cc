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

#include "cc/lookup_server/health_service/src/health_service.h"

#include <memory>
#include <string>
#include <vector>

#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "core/http2_server/mock/mock_http2_server.h"
#include "core/interface/async_context.h"
#include "core/interface/http_server_interface.h"
#include "gtest/gtest.h"
#include "public/core/interface/execution_result.h"

#include "cc/lookup_server/health_service/mock/pass_through_health_service.h"
#include "cc/lookup_server/health_service/src/error_codes.h"
#include "cc/lookup_server/match_data_storage/mock/mock_match_data_storage.h"
#include "cc/lookup_server/service/src/json_serialization_functions.h"
#include "protos/lookup_server/api/healthcheck.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"

namespace google::confidential_match::lookup_server {
namespace {
using ::google::confidential_match::lookup_server::proto_api::
    HealthcheckResponse;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::confidential_match::lookup_server::proto_backend::ServiceStatus;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::Byte;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::HttpServerInterface;
using ::google::scp::core::RetryExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::http2_server::mock::MockHttp2Server;
using ::google::scp::core::test::ResultIs;
using ::testing::Return;

constexpr char kStorageServiceName[] = "MatchDataStorage";

}  // namespace

class HealthServiceTest : public testing::Test {
 protected:
  HealthServiceTest()
      : mock_match_data_storage_(std::make_shared<MockMatchDataStorage>()),
        mock_http2_server_(std::make_shared<MockHttp2Server>()),
        health_service_(mock_http2_server_,
                        {{kStorageServiceName, mock_match_data_storage_}}) {}

  std::shared_ptr<MockMatchDataStorage> mock_match_data_storage_;
  std::shared_ptr<MockHttp2Server> mock_http2_server_;
  PassThroughHealthService health_service_;
};

TEST_F(HealthServiceTest, CheckHealthSuccess) {
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillRepeatedly(Return(ServiceStatus::OK));

  ExecutionResult result = health_service_.CheckHealth(http_context);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(http_context.result);
  HealthcheckResponse response;
  EXPECT_TRUE(
      JsonBytesBufferToProto(http_context.response->body, &response).ok());
  EXPECT_FALSE(response.response_id().empty());
}

TEST_F(HealthServiceTest, CheckStartupHealthSuccess) {
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillRepeatedly(Return(ServiceStatus::OK));

  ExecutionResult result = health_service_.CheckStartupHealth(http_context);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(http_context.result);
  HealthcheckResponse response;
  EXPECT_TRUE(
      JsonBytesBufferToProto(http_context.response->body, &response).ok());
  EXPECT_EQ(response.services().size(), 1);
  EXPECT_EQ(response.services(0).name(), kStorageServiceName);
  EXPECT_EQ(response.services(0).status(),
            ServiceStatus_Name(ServiceStatus::OK));
  EXPECT_FALSE(response.response_id().empty());
}

TEST_F(HealthServiceTest, CheckStartupHealthMissingStorageService) {
  PassThroughHealthService health_service(
      mock_http2_server_,
      absl::flat_hash_map<std::string,
                          std::shared_ptr<StatusProviderInterface>>());

  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.response = std::make_shared<HttpResponse>();

  ExecutionResult result = health_service.CheckStartupHealth(http_context);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(
      http_context.result,
      ResultIs(FailureExecutionResult(HEALTH_SERVICE_MISSING_STORAGE_SERVICE)));
}

TEST_F(HealthServiceTest, CheckStartupHealthWithPendingDataLoad) {
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillRepeatedly(Return(ServiceStatus::PENDING));
  EXPECT_SUCCESS(health_service_.Init());

  ExecutionResult result = health_service_.CheckStartupHealth(http_context);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(
      http_context.result,
      ResultIs(FailureExecutionResult(HEALTH_SERVICE_STORAGE_SERVICE_ERROR)));
}

TEST_F(HealthServiceTest, CheckStartupHealthWithStorageServiceError) {
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillRepeatedly(Return(ServiceStatus::ERROR));
  EXPECT_SUCCESS(health_service_.Init());

  ExecutionResult result = health_service_.CheckStartupHealth(http_context);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(
      http_context.result,
      ResultIs(FailureExecutionResult(HEALTH_SERVICE_STORAGE_SERVICE_ERROR)));
}

TEST_F(HealthServiceTest, CheckStartupHealthWithPendingThenSuccessfulDataLoad) {
  // Testing first state: Service is pending status
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillRepeatedly(Return(ServiceStatus::PENDING));
  EXPECT_SUCCESS(health_service_.Init());

  ExecutionResult result = health_service_.CheckStartupHealth(http_context);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(
      http_context.result,
      ResultIs(FailureExecutionResult(HEALTH_SERVICE_STORAGE_SERVICE_ERROR)));

  // Testing second state: Service is healthy
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillRepeatedly(Return(ServiceStatus::OK));

  ExecutionResult second_result =
      health_service_.CheckStartupHealth(http_context);

  EXPECT_SUCCESS(second_result);
  EXPECT_SUCCESS(http_context.result);
  HealthcheckResponse response;
  EXPECT_TRUE(
      JsonBytesBufferToProto(http_context.response->body, &response).ok());
  EXPECT_EQ(response.services().size(), 1);
  EXPECT_EQ(response.services(0).name(), kStorageServiceName);
  EXPECT_EQ(response.services(0).status(),
            ServiceStatus_Name(ServiceStatus::OK));
  EXPECT_FALSE(response.response_id().empty());
}

}  // namespace google::confidential_match::lookup_server
