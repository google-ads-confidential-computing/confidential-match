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

#include "cc/match_service/service/match_service.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "cc/core/async/async_context.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/tasks/mock_match_task.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "cc/public/cpio/mock/metric_client/mock_metric_client.h"
#include "grpcpp/grpcpp.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::cmrt::sdk::metric_service::v1::Metric;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsRequest;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsResponse;
using ::google::confidential_match::match_service::api::v1::MatchRequest;
using ::google::confidential_match::match_service::api::v1::MatchResponse;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::SubstituteAndParseTextToProto;
using ::google::scp::cpio::MockMetricClient;
using ::testing::_;
using ::testing::An;
using ::testing::AtLeast;
using ::testing::Invoke;
using ::testing::Property;
using ::testing::Return;
using ::testing::UnorderedElementsAre;

class MatchServiceTest : public ::testing::Test {
 protected:
  MatchServiceTest() : port_number_(0) {
    mock_async_executor_.schedule_mock = [](const AsyncOperation& work) {
      work();
      return SuccessExecutionResult();
    };
  }

  void StartServer(MatchServiceOptions options) {
    service_ = std::make_unique<MatchService>(
        &mock_async_executor_, &mock_match_task_, &mock_metric_client_,
        "test_namespace", options);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(),
                             &port_number_);
    builder.RegisterService(service_.get());
    grpc_server_ = builder.BuildAndStart();

    stub_ = api::v1::MatchService::NewStub(
        grpc::CreateChannel(absl::StrCat("localhost:", port_number_),
                            grpc::InsecureChannelCredentials()));
  }

  ~MatchServiceTest() {
    if (grpc_server_) {
      grpc_server_->Shutdown();
    }
  }

  MockAsyncExecutor mock_async_executor_;
  MockMatchTask mock_match_task_;
  MockMetricClient mock_metric_client_;
  std::unique_ptr<MatchService> service_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<api::v1::MatchService::Stub> stub_;
  int port_number_;
};

TEST_F(MatchServiceTest, MatchSuccessful) {
  MatchServiceOptions options;
  options.allow_all_callers = true;
  StartServer(options);

  MatchRequest request;
  request.set_application(api::v1::APPLICATION_ECL);
  request.set_match_key_format(api::v1::MATCH_KEY_FORMAT_HASHED);
  MatchResponse response;

  EXPECT_CALL(mock_metric_client_, PutMetrics)
      .WillOnce([](auto& ctx) {
        auto expected_request =
            SubstituteAndParseTextToProto<PutMetricsRequest>(
                R"pb(
                  metric_namespace: "test_namespace"
                  metrics {
                    name: "MatchRequestCount"
                    type: METRIC_TYPE_COUNTER
                    unit: METRIC_UNIT_COUNT
                    value: "1"
                    labels { key: "EncryptionType" value: "UNSET" }
                    labels { key: "Application" value: "APPLICATION_ECL" }
                    labels {
                      key: "MatchKeyFormat"
                      value: "MATCH_KEY_FORMAT_HASHED"
                    }
                  }
                )pb");
        EXPECT_THAT(*ctx.request, EqualsProto(expected_request));
        ctx.result = SuccessExecutionResult();
        ctx.Finish();
      })
      .WillOnce([](auto& ctx) {
        auto expected_request =
            SubstituteAndParseTextToProto<PutMetricsRequest>(
                R"pb(
                  metric_namespace: "test_namespace"
                  metrics {
                    name: "MatchRequestLatency"
                    type: METRIC_TYPE_HISTOGRAM
                    unit: METRIC_UNIT_MILLISECONDS
                    # value: "1" # We scrub this value since it is indeterminate
                    labels { key: "EncryptionType" value: "UNSET" }
                    labels { key: "Application" value: "APPLICATION_ECL" }
                    labels {
                      key: "MatchKeyFormat"
                      value: "MATCH_KEY_FORMAT_HASHED"
                    }
                  }
                )pb");
        auto scrubbed_request = *ctx.request;
        scrubbed_request.mutable_metrics(0)->clear_value();
        EXPECT_THAT(scrubbed_request, EqualsProto(expected_request));
        ctx.result = SuccessExecutionResult();
        ctx.Finish();
      });

  // MatchTask mock
  EXPECT_CALL(mock_match_task_, Match).WillOnce([](auto ctx) {
    ctx.response = std::make_shared<backend::MatchResponse>();
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });

  grpc::ClientContext context;
  grpc::Status status = stub_->Match(&context, request, &response);
  EXPECT_TRUE(status.ok()) << status.error_message();
}

TEST_F(MatchServiceTest, MatchSuccessfulWithEncryptionKey) {
  MatchServiceOptions options;
  options.allow_all_callers = true;
  StartServer(options);

  MatchRequest request;
  // Contents unimportant, just create the key.
  request.mutable_encryption_key()->mutable_coordinator_key();
  request.set_application(api::v1::APPLICATION_ECL);
  request.set_match_key_format(api::v1::MATCH_KEY_FORMAT_HASHED);
  MatchResponse response;

  bool metric_verified = false;
  EXPECT_CALL(mock_metric_client_, PutMetrics)
      .WillRepeatedly([&metric_verified](auto& ctx) {
        if (ctx.request->metrics(0).name() == "MatchRequestCount") {
          auto expected_request =
              SubstituteAndParseTextToProto<PutMetricsRequest>(
                  R"pb(
                    metric_namespace: "test_namespace"
                    metrics {
                      name: "MatchRequestCount"
                      type: METRIC_TYPE_COUNTER
                      unit: METRIC_UNIT_COUNT
                      value: "1"
                      labels { key: "EncryptionType" value: "COORDINATOR_KEY" }
                      labels { key: "Application" value: "APPLICATION_ECL" }
                      labels {
                        key: "MatchKeyFormat"
                        value: "MATCH_KEY_FORMAT_HASHED"
                      }
                    }
                  )pb");
          EXPECT_THAT(*ctx.request, EqualsProto(expected_request));
          metric_verified = true;
        }
        ctx.result = SuccessExecutionResult();
        ctx.Finish();
      });

  // MatchTask mock
  EXPECT_CALL(mock_match_task_, Match).WillOnce([](auto ctx) {
    ctx.response = std::make_shared<backend::MatchResponse>();
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });

  grpc::ClientContext context;
  grpc::Status status = stub_->Match(&context, request, &response);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_TRUE(metric_verified);
}

TEST_F(MatchServiceTest, MatchFailureLogsErrorMetric) {
  MatchServiceOptions options;
  options.allow_all_callers = true;
  StartServer(options);

  MatchRequest request;
  request.set_application(api::v1::APPLICATION_ECL);
  request.set_match_key_format(api::v1::MATCH_KEY_FORMAT_HASHED);
  MatchResponse response;

  // Ensure that the *error* metric was captured.
  bool error_metric_verified = false;
  EXPECT_CALL(mock_metric_client_, PutMetrics).WillRepeatedly([&](auto& ctx) {
    if (ctx.request->metrics(0).name() == "MatchRequestErrorCount") {
      auto expected_request = SubstituteAndParseTextToProto<PutMetricsRequest>(
          R"pb(
            metric_namespace: "test_namespace"
            metrics {
              name: "MatchRequestErrorCount"
              type: METRIC_TYPE_COUNTER
              unit: METRIC_UNIT_COUNT
              value: "1"
              labels { key: "EncryptionType" value: "UNSET" }
              labels { key: "Application" value: "APPLICATION_ECL" }
              labels { key: "MatchKeyFormat" value: "MATCH_KEY_FORMAT_HASHED" }
              labels { key: "BackendErrorReason" value: "ALREADY_EXISTS" }
            }
          )pb");
      EXPECT_THAT(*ctx.request, EqualsProto(expected_request));
      error_metric_verified = true;
    }
    ctx.result = SuccessExecutionResult();
    ctx.Finish();
  });

  // MatchTask mock returns failure
  EXPECT_CALL(mock_match_task_, Match).WillOnce([](auto ctx) {
    ctx.status = Status(backend::Error_Reason_ALREADY_EXISTS, "msg");
    ctx.Finish();
  });

  grpc::ClientContext context;
  grpc::Status status = stub_->Match(&context, request, &response);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::ALREADY_EXISTS);
  EXPECT_TRUE(error_metric_verified);
}

// Hard to test the positive case for an allowed caller because it is hard to
// create the client context with an allowed caller using grpc public interface.
TEST_F(MatchServiceTest, UnauthorizedReturnsUnauthenticated) {
  MatchServiceOptions options;
  options.allow_all_callers = false;
  options.allowed_client_accounts = {"authorized@gserviceaccount.com"};
  StartServer(options);

  MatchRequest request;
  MatchResponse response;

  // Ensure that the *error* metric was captured.
  bool error_metric_verified = false;
  EXPECT_CALL(mock_metric_client_, PutMetrics).WillRepeatedly([&](auto& ctx) {
    if (ctx.request->metrics(0).name() == "MatchRequestGrpcErrorCount") {
      auto expected_request = SubstituteAndParseTextToProto<PutMetricsRequest>(
          R"pb(
            metric_namespace: "test_namespace"
            metrics {
              name: "MatchRequestGrpcErrorCount"
              type: METRIC_TYPE_COUNTER
              unit: METRIC_UNIT_COUNT
              value: "1"
              labels { key: "EncryptionType" value: "UNSET" }
              labels { key: "Application" value: "APPLICATION_UNSPECIFIED" }
              labels {
                key: "MatchKeyFormat"
                value: "MATCH_KEY_FORMAT_UNSPECIFIED"
              }
              labels { key: "GrpcStatusCode" value: "PERMISSION_DENIED" }
            }
          )pb");
      EXPECT_THAT(*ctx.request, EqualsProto(expected_request));
      error_metric_verified = true;
    }
    ctx.result = SuccessExecutionResult();
    ctx.Finish();
  });

  grpc::ClientContext context;
  grpc::Status status = stub_->Match(&context, request, &response);
  EXPECT_FALSE(status.ok());
  // AltsClientAuthzCheck returns PERMISSION_DENIED when context is missing.
  EXPECT_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
  EXPECT_TRUE(error_metric_verified);
}

TEST_F(MatchServiceTest, MatchPublishesDataRecordAndMatchKeyLevelMetrics) {
  MatchServiceOptions options;
  options.allow_all_callers = true;
  StartServer(options);

  MatchRequest request;
  request.set_application(api::v1::APPLICATION_ECL);
  request.set_match_key_format(api::v1::MATCH_KEY_FORMAT_HASHED_ENCRYPTED);
  request.mutable_encryption_key()->mutable_coordinator_key();

  // record1 has pii-level key, record pii-level key
  auto* record1 = request.add_data_records();
  record1->mutable_encryption_key()->mutable_wrapped_key();
  record1->add_match_keys()
      ->mutable_encryption_key()
      ->mutable_coordinator_key();  // coordinator key
  record1->add_match_keys()
      ->mutable_encryption_key()
      ->mutable_coordinator_key();  // coordinator key
  record1->add_match_keys()
      ->mutable_encryption_key()
      ->mutable_wrapped_key();  // wrapped key

  // record2 has record-level wrapped key and key2 has no key, keeping it valid
  auto* record2 = request.add_data_records();
  record2->mutable_encryption_key()->mutable_wrapped_key();
  record2->add_match_keys();  // inherits wrapped key from record2

  // record3 uses request-level coordinator key
  auto* record3 = request.add_data_records();
  record3->add_match_keys();  // inherits coordinator key from request

  MatchResponse response;

  int record_coordinator_count = 0;
  int record_wrapped_count = 0;
  int record_unset_count = 0;
  int match_key_coordinator_count = 0;
  int match_key_wrapped_count = 0;

  EXPECT_CALL(mock_metric_client_, PutMetrics).WillRepeatedly([&](auto& ctx) {
    EXPECT_EQ(ctx.request->metric_namespace(), "test_namespace");
    for (const auto& metric : ctx.request->metrics()) {
      if (metric.name() == "MatchRequestDataRecordLevelRequestCount") {
        std::string enc_type = metric.labels().at("EncryptionType");
        if (enc_type == "COORDINATOR_KEY") {
          auto expected_metric = SubstituteAndParseTextToProto<Metric>(
              R"pb(
                name: "MatchRequestDataRecordLevelRequestCount"
                type: METRIC_TYPE_COUNTER
                unit: METRIC_UNIT_COUNT
                value: "1"
                labels { key: "EncryptionType" value: "COORDINATOR_KEY" }
                labels { key: "Application" value: "APPLICATION_ECL" }
                labels {
                  key: "MatchKeyFormat"
                  value: "MATCH_KEY_FORMAT_HASHED_ENCRYPTED"
                }
              )pb");
          EXPECT_THAT(metric, EqualsProto(expected_metric));
          record_coordinator_count++;
        } else if (enc_type == "WRAPPED_KEY") {
          auto expected_metric = SubstituteAndParseTextToProto<Metric>(
              R"pb(
                name: "MatchRequestDataRecordLevelRequestCount"
                type: METRIC_TYPE_COUNTER
                unit: METRIC_UNIT_COUNT
                value: "2"
                labels { key: "EncryptionType" value: "WRAPPED_KEY" }
                labels { key: "Application" value: "APPLICATION_ECL" }
                labels {
                  key: "MatchKeyFormat"
                  value: "MATCH_KEY_FORMAT_HASHED_ENCRYPTED"
                }
              )pb");
          EXPECT_THAT(metric, EqualsProto(expected_metric));
          record_wrapped_count++;
        } else if (enc_type == "UNSET") {
          auto expected_metric = SubstituteAndParseTextToProto<Metric>(
              R"pb(
                name: "MatchRequestDataRecordLevelRequestCount"
                type: METRIC_TYPE_COUNTER
                unit: METRIC_UNIT_COUNT
                value: "1"
                labels { key: "EncryptionType" value: "UNSET" }
                labels { key: "Application" value: "APPLICATION_ECL" }
                labels {
                  key: "MatchKeyFormat"
                  value: "MATCH_KEY_FORMAT_HASHED_ENCRYPTED"
                }
              )pb");
          EXPECT_THAT(metric, EqualsProto(expected_metric));
          record_unset_count++;
        }
      } else if (metric.name() == "MatchRequestMatchKeyLevelRequestCount") {
        std::string enc_type = metric.labels().at("EncryptionType");
        if (enc_type == "COORDINATOR_KEY") {
          auto expected_metric = SubstituteAndParseTextToProto<Metric>(
              R"pb(
                name: "MatchRequestMatchKeyLevelRequestCount"
                type: METRIC_TYPE_COUNTER
                unit: METRIC_UNIT_COUNT
                value: "3"
                labels { key: "EncryptionType" value: "COORDINATOR_KEY" }
                labels { key: "Application" value: "APPLICATION_ECL" }
                labels {
                  key: "MatchKeyFormat"
                  value: "MATCH_KEY_FORMAT_HASHED_ENCRYPTED"
                }
              )pb");
          EXPECT_THAT(metric, EqualsProto(expected_metric));
          match_key_coordinator_count++;
        } else if (enc_type == "WRAPPED_KEY") {
          auto expected_metric = SubstituteAndParseTextToProto<Metric>(
              R"pb(
                name: "MatchRequestMatchKeyLevelRequestCount"
                type: METRIC_TYPE_COUNTER
                unit: METRIC_UNIT_COUNT
                value: "2"
                labels { key: "EncryptionType" value: "WRAPPED_KEY" }
                labels { key: "Application" value: "APPLICATION_ECL" }
                labels {
                  key: "MatchKeyFormat"
                  value: "MATCH_KEY_FORMAT_HASHED_ENCRYPTED"
                }
              )pb");
          EXPECT_THAT(metric, EqualsProto(expected_metric));
          match_key_wrapped_count++;
        }
      }
    }
    ctx.result = SuccessExecutionResult();
    ctx.Finish();
  });

  EXPECT_CALL(mock_match_task_, Match).WillOnce([](auto ctx) {
    ctx.response = std::make_shared<backend::MatchResponse>();
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });

  grpc::ClientContext context;
  grpc::Status status = stub_->Match(&context, request, &response);
  EXPECT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(record_coordinator_count, 0);
  EXPECT_EQ(record_wrapped_count, 1);
  EXPECT_EQ(record_unset_count, 1);
  EXPECT_EQ(match_key_coordinator_count, 1);
  EXPECT_EQ(match_key_wrapped_count, 1);
}

}  // namespace
}  // namespace google::confidential_match::match_service
