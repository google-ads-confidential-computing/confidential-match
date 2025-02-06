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

#include "cc/lookup_server/metric_client/src/metric_client.h"

#include <memory>

#include "absl/strings/str_format.h"
#include "cc/core/interface/type_def.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "public/cpio/mock/metric_client/mock_metric_client.h"

#include "cc/lookup_server/metric_client/mock/fake_metric_client.h"
#include "cc/lookup_server/metric_client/mock/mock_metric_client.h"
#include "cc/lookup_server/metric_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::metric_service::v1::Metric;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsRequest;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::testing::Return;

constexpr absl::string_view kMetricNamespace = "metric-namespace";
constexpr absl::string_view kMetricName = "metric-name";
constexpr absl::string_view kMetricValue = "1";
constexpr MetricUnit kMetricUnit = MetricUnit::METRIC_UNIT_COUNT;
constexpr absl::string_view kBaseLabelKey = "base-label-key";
constexpr absl::string_view kBaseLabelValue = "base-label-value";
constexpr absl::string_view kLabelKey = "label-key";
constexpr absl::string_view kLabelValue = "label-value";

class MetricClientTest : public testing::Test {
 public:
  ExecutionResult CaptureAsyncContext(
      AsyncContext<PutMetricsRequest, PutMetricsResponse> context);

 protected:
  MetricClientTest()
      : mock_cpio_metric_client_(
            std::make_shared<scp::cpio::MockMetricClient>()),
        metric_client_(mock_cpio_metric_client_, kMetricNamespace,
                       absl::flat_hash_map<std::string, std::string>()) {}

  std::shared_ptr<scp::cpio::MockMetricClient> mock_cpio_metric_client_;
  MetricClient metric_client_;
  AsyncContext<PutMetricsRequest, PutMetricsResponse> captured_context_;
};

TEST_F(MetricClientTest, StartStop) {
  EXPECT_CALL(*mock_cpio_metric_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_metric_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_metric_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));

  EXPECT_TRUE(metric_client_.Init().Successful());
  EXPECT_TRUE(metric_client_.Run().Successful());
  EXPECT_TRUE(metric_client_.Stop().Successful());
}

ExecutionResult MetricClientTest::CaptureAsyncContext(
    AsyncContext<PutMetricsRequest, PutMetricsResponse> context) {
  captured_context_ = context;
  return SuccessExecutionResult();
}

TEST_F(MetricClientTest, RecordMetricIsSuccessful) {
  EXPECT_CALL(*mock_cpio_metric_client_, PutMetrics)
      .WillOnce(Invoke(this, &MetricClientTest::CaptureAsyncContext));
  PutMetricsRequest expected;
  *expected.mutable_metric_namespace() = kMetricNamespace;
  Metric* expected_metric = expected.add_metrics();
  *expected_metric->mutable_name() = kMetricName;
  *expected_metric->mutable_value() = kMetricValue;
  expected_metric->set_unit(kMetricUnit);

  ExecutionResult result =
      metric_client_.RecordMetric(kMetricName, kMetricValue, kMetricUnit);

  EXPECT_SUCCESS(result);
  // ignore timestamp
  expected_metric->mutable_timestamp()->set_seconds(
      captured_context_.request->metrics().Get(0).timestamp().seconds());
  EXPECT_THAT(*captured_context_.request, EqualsProto(expected));
}

TEST_F(MetricClientTest, RecordMetricWithLabelsIsSuccessful) {
  EXPECT_CALL(*mock_cpio_metric_client_, PutMetrics)
      .WillOnce(Invoke(this, &MetricClientTest::CaptureAsyncContext));
  PutMetricsRequest expected;
  *expected.mutable_metric_namespace() = kMetricNamespace;
  Metric* expected_metric = expected.add_metrics();
  *expected_metric->mutable_name() = kMetricName;
  *expected_metric->mutable_value() = kMetricValue;
  expected_metric->set_unit(kMetricUnit);
  (*expected_metric->mutable_labels())[kLabelKey] = kLabelValue;

  absl::flat_hash_map<std::string, std::string> labels;
  labels[kLabelKey] = kLabelValue;

  ExecutionResult result = metric_client_.RecordMetric(
      kMetricName, kMetricValue, kMetricUnit, labels);

  EXPECT_SUCCESS(result);
  // ignore timestamp
  expected_metric->mutable_timestamp()->set_seconds(
      captured_context_.request->metrics().Get(0).timestamp().seconds());
  EXPECT_THAT(*captured_context_.request, EqualsProto(expected));
}

TEST_F(MetricClientTest, RecordMetricWithBaseLabelsIsSuccessful) {
  absl::flat_hash_map<std::string, std::string> base_labels;
  base_labels[kBaseLabelKey] = kBaseLabelValue;
  MetricClient metric_client(mock_cpio_metric_client_, kMetricNamespace,
                             base_labels);
  EXPECT_CALL(*mock_cpio_metric_client_, PutMetrics)
      .WillOnce(Invoke(this, &MetricClientTest::CaptureAsyncContext));

  PutMetricsRequest expected;
  *expected.mutable_metric_namespace() = kMetricNamespace;
  Metric* expected_metric = expected.add_metrics();
  *expected_metric->mutable_name() = kMetricName;
  *expected_metric->mutable_value() = kMetricValue;
  expected_metric->set_unit(kMetricUnit);
  (*expected_metric->mutable_labels())[kBaseLabelKey] = kBaseLabelValue;

  ExecutionResult result =
      metric_client.RecordMetric(kMetricName, kMetricValue, kMetricUnit);

  EXPECT_SUCCESS(result);
  // ignore timestamp
  expected_metric->mutable_timestamp()->set_seconds(
      captured_context_.request->metrics().Get(0).timestamp().seconds());
  EXPECT_THAT(*captured_context_.request, EqualsProto(expected));
}

TEST_F(MetricClientTest, RecordMetricWithBaseAndArgLabelsIsSuccessful) {
  absl::flat_hash_map<std::string, std::string> base_labels;
  base_labels[kBaseLabelKey] = kBaseLabelValue;
  MetricClient metric_client(mock_cpio_metric_client_, kMetricNamespace,
                             base_labels);
  EXPECT_CALL(*mock_cpio_metric_client_, PutMetrics)
      .WillOnce(Invoke(this, &MetricClientTest::CaptureAsyncContext));

  PutMetricsRequest expected;
  *expected.mutable_metric_namespace() = kMetricNamespace;
  Metric* expected_metric = expected.add_metrics();
  *expected_metric->mutable_name() = kMetricName;
  *expected_metric->mutable_value() = kMetricValue;
  expected_metric->set_unit(kMetricUnit);
  (*expected_metric->mutable_labels())[kBaseLabelKey] = kBaseLabelValue;
  (*expected_metric->mutable_labels())[kLabelKey] = kLabelValue;

  absl::flat_hash_map<std::string, std::string> labels;
  labels[kLabelKey] = kLabelValue;

  ExecutionResult result = metric_client.RecordMetric(kMetricName, kMetricValue,
                                                      kMetricUnit, labels);

  EXPECT_SUCCESS(result);
  // ignore timestamp
  expected_metric->mutable_timestamp()->set_seconds(
      captured_context_.request->metrics().Get(0).timestamp().seconds());
  EXPECT_THAT(*captured_context_.request, EqualsProto(expected));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
