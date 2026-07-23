// Copyright 2026 Google LLC
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

#include "cc/match_service/metrics/metrics_util.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include <gmock/gmock.h>

#include "absl/log/scoped_mock_log.h"
#include "absl/time/time.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/mock/metric_client/mock_metric_client.h"

namespace google::confidential_match::match_service::metrics {
namespace {

using ::google::scp::core::FailureExecutionResult;
using ::google::scp::cpio::MockMetricClient;
using ::testing::_;
using ::testing::HasSubstr;

class MetricsUtilTest : public ::testing::Test {
 protected:
  MockMetricClient mock_metric_client_;
};

TEST_F(MetricsUtilTest, LogsErrorWhenLoggerIsNullAndPutMetricFails) {
  absl::ScopedMockLog mock_log;

  EXPECT_CALL(mock_log,
              Log(absl::LogSeverity::kError, _,
                  HasSubstr("Failed to PutMetric [ServerStartupLatency]: ")))
      .Times(1);

  EXPECT_CALL(mock_metric_client_, PutMetrics).WillOnce([](auto& ctx) {
    ctx.result = FailureExecutionResult(12345);
    ctx.Finish();
  });

  mock_log.StartCapturingLogs();

  google::cmrt::sdk::metric_service::v1::Metric m;
  m.set_name(std::string(kServerStartupLatencyMetricName));
  PutMetric(/*logger=*/nullptr, &mock_metric_client_, "namespace",
            std::move(m));
}

TEST_F(MetricsUtilTest, CreateServerStartupLatencyMetricHasCorrectValues) {
  absl::Duration duration = absl::Seconds(5);
  auto metric = CreateServerStartupLatencyMetric(duration);
  EXPECT_EQ(metric.name(), kServerStartupLatencyMetricName);
  EXPECT_EQ(metric.value(), "5000");
  EXPECT_EQ(metric.unit(), google::cmrt::sdk::metric_service::v1::MetricUnit::
                               METRIC_UNIT_MILLISECONDS);
  EXPECT_EQ(
      metric.type(),
      google::cmrt::sdk::metric_service::v1::MetricType::METRIC_TYPE_HISTOGRAM);
}

TEST_F(MetricsUtilTest, CreateServerStartupErrorCountMetricHasCorrectValues) {
  auto metric = CreateServerStartupErrorCountMetric();
  EXPECT_EQ(metric.name(), kServerStartupErrorCountMetricName);
  EXPECT_EQ(metric.value(), "1");
  EXPECT_EQ(
      metric.unit(),
      google::cmrt::sdk::metric_service::v1::MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(
      metric.type(),
      google::cmrt::sdk::metric_service::v1::MetricType::METRIC_TYPE_COUNTER);
}

}  // namespace
}  // namespace google::confidential_match::match_service::metrics
