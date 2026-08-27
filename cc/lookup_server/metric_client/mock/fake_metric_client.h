/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CC_LOOKUP_SERVER_METRIC_CLIENT_MOCK_FAKE_METRIC_CLIENT_H_
#define CC_LOOKUP_SERVER_METRIC_CLIENT_MOCK_FAKE_METRIC_CLIENT_H_

#include <mutex>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

class FakeMetricClient : public MetricClientInterface {
 public:
  struct RecordedMetric {
    std::string name;
    std::string value;
    MetricUnit unit;
    MetricType type;
    absl::flat_hash_map<std::string, std::string> labels;
  };

  scp::core::ExecutionResult Init() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Run() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Stop() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  // TODO(b/542801533): Remove this method when cleaning up legacy metrics.
  scp::core::ExecutionResult RecordMetric(absl::string_view name,
                                          absl::string_view value,
                                          MetricUnit unit) noexcept override {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    recorded_metrics_.push_back({
        std::string(name),
        std::string(value),
        unit,
        MetricType::METRIC_TYPE_UNKNOWN,
        {},
    });
    return scp::core::SuccessExecutionResult();
  }

  // TODO(b/542801533): Remove this method when cleaning up legacy metrics.
  scp::core::ExecutionResult RecordMetric(
      absl::string_view name, absl::string_view value, MetricUnit unit,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept
      override {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    recorded_metrics_.push_back({
        std::string(name),
        std::string(value),
        unit,
        MetricType::METRIC_TYPE_UNKNOWN,
        labels,
    });
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult RecordMetric(
      absl::string_view name, absl::string_view value, MetricUnit unit,
      MetricType type,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept
      override {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    recorded_metrics_.push_back({
        std::string(name),
        std::string(value),
        unit,
        type,
        labels,
    });
    return scp::core::SuccessExecutionResult();
  }

  const std::vector<RecordedMetric>& GetRecordedMetrics() const noexcept {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    return recorded_metrics_;
  }

  void ClearRecordedMetrics() noexcept {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    recorded_metrics_.clear();
  }

 private:
  mutable std::mutex metrics_mutex_;
  std::vector<RecordedMetric> recorded_metrics_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_METRIC_CLIENT_MOCK_FAKE_METRIC_CLIENT_H_
