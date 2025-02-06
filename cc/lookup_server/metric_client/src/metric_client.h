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

#ifndef CC_LOOKUP_SERVER_METRIC_CLIENT_SRC_METRIC_CLIENT_H_
#define CC_LOOKUP_SERVER_METRIC_CLIENT_SRC_METRIC_CLIENT_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"

#include "cc/core/data_table/src/data_table.h"
#include "cc/lookup_server/interface/metric_client_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Client responsible for recording metrics.
 */
class MetricClient : public MetricClientInterface {
 public:
  /**
   * @brief Initializes a metric client.
   *
   * Uses a default metric namespace of `gce_instance`.
   */
  MetricClient();

  /**
   * @brief Initializes a metric client.
   *
   * @param metric_namespace the namespace that metrics will be written to
   * @param base_labels a map of labels to be included on all put requests
   */
  explicit MetricClient(
      absl::string_view metric_namespace,
      const absl::flat_hash_map<std::string, std::string>& base_labels);

  /**
   * @brief Initializes a metric client.
   *
   * @param cpio_metric_client the underlying CPIO client to use
   * @param metric_namespace the namespace that metrics will be written to
   * @param base_labels a map of labels to be included on all put requests
   */
  explicit MetricClient(
      std::shared_ptr<scp::cpio::MetricClientInterface> cpio_metric_client,
      absl::string_view metric_namespace,
      const absl::flat_hash_map<std::string, std::string>& base_labels);

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  /**
   * @brief Records a metric.
   *
   * If a metric fails to be recorded after starting, an error is logged.
   *
   * @param name the name of the metric
   * @param value the value to be recorded for that metric
   * @param unit the unit of the value being recorded
   * @return whether the metric recording was started successfully
   */
  scp::core::ExecutionResult RecordMetric(absl::string_view name,
                                          absl::string_view value,
                                          MetricUnit unit) noexcept override;

  /**
   * @brief Records a metric.
   *
   * If a metric fails to be recorded after starting, an error is logged.
   *
   * @param name the name of the metric
   * @param value the value to be recorded for that metric
   * @param unit the unit of the value being recorded
   * @param labels the labels to attach to the metric
   * @return whether the metric recording was started successfully
   */
  scp::core::ExecutionResult RecordMetric(
      absl::string_view name, absl::string_view value, MetricUnit unit,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept
      override;

 private:
  // The internal CPIO metric client that is used to record metrics.
  std::shared_ptr<scp::cpio::MetricClientInterface> cpio_metric_client_;
  // The namespace that metrics will be written to.
  std::string metric_namespace_;
  // A mapping of base labels to be added to all recorded metrics.
  absl::flat_hash_map<std::string, std::string> base_labels_;
};
}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_METRIC_CLIENT_SRC_METRIC_CLIENT_H_
