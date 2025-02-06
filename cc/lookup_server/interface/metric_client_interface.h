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

#ifndef CC_LOOKUP_SERVER_INTERFACE_METRIC_CLIENT_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_METRIC_CLIENT_INTERFACE_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/proto/metric_service/v1/metric_service.pb.h"

#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

// Defines the unit type of the metric.
using MetricUnit = ::google::cmrt::sdk::metric_service::v1::MetricUnit;

/** @brief Interface for a client used to record metrics. */
class MetricClientInterface : public scp::core::ServiceInterface {
 public:
  virtual ~MetricClientInterface() = default;

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
  virtual scp::core::ExecutionResult RecordMetric(absl::string_view name,
                                                  absl::string_view value,
                                                  MetricUnit unit) noexcept = 0;

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
  virtual scp::core::ExecutionResult RecordMetric(
      absl::string_view name, absl::string_view value, MetricUnit unit,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_METRIC_CLIENT_INTERFACE_H_
