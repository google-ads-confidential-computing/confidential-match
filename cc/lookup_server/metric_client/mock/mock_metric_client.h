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

#ifndef CC_LOOKUP_SERVER_METRIC_CLIENT_MOCK_MOCK_METRIC_CLIENT_H_
#define CC_LOOKUP_SERVER_METRIC_CLIENT_MOCK_MOCK_METRIC_CLIENT_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"

#include "cc/lookup_server/interface/metric_client_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

class MockMetricClient : public MetricClientInterface {
 public:
  MOCK_METHOD(scp::core::ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Stop, (), (noexcept, override));

  scp::core::ExecutionResult RecordMetric(absl::string_view name,
                                          absl::string_view value,
                                          MetricUnit unit) noexcept override {
    return RecordMetric();
  }

  scp::core::ExecutionResult RecordMetric(
      absl::string_view name, absl::string_view value, MetricUnit unit,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept
      override {
    return RecordMetric();
  }

  MOCK_METHOD(scp::core::ExecutionResult, RecordMetric, (), (noexcept));
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_METRIC_CLIENT_MOCK_MOCK_METRIC_CLIENT_H_
