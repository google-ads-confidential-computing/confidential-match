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

#include <future>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "cc/public/cpio/proto/metric_service/v1/metric_service.pb.h"

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
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::cpio::MetricClientFactory;
using ::google::scp::cpio::MetricClientOptions;

constexpr absl::string_view kComponentName = "MetricClient";
// The default namespace that metrics will be written to.
constexpr absl::string_view kDefaultMetricNamespace = "gce_instance";

}  // namespace

MetricClient::MetricClient()
    : cpio_metric_client_(MetricClientFactory::Create(MetricClientOptions())),
      metric_namespace_(kDefaultMetricNamespace),
      base_labels_() {}

MetricClient::MetricClient(
    absl::string_view metric_namespace,
    const absl::flat_hash_map<std::string, std::string>& base_labels)
    : cpio_metric_client_(MetricClientFactory::Create(MetricClientOptions())),
      metric_namespace_(metric_namespace),
      base_labels_(base_labels) {}

MetricClient::MetricClient(
    std::shared_ptr<scp::cpio::MetricClientInterface> cpio_metric_client,
    absl::string_view metric_namespace,
    const absl::flat_hash_map<std::string, std::string>& base_labels)
    : cpio_metric_client_(cpio_metric_client),
      metric_namespace_(metric_namespace),
      base_labels_(base_labels) {}

ExecutionResult MetricClient::Init() noexcept {
  return cpio_metric_client_->Init();
}

ExecutionResult MetricClient::Run() noexcept {
  return cpio_metric_client_->Run();
}

ExecutionResult MetricClient::Stop() noexcept {
  return cpio_metric_client_->Stop();
}

ExecutionResult MetricClient::RecordMetric(absl::string_view name,
                                           absl::string_view value,
                                           MetricUnit unit) noexcept {
  return RecordMetric(name, value, unit,
                      absl::flat_hash_map<std::string, std::string>());
}

ExecutionResult MetricClient::RecordMetric(
    absl::string_view name, absl::string_view value, MetricUnit unit,
    const absl::flat_hash_map<std::string, std::string>& labels) noexcept {
  AsyncContext<PutMetricsRequest, PutMetricsResponse> put_metrics_context;
  put_metrics_context.request = std::make_shared<PutMetricsRequest>();
  *put_metrics_context.request->mutable_metric_namespace() = metric_namespace_;
  Metric* metric = put_metrics_context.request->add_metrics();
  *metric->mutable_name() = name;
  *metric->mutable_value() = value;
  metric->set_unit(unit);
  metric->mutable_labels()->insert(base_labels_.begin(), base_labels_.end());
  metric->mutable_labels()->insert(labels.begin(), labels.end());
  // TODO(b/309462764): Remove if fixed on SCP side
  metric->mutable_timestamp()->set_seconds(absl::ToUnixSeconds(absl::Now()));

  put_metrics_context.callback = [](auto& context) -> void {
    if (!context.result.Successful()) {
      SCP_ERROR(
          kComponentName, kZeroUuid, context.result,
          absl::StrFormat("Failed to record metric. Error: %s, metric: %s",
                          GetErrorMessage(context.result.status_code),
                          context.request->DebugString()));
    }
  };

  cpio_metric_client_->PutMetrics(put_metrics_context);
  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
