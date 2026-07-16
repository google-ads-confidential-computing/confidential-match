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

#include <memory>
#include <utility>
#include <vector>

#include "cc/core/async/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/logger/log.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match::match_service::metrics {

using ::google::cmrt::sdk::metric_service::v1::PutMetricsRequest;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::errors::GetErrorMessage;

void PutMetric(
    std::shared_ptr<google::confidential_match::LoggerInterface> logger,
    google::scp::cpio::MetricClientInterface* metric_client,
    absl::string_view metric_namespace,
    google::cmrt::sdk::metric_service::v1::Metric metric) {
  if (metric_client == nullptr) {
    return;
  }

  AsyncContext<PutMetricsRequest, PutMetricsResponse> ctx;
  ctx.request = std::make_shared<PutMetricsRequest>();
  *ctx.request->add_metrics() = std::move(metric);
  ctx.request->set_metric_namespace(metric_namespace);

  ctx.callback = [logger = std::move(logger)](auto& context) {
    if (!context.result.Successful()) {
      LOG_ERROR(*logger, "Failed to PutMetric [%s]: %s",
                context.request->metrics(0).name(),
                GetErrorMessage(context.result.status_code));
    }
  };

  metric_client->PutMetrics(ctx);
}

void PutMetrics(
    std::shared_ptr<google::confidential_match::LoggerInterface> logger,
    google::scp::cpio::MetricClientInterface* metric_client,
    absl::string_view metric_namespace,
    std::vector<google::cmrt::sdk::metric_service::v1::Metric> metrics) {
  if (metric_client == nullptr || metrics.empty()) {
    return;
  }

  AsyncContext<PutMetricsRequest, PutMetricsResponse> ctx;
  ctx.request = std::make_shared<PutMetricsRequest>();
  ctx.request->set_metric_namespace(metric_namespace);

  auto* proto_metrics = ctx.request->mutable_metrics();
  proto_metrics->Reserve(metrics.size());
  for (auto& metric : metrics) {
    *proto_metrics->Add() = std::move(metric);
  }

  ctx.callback = [logger = std::move(logger)](auto& context) {
    if (!context.result.Successful()) {
      LOG_ERROR(*logger, "Failed to PutMetrics for a batch of metrics: %s",
                GetErrorMessage(context.result.status_code));
    }
  };

  metric_client->PutMetrics(ctx);
}

}  // namespace google::confidential_match::match_service::metrics
