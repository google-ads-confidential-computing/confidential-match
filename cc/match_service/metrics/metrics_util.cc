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
#include <string>
#include <utility>
#include <vector>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/core/async/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/logger/log.h"
#include "cc/public/core/interface/execution_result.h"
#include "grpcpp/ext/otel_plugin.h"
#include "grpcpp/grpcpp.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/metrics/aggregation/histogram_aggregation.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/view/instrument_selector_factory.h"
#include "opentelemetry/sdk/metrics/view/meter_selector_factory.h"
#include "opentelemetry/sdk/metrics/view/view_factory.h"

namespace google::confidential_match::match_service::metrics {
namespace {

using ::google::cmrt::sdk::metric_service::v1::Metric;
using ::google::cmrt::sdk::metric_service::v1::MetricType;
using ::google::cmrt::sdk::metric_service::v1::MetricUnit;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsRequest;
using ::google::cmrt::sdk::metric_service::v1::PutMetricsResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::errors::GetErrorMessage;

// Histogram bucket boundaries (in seconds) with millisecond resolution starting
// at 1ms (0.001s) up to 10s so that p50, p95, p99, and p99.9 can be accurately
// computed for gRPC call durations.
std::vector<double> GetGrpcDurationHistogramBoundaries() {
  return {// 1ms - 10ms
          0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.008, 0.01,
          // 13ms - 100ms
          0.013, 0.016, 0.02, 0.025, 0.03, 0.04, 0.05, 0.065, 0.08, 0.1,
          // 130ms - 1s
          0.13, 0.16, 0.2, 0.25, 0.3, 0.4, 0.5, 0.65, 0.8, 1,
          // 1.3s - 10s
          1.3, 1.6, 2, 2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10};
}

void ConfigureGrpcDurationHistogramViews(
    opentelemetry::sdk::metrics::MeterProvider* sdk_meter_provider) {
  if (sdk_meter_provider == nullptr) {
    return;
  }
  for (absl::string_view instrument_name :
       {grpc::OpenTelemetryPluginBuilder::kServerCallDurationInstrumentName,
        grpc::OpenTelemetryPluginBuilder::
            kClientAttemptDurationInstrumentName}) {
    auto histogram_config = std::make_shared<
        opentelemetry::sdk::metrics::HistogramAggregationConfig>();
    histogram_config->boundaries_ = GetGrpcDurationHistogramBoundaries();
    sdk_meter_provider->AddView(
        opentelemetry::sdk::metrics::InstrumentSelectorFactory::Create(
            opentelemetry::sdk::metrics::InstrumentType::kHistogram,
            std::string(instrument_name), "s"),
        opentelemetry::sdk::metrics::MeterSelectorFactory::Create(
            "grpc-c++", grpc::Version(), ""),
        opentelemetry::sdk::metrics::ViewFactory::Create(
            std::string(instrument_name), "",
            opentelemetry::sdk::metrics::AggregationType::kHistogram,
            std::move(histogram_config)));
  }
}

}  // namespace

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
      if (logger != nullptr) {
        LOG_ERROR(*logger, "Failed to PutMetric [%s]: %s",
                  context.request->metrics(0).name(),
                  GetErrorMessage(context.result.status_code));
      } else {
        LOG(ERROR) << "Failed to PutMetric ["
                   << context.request->metrics(0).name()
                   << "]: " << GetErrorMessage(context.result.status_code);
      }
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
      if (logger != nullptr) {
        LOG_ERROR(*logger, "Failed to PutMetrics for a batch of metrics: %s",
                  GetErrorMessage(context.result.status_code));
      } else {
        LOG(ERROR) << "Failed to PutMetrics for a batch of metrics: "
                   << GetErrorMessage(context.result.status_code);
      }
    }
  };

  metric_client->PutMetrics(ctx);
}

Metric CreateServerStartupLatencyMetric(absl::Duration duration) {
  Metric m;
  m.set_name(std::string(kServerStartupLatencyInMilliMetricName));
  m.set_type(MetricType::METRIC_TYPE_GAUGE);
  m.set_value(absl::StrCat(absl::ToInt64Milliseconds(duration)));
  m.set_unit(MetricUnit::METRIC_UNIT_MILLISECONDS);
  return m;
}

Metric CreateServerStartupErrorCountMetric() {
  Metric m;
  m.set_name(std::string(kServerStartupErrorCountMetricName));
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  return m;
}

absl::Status RegisterGrpcOpenTelemetryPlugin() {
  auto meter_provider = opentelemetry::metrics::Provider::GetMeterProvider();
  auto* sdk_meter_provider =
      dynamic_cast<opentelemetry::sdk::metrics::MeterProvider*>(
          meter_provider.get());
  if (sdk_meter_provider == nullptr) {
    return absl::FailedPreconditionError(
        "Global OpenTelemetry MeterProvider is not an SDK MeterProvider; "
        "ensure MetricClient::Init() is called before registering the gRPC "
        "OpenTelemetry plugin.");
  }

  static const absl::Status* const registration_status =
      new absl::Status([sdk_meter_provider, &meter_provider]() {
        ConfigureGrpcDurationHistogramViews(sdk_meter_provider);
        return grpc::OpenTelemetryPluginBuilder()
            .SetMeterProvider(meter_provider)
            .BuildAndRegisterGlobal();
      }());
  return *registration_status;
}

}  // namespace google::confidential_match::match_service::metrics
