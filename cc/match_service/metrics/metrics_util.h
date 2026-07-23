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

#ifndef CC_MATCH_SERVICE_METRICS_METRICS_UTIL_H_
#define CC_MATCH_SERVICE_METRICS_METRICS_UTIL_H_

#include <memory>
#include <vector>

#include "absl/strings/string_view.h"
#include "cc/core/logger/logger_interface.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "public/cpio/proto/metric_service/v1/metric_service.pb.h"

namespace google::confidential_match::match_service::metrics {

// Metric names
inline constexpr absl::string_view kKeyFetchingRequestCountMetricName =
    "KeyFetchingRequestCount";
inline constexpr absl::string_view kKeyFetchingRequestLatencyMetricName =
    "KeyFetchingRequestLatency";
inline constexpr absl::string_view kKeyFetchingRequestErrorCountMetricName =
    "KeyFetchingRequestErrorCount";

inline constexpr absl::string_view kLookupServerRequestCountMetricName =
    "LookupServerRequestCount";
inline constexpr absl::string_view kLookupServerRequestLatencyMetricName =
    "LookupServerRequestLatency";
inline constexpr absl::string_view kLookupServerRequestErrorCountMetricName =
    "LookupServerRequestErrorCount";

inline constexpr absl::string_view kRequestCountMetricName =
    "MatchRequestCount";
inline constexpr absl::string_view kDataRecordLevelRequestCountMetricName =
    "MatchRequestDataRecordLevelRequestCount";
inline constexpr absl::string_view kMatchKeyLevelRequestCountMetricName =
    "MatchRequestMatchKeyLevelRequestCount";
inline constexpr absl::string_view kRequestLatencyMetricName =
    "MatchRequestLatency";
inline constexpr absl::string_view kErrorCountMetricName =
    "MatchRequestErrorCount";
inline constexpr absl::string_view kGrpcErrorCountMetricName =
    "MatchRequestGrpcErrorCount";
inline constexpr absl::string_view kServerStartupLatencyMetricName =
    "ServerStartupLatency";
inline constexpr absl::string_view kServerStartupErrorCountMetricName =
    "ServerStartupErrorCount";

// Metric labels and values
inline constexpr absl::string_view kEncryptionTypeLabel = "EncryptionType";
inline constexpr absl::string_view kNotEncrypted = "UNENCRYPTED";
inline constexpr absl::string_view kUnset = "UNSET";
inline constexpr absl::string_view kWrappedKeyType = "WRAPPED_KEY";
inline constexpr absl::string_view kCoordinatorKeyType = "COORDINATOR_KEY";

inline constexpr absl::string_view kApplicationLabel = "Application";
inline constexpr absl::string_view kMatchKeyFormatLabel = "MatchKeyFormat";
inline constexpr absl::string_view kBackendErrorLabel = "BackendError";
inline constexpr absl::string_view kBackendErrorReasonLabel =
    "BackendErrorReason";
inline constexpr absl::string_view kGrpcStatusCodeLabel = "GrpcStatusCode";

inline constexpr absl::string_view kShardLabel = "LookupServerShard";
inline constexpr absl::string_view kKeyFormatLabel = "KeyFormat";

// Asynchronously publishes a metric using the provided MetricClient.
// Logs an error message to the logger if the metric client fails to record it.
// If metric_client is nullptr, this is a no-op.
void PutMetric(
    std::shared_ptr<google::confidential_match::LoggerInterface> logger,
    google::scp::cpio::MetricClientInterface* metric_client,
    absl::string_view metric_namespace,
    google::cmrt::sdk::metric_service::v1::Metric metric);

// Asynchronously publishes a batch of metrics using the provided MetricClient.
// Logs an error message to the logger if the metric client fails to record it.
// If metric_client is nullptr or metrics is empty, this is a no-op.
void PutMetrics(
    std::shared_ptr<google::confidential_match::LoggerInterface> logger,
    google::scp::cpio::MetricClientInterface* metric_client,
    absl::string_view metric_namespace,
    std::vector<google::cmrt::sdk::metric_service::v1::Metric> metrics);

google::cmrt::sdk::metric_service::v1::Metric CreateServerStartupLatencyMetric(
    absl::Duration duration);

google::cmrt::sdk::metric_service::v1::Metric
CreateServerStartupErrorCountMetric();

}  // namespace google::confidential_match::match_service::metrics

#endif  // CC_MATCH_SERVICE_METRICS_METRICS_UTIL_H_
