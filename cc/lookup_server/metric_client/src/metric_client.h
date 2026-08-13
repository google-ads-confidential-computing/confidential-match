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
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"

namespace google::confidential_match::lookup_server {

// Lookup Server metric names
inline constexpr absl::string_view kLookupServerRequestCountMetricName =
    "lookup_server_request_count";
inline constexpr absl::string_view kLookupServerRequestLatencyMetricName =
    "lookup_server_request_latency";
inline constexpr absl::string_view kLookupServerRequestErrorCountMetricName =
    "lookup_server_request_error_count";

// Data loader metric names

// todo: Inaccurate metrics type, following 2 metrics will be deprecated and
// replaced. Measures the time taken to retrieve the latest GCS data export and
// update the lookup table in seconds.
inline constexpr absl::string_view kDataLoaderUpdateDurationMetricName =
    "data_loader_update_duration";
// Measures the time taken to retrieve the latest GCS data export, update to the
// lookup table, and finalize the data upload (ie. cleaning outdated records) in
// seconds.
inline constexpr absl::string_view
    kDataLoaderUpdateFullCycleDurationMetricName =
        "data_loader_update_full_cycle_duration";
// todo: End.

// Measures the time duration since the last successful data refresh in
// seconds.
inline constexpr absl::string_view
    kDataLoaderDurationSinceLastRefreshMetricName =
        "data_loader_duration_since_last_refresh";

// Measures the latency of fetching the DataExportInfo from the
// OrchestratorClient in milliseconds.
inline constexpr absl::string_view kGetDataExportInfoDurationMetricName =
    "data_loader_data_export_info_duration";
// Measures the latency of fetching the ExportMetadata file from the
// DataProvider in milliseconds.
inline constexpr absl::string_view kGetExportMetadataDurationMetricName =
    "data_loader_get_export_metadata_duration";

// Match data provider metric names
// Match data storage metric names
// System level metric names

// Lookup Server metric labels
inline constexpr absl::string_view kKeyFormatLabel = "KeyFormat";
inline constexpr absl::string_view kSuccessfulRequestLabel =
    "IsRequestSuccessful";
inline constexpr absl::string_view kIsSuccessfulLabel = "IsSuccessful";
inline constexpr absl::string_view kBackendErrorReasonLabel =
    "BackendErrorReason";
inline constexpr absl::string_view kClusterIdLabel = "cluster_id";
inline constexpr absl::string_view kClusterGroupIdLabel = "cluster_group_id";

inline constexpr char kKeyFormatUnspecifiedMetricLabel[] = "UNSPECIFIED";
inline constexpr char kKeyFormatHashedMetricLabel[] = "HASHED";
inline constexpr char kKeyFormatHashedEncryptedMetricLabel[] =
    "HASHED_ENCRYPTED";
inline constexpr char kKeyFormatHashedEncryptedCoordinatorMetricLabel[] =
    "HASHED_ENCRYPTED_COORDINATOR_KEY";
inline constexpr char kKeyFormatHashedEncryptedAwsWrappedMetricLabel[] =
    "HASHED_ENCRYPTED_AWS_WRAPPED_KEY";
inline constexpr char kKeyFormatHashedEncryptedGcpWrappedMetricLabel[] =
    "HASHED_ENCRYPTED_GCP_WRAPPED_KEY";

inline constexpr absl::string_view kTrueMetricValue = "true";
inline constexpr absl::string_view kFalseMetricValue = "false";

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

  /**
   * @brief Records a metric with custom type.
   *
   * If a metric fails to be recorded after starting, an error is logged.
   *
   * @param name the name of the metric
   * @param value the value to be recorded for that metric
   * @param unit the unit of the value being recorded
   * @param type the type of the metric (e.g. counter, histogram)
   * @param labels the labels to attach to the metric
   * @return whether the metric recording was started successfully
   */
  scp::core::ExecutionResult RecordMetric(
      absl::string_view name, absl::string_view value, MetricUnit unit,
      MetricType type,
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
