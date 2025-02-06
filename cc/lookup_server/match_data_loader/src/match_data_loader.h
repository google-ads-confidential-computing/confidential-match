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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_LOADER_SRC_MATCH_DATA_LOADER_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_LOADER_SRC_MATCH_DATA_LOADER_H_

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/data_provider_interface.h"
#include "cc/lookup_server/interface/match_data_loader_interface.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/lookup_server/interface/orchestrator_client_interface.h"
#include "cc/lookup_server/interface/streamed_match_data_provider_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"
#include "protos/lookup_server/backend/export_metadata.pb.h"
#include "protos/lookup_server/backend/location.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Service responsible for loading match data from a data provider into
 * a data storage component.
 */
class MatchDataLoader : public MatchDataLoaderInterface {
 public:
  MatchDataLoader(
      std::shared_ptr<DataProviderInterface> data_provider,
      std::shared_ptr<StreamedMatchDataProviderInterface> match_data_provider,
      std::shared_ptr<MatchDataStorageInterface> match_data_storage,
      std::shared_ptr<MetricClientInterface> metric_client,
      std::shared_ptr<OrchestratorClientInterface> orchestrator_client,
      std::shared_ptr<CryptoClientInterface> crypto_client,
      absl::string_view cluster_group_id, absl::string_view cluster_id,
      absl::string_view kms_resource_name, absl::string_view kms_region,
      absl::string_view kms_wip_provider, uint64_t data_refresh_interval_mins)
      : is_running_(false),
        data_provider_(data_provider),
        match_data_provider_(match_data_provider),
        match_data_storage_(match_data_storage),
        metric_client_(metric_client),
        orchestrator_client_(orchestrator_client),
        crypto_client_(crypto_client),
        cluster_group_id_(cluster_group_id),
        cluster_id_(cluster_id),
        kms_resource_name_(kms_resource_name),
        kms_region_(kms_region),
        kms_wip_provider_(kms_wip_provider),
        data_refresh_interval_mins_(data_refresh_interval_mins),
        last_successful_data_load_sec_(absl::ToUnixSeconds(absl::Now())) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  /** @brief Loads match data for the provided data export into storage. */
  scp::core::ExecutionResult Load(
      const proto_backend::DataExportInfo& data_export_info,
      absl::string_view encrypted_dek) noexcept override;

 private:
  /**
   * @brief Periodically checks for new match data and loads it into memory
   * while the service is running.
   */
  void DataRefreshLoop() noexcept;

  /** @brief Loads match data into storage. */
  scp::core::ExecutionResult Load() noexcept;

  /**
   * @brief Handles the callback made after getting the crypto key.
   *
   * @param crypto_key_context the context containing the crypto key result
   * @param data_export_info information tied to the data export
   */
  void HandleGetCryptoKeyCallback(
      const scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                                    CryptoKeyInterface>& crypto_key_context,
      const proto_backend::DataExportInfo& data_export_info) noexcept;

  /**
   * @brief Handles the callbacks made after starting the match data fetch.
   *
   * @param get_match_data_context the context containing a match data batch
   * @param get_match_data_context_is_finished indicates whether the upstream
   * data provider has finished making all callbacks
   * @param data_export_info information tied to the data export
   * @param start_time the time at which the data fetch process was started
   * @param record_count the counter tracking of the total number of records
   * @param key_count the counter tracking of the total number of hash keys
   * @param threads_started_count the counter tracking the number of threads
   * that have been started so far
   * @param threads_finished_count the counter tracking the number of threads
   * that have completed so far
   * @param queue_reads_complete a boolean tracking whether all data has been
   * read from the concurrent data queue (and no further data will be added)
   */
  void HandleMatchDataBatchCallback(
      scp::core::ConsumerStreamingContext<
          lookup_server::proto_backend::Location, MatchDataBatch>
          get_match_data_context,
      bool get_match_data_context_is_finished,
      const proto_backend::DataExportInfo& data_export_info,
      absl::Time start_time, std::shared_ptr<std::atomic_uint64_t> record_count,
      std::shared_ptr<std::atomic_uint64_t> key_count,
      std::shared_ptr<std::atomic_uint64_t> threads_started_count,
      std::shared_ptr<std::atomic_uint64_t> threads_finished_count,
      std::shared_ptr<std::atomic_bool> queue_reads_complete) noexcept;

  /**
   * @brief Helper to finalize a successful data loading operation.
   *
   * @param metric_labels the metric labels to be recorded on completion
   * @param record_count the final number of records written to the table
   * @param key_count the final number of keys written to the table
   * @param start_time the time at which the data fetch process was started
   */
  void FinalizeUpdate(
      absl::flat_hash_map<std::string, std::string> metric_labels,
      uint64_t record_count, uint64_t key_count,
      absl::Time start_time) noexcept;

  /** @brief Fetches the data export info from the Orchestrator. */
  scp::core::ExecutionResultOr<proto_backend::DataExportInfo>
  GetDataExportInfo() noexcept;

  /** @brief Fetches the export metadata from the provided location. */
  scp::core::ExecutionResultOr<proto_backend::ExportMetadata> GetExportMetadata(
      const proto_backend::Location& location) noexcept;

  /** @brief Helper to record a count metric to the cloud. */
  void RecordMetric(
      absl::string_view name, uint64_t count,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept;

  /** @brief Helper to record a duration metric to the cloud. */
  void RecordMetric(
      absl::string_view name, absl::Duration duration,
      const absl::flat_hash_map<std::string, std::string>& labels) noexcept;

  std::atomic<bool> is_running_;
  std::shared_ptr<DataProviderInterface> data_provider_;
  std::shared_ptr<StreamedMatchDataProviderInterface> match_data_provider_;
  std::shared_ptr<MatchDataStorageInterface> match_data_storage_;
  std::shared_ptr<MetricClientInterface> metric_client_;
  std::shared_ptr<OrchestratorClientInterface> orchestrator_client_;
  std::shared_ptr<CryptoClientInterface> crypto_client_;
  const std::string cluster_group_id_;
  const std::string cluster_id_;
  const std::string kms_resource_name_;
  const std::string kms_region_;
  const std::string kms_wip_provider_;
  // The delay time to wait between data refresh operations in minutes
  const uint64_t data_refresh_interval_mins_;
  // Unix timestamp for the last successful data load operation in seconds.
  // Used for metrics and alerting purposes
  std::atomic<uint64_t> last_successful_data_load_sec_;

  std::unique_ptr<std::thread> data_loading_thread_;
};
}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_MATCH_DATA_LOADER_SRC_MATCH_DATA_LOADER_H_
