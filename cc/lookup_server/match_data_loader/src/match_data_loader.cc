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

#include "cc/lookup_server/match_data_loader/src/match_data_loader.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "cc/core/common/concurrent_queue/src/concurrent_queue.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/core/interface/type_def.h"
#include "cc/public/core/interface/execution_result.h"
#include "external/tink_cc/proto/tink.pb.h"

#include "cc/lookup_server/match_data_loader/src/error_codes.h"
#include "cc/lookup_server/match_data_storage/src/error_codes.h"
#include "cc/lookup_server/parsers/src/export_metadata_parser.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"
#include "protos/lookup_server/backend/export_metadata.pb.h"
#include "protos/lookup_server/backend/exporter_data_row.pb.h"
#include "protos/lookup_server/backend/location.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    DataExportInfo;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::
    ExporterAssociatedData_KeyValue;
using ::google::confidential_match::lookup_server::proto_backend::
    ExporterDataRow;
using ::google::confidential_match::lookup_server::proto_backend::
    ExportMetadata;
using ::google::confidential_match::lookup_server::proto_backend::Location;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::crypto::tink::EncryptedKeyset;
using ::google::crypto::tink::Keyset;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ConsumerStreamingContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::ExecutionStatus;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::ConcurrentQueue;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::std::placeholders::_1;
using ::std::placeholders::_2;

constexpr absl::string_view kComponentName = "MatchDataLoader";

// Metric measuring the number of data records loaded into memory.
constexpr absl::string_view kRecordCountMetricName = "record_count";
// Metric measuring the number of unique hash keys loaded into memory.
constexpr absl::string_view kKeyCountMetricName = "key_count";
// Metric measuring the time taken to fetch all data and add it to the table.
constexpr absl::string_view kDataFetchingDurationMetricName =
    "data_fetching_duration";
// Metric measuring the time it takes to fetch/add all data plus the time
// taken to finalize the data upload (ie. cleaning outdated records).
constexpr absl::string_view kTableUpdateDurationMetricName =
    "table_update_duration";
// Metric measuring the time duration from the last successful data refresh to
// the current time.
constexpr absl::string_view kDurationSinceLastRefreshName =
    "duration_since_last_refresh";

constexpr absl::string_view kDataExportIdMetricKey = "data_export_id";
constexpr absl::string_view kShardingSchemeTypeMetricKey =
    "sharding_scheme_type";
constexpr absl::string_view kShardingSchemeNumShardsMetricKey =
    "sharding_scheme_num_shards";
constexpr absl::string_view kIsSuccessfulMetricKey = "is_successful";
constexpr absl::string_view kTrueMetricValue = "true";
constexpr absl::string_view kFalseMetricValue = "false";

// The delay before retrying when a data loading operation fails to start
constexpr int kLoadErrorRetryDelaySeconds = 30;
// Logs the duration since the last successful data refresh to cloud metrics
// roughly once every N iterations of the data refresh loop.
constexpr int kLogLastRefreshMetricsEveryNIterations = 300;
// Logs progress of data loading, showing the count roughly once every N threads
constexpr int kLogRecordCountEveryNThreads = 100;

// Parses a base64 encoded DEK string into an EncryptedKeyset and returns the
// encrypted portion into an encoded string
ExecutionResult ParseEncryptedKeyset(absl::string_view encrypted_dek,
                                     std::string& encoded_keyset) {
  std::string decoded_dek;
  if (!absl::Base64Unescape(encrypted_dek, &decoded_dek)) {
    return FailureExecutionResult(MATCH_DATA_LOADER_INVALID_ENCRYPTED_DEK);
  }
  EncryptedKeyset encrypted_keyset;
  if (!encrypted_keyset.ParseFromString(decoded_dek)) {
    return FailureExecutionResult(MATCH_DATA_LOADER_INVALID_ENCRYPTED_DEK);
  }
  absl::Base64Escape(encrypted_keyset.encrypted_keyset(), &encoded_keyset);
  return SuccessExecutionResult();
}

// Builds a mapping of labels used for metric recording, using information
// within a DataExportInfo object.
absl::flat_hash_map<std::string, std::string> BuildMetricLabels(
    const DataExportInfo& data_export_info) {
  absl::flat_hash_map<std::string, std::string> labels;
  labels[kDataExportIdMetricKey] = data_export_info.data_export_id();
  labels[kShardingSchemeTypeMetricKey] =
      data_export_info.sharding_scheme().type();
  labels[kShardingSchemeNumShardsMetricKey] =
      data_export_info.sharding_scheme().num_shards();
  return labels;
}

}  // namespace

ExecutionResult MatchDataLoader::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult MatchDataLoader::Run() noexcept {
  if (is_running_.exchange(true)) {
    return FailureExecutionResult(MATCH_DATA_LOADER_ALREADY_RUNNING);
  }

  std::promise<void> thread_started_promise;
  data_loading_thread_ =
      std::make_unique<std::thread>([this, &thread_started_promise] {
        thread_started_promise.set_value();
        DataRefreshLoop();
      });

  thread_started_promise.get_future().wait();
  return SuccessExecutionResult();
}

ExecutionResult MatchDataLoader::Stop() noexcept {
  if (!is_running_.exchange(false)) {
    return FailureExecutionResult(MATCH_DATA_LOADER_ALREADY_STOPPED);
  }

  if (data_loading_thread_->joinable()) {
    data_loading_thread_->join();
  }

  return SuccessExecutionResult();
}

void MatchDataLoader::DataRefreshLoop() noexcept {
  std::chrono::time_point next_load_time = std::chrono::steady_clock::now();

  while (is_running_.load()) {
    if (std::rand() % kLogLastRefreshMetricsEveryNIterations == 0) {
      RecordMetric(
          kDurationSinceLastRefreshName,
          absl::Now() - absl::FromUnixSeconds(last_successful_data_load_sec_),
          absl::flat_hash_map<std::string, std::string>());
    }

    if (std::chrono::steady_clock::now() < next_load_time) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    ExecutionResult data_loading_result = Load();
    if (!data_loading_result.Successful()) {
      SCP_ERROR(kComponentName, kZeroUuid, data_loading_result,
                absl::StrCat("Failed to start the data loading operation: ",
                             GetErrorMessage(data_loading_result.status_code)));
      next_load_time = std::chrono::steady_clock::now() +
                       std::chrono::seconds(kLoadErrorRetryDelaySeconds);
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat(
                   "Failed loading operation will be retried in %d seconds.",
                   kLoadErrorRetryDelaySeconds));
      continue;
    }

    next_load_time = std::chrono::steady_clock::now() +
                     std::chrono::minutes(data_refresh_interval_mins_);
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrFormat(
                 "Next data loading operation will be started in %d minutes.",
                 data_refresh_interval_mins_));
  }
}

ExecutionResult MatchDataLoader::Load() noexcept {
  ExecutionResultOr<DataExportInfo> data_export_info_or = GetDataExportInfo();
  RETURN_IF_FAILURE(data_export_info_or.result());

  ExecutionResultOr<ExportMetadata> export_metadata_or =
      GetExportMetadata(data_export_info_or->metadata_location());
  RETURN_IF_FAILURE(export_metadata_or.result());

  ExecutionResult load_result =
      Load(*data_export_info_or, export_metadata_or->encrypted_dek());
  if (!load_result.Successful()) {
    // For now, just log a warning and continue with service startup in case
    // data is not available from current environment.
    SCP_WARNING(kComponentName, kZeroUuid,
                "Failed to load match data on startup.");
    return load_result;
  }

  return SuccessExecutionResult();
}

ExecutionResult MatchDataLoader::Load(
    const DataExportInfo& data_export_info,
    absl::string_view encrypted_dek) noexcept {
  const Location& location = data_export_info.shard_location();

  if (data_export_info.data_export_id() ==
      match_data_storage_->GetDatasetId()) {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrFormat(
                 "Skipping data loading operation, dataset is up to date. "
                 "(Bucket: '%s', Path: '%s', Dataset ID: '%s')",
                 location.blob_storage_location().bucket_name(),
                 location.blob_storage_location().path(),
                 data_export_info.data_export_id()));
    return SuccessExecutionResult();
  } else if (data_export_info.data_export_id() ==
             match_data_storage_->GetPendingDatasetId()) {
    // This may happen if previous data load operation failed and needs to be
    // retried.
    // In the event that a previous operation is taking too long to complete,
    // retrying will also not cause issues. All operations will terminate once
    // any data loading operation completes successfully
    SCP_WARNING(
        kComponentName, kZeroUuid,
        "Found matching pending dataset, data load operation will be retried.");
  }
  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Starting data loading operation. "
                           "(Bucket: '%s', Path: '%s', Dataset ID: '%s')",
                           location.blob_storage_location().bucket_name(),
                           location.blob_storage_location().path(),
                           data_export_info.data_export_id()));

  // Decrypt the encrypted DEK, then start data loading in the callback if
  // successful
  std::string encoded_keyset;
  RETURN_IF_FAILURE(ParseEncryptedKeyset(encrypted_dek, encoded_keyset));
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> decrypt_context;
  decrypt_context.request = std::make_shared<EncryptionKeyInfo>();
  auto* wrapped_key_info = decrypt_context.request->mutable_wrapped_key_info();
  wrapped_key_info->set_encrypted_dek(encoded_keyset);
  wrapped_key_info->set_kek_kms_resource_id(kms_resource_name_);
  wrapped_key_info->mutable_gcp_wrapped_key_info()->set_wip_provider(
      kms_wip_provider_);
  decrypt_context.callback = std::bind(
      &MatchDataLoader::HandleGetCryptoKeyCallback, this, _1, data_export_info);
  crypto_client_->GetCryptoKey(decrypt_context);

  return SuccessExecutionResult();
}

void MatchDataLoader::HandleGetCryptoKeyCallback(
    const AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>&
        crypto_key_context,
    const proto_backend::DataExportInfo& data_export_info) noexcept {
  if (!crypto_key_context.result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, crypto_key_context,
                      crypto_key_context.result,
                      "Failed to build the data loading crypto key.");
    return;
  }

  std::shared_ptr<CryptoKeyInterface> data_encryption_key =
      crypto_key_context.response;
  ExecutionResult start_update_result =
      match_data_storage_->StartUpdate(data_export_info);
  if (start_update_result.status_code ==
      MATCH_DATA_STORAGE_UPDATE_ALREADY_IN_PROGRESS) {
    return;
  } else if (!start_update_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, start_update_result,
              absl::StrCat("Failed to start the update process: ",
                           GetErrorMessage(start_update_result.status_code)));
    return;
  }

  absl::Time start_time = absl::Now();
  auto key_count = std::make_shared<std::atomic_uint64_t>(0);
  auto record_count = std::make_shared<std::atomic_uint64_t>(0);

  // Used to determine when all data (across all threads) have been
  // written to the table, so that we can mark the update complete.
  // The ConsumerStreamingContext API currently does not provide this info:
  // `finished` only indicates that all data has been written to the queue,
  // and `TryGetNextResponse() == nullptr` only indicates that all data has
  // been consumed from the queue, which is different but significant
  auto threads_started_count = std::make_shared<std::atomic_uint64_t>(0);
  auto threads_finished_count = std::make_shared<std::atomic_uint64_t>(0);
  auto queue_reads_complete = std::make_shared<std::atomic_bool>(false);

  ConsumerStreamingContext<Location, MatchDataBatch> get_match_data_context(
      INT64_MAX);
  get_match_data_context.request =
      std::make_shared<Location>(data_export_info.shard_location());
  get_match_data_context.process_callback = std::bind(
      &MatchDataLoader::HandleMatchDataBatchCallback, this, _1, _2,
      data_export_info, start_time, key_count, record_count,
      threads_started_count, threads_finished_count, queue_reads_complete);

  // Start fetching match data, then process each match data batch within the
  // callback handler
  ExecutionResult result = match_data_provider_->GetMatchData(
      get_match_data_context, data_encryption_key);
  if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result, "GetMatchData failed.");
    return;
  }
  SCP_INFO(kComponentName, kZeroUuid, "Data loading started.");
}

void MatchDataLoader::HandleMatchDataBatchCallback(
    ConsumerStreamingContext<Location, MatchDataBatch> context,
    bool context_is_finished, const DataExportInfo& data_export_info,
    absl::Time start_time, std::shared_ptr<std::atomic_uint64_t> key_count,
    std::shared_ptr<std::atomic_uint64_t> record_count,
    std::shared_ptr<std::atomic_uint64_t> threads_started_count,
    std::shared_ptr<std::atomic_uint64_t> threads_finished_count,
    std::shared_ptr<std::atomic_bool> queue_reads_complete) noexcept {
  ++*threads_started_count;

  if (context_is_finished && context.result != SuccessExecutionResult()) {
    SCP_ERROR_CONTEXT(
        kComponentName, context, context.result,
        absl::StrCat("Data loading finished with an error result.",
                     GetErrorMessage(context.result.status_code)));

    ExecutionResult cancel_result = match_data_storage_->CancelUpdate();
    if (!cancel_result.Successful()) {
      SCP_CRITICAL_CONTEXT(kComponentName, context, cancel_result,
                           "Unable to cancel failed data loading operation.");
    }

    absl::flat_hash_map<std::string, std::string> metric_labels =
        BuildMetricLabels(data_export_info);
    metric_labels[kIsSuccessfulMetricKey] = kFalseMetricValue;
    RecordMetric(kRecordCountMetricName, record_count->load(), metric_labels);
    RecordMetric(kKeyCountMetricName, key_count->load(), metric_labels);
    RecordMetric(kTableUpdateDurationMetricName, absl::Now() - start_time,
                 metric_labels);
    return;
  }

  std::unique_ptr<MatchDataBatch> match_data_batch =
      context.TryGetNextResponse();
  if (match_data_batch == nullptr) {
    // A nullptr means that the queue is finished
    if (!context.IsMarkedDone()) {
      auto dequeue_result =
          FailureExecutionResult(MATCH_DATA_LOADER_DEQUEUE_ERROR);
      // Should never happen
      SCP_ERROR_CONTEXT(
          kComponentName, context, dequeue_result,
          "Failed to dequeue match data though stream was not finished.");
      return;
    }

    // At this point, all stream data has been read for loading
    SCP_INFO(kComponentName, kZeroUuid,
             "All match data was received from cloud storage successfully.");
    queue_reads_complete->store(true);
    match_data_batch = std::make_unique<MatchDataBatch>();
  }

  if (std::rand() % kLogRecordCountEveryNThreads == 0) {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrFormat("Loaded %d records, %d record keys...",
                             record_count->load(), key_count->load()));
  }
  key_count->fetch_add(match_data_batch->size());

  for (const auto& match_data_group : *match_data_batch) {
    if (match_data_group.empty()) {
      continue;
    }
    record_count->fetch_add(match_data_group.size());

    ExecutionResult load_result = match_data_storage_->Replace(
        match_data_group[0].key(), match_data_group);
    if (!load_result.Successful()) {
      SCP_ERROR_CONTEXT(kComponentName, context, load_result,
                        "Failed to load match data row to storage.");
      context.MarkDone();
      return;
    }
  }
  // Storing the atomically incremented count guarantees that each thread has a
  // unique value, so that exactly one thread executes the finalize operation.
  uint64_t num_finished_threads = ++*threads_finished_count;

  if (queue_reads_complete->load()) {
    if (threads_started_count->load() == num_finished_threads) {
      // At this point, all other threads have completed writing match data
      // successfully
      absl::flat_hash_map<std::string, std::string> metric_labels =
          BuildMetricLabels(data_export_info);
      RecordMetric(kDataFetchingDurationMetricName, absl::Now() - start_time,
                   metric_labels);
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat("Match data inserted to table. Total records: "
                               "%d, total keys: %d (using %d threads)",
                               record_count->load(), key_count->load(),
                               threads_started_count->load()));

      // Executing a long-running task on an AsyncExecutor results in a fraction
      // of subsequent tasks getting blocked until the task completes, when
      // tasks are placed on the same job queue as the long-running task.
      // Finalizing the update in a separate thread ensures that no
      // AsyncExecutor threads are blocked.
      std::thread finalize_thread(&MatchDataLoader::FinalizeUpdate, this,
                                  metric_labels, record_count->load(),
                                  key_count->load(), start_time);
      // The thread won't run for the entire lifetime of the program, but will
      // terminate at the end of the finalize process. We call detach() instead
      // of join() since we currently don't have a good place to join() at the
      // end of the process.
      finalize_thread.detach();
    } else {
      SCP_INFO(
          kComponentName, kZeroUuid,
          absl::StrFormat("All data has been received, however one or more "
                          "threads are still processing data. FinalizeUpdate "
                          "will be deferred to the last finishing thread. "
                          "Thread number: %d, total threads: %d. "
                          "Loaded %d records, %d record keys so far.",
                          num_finished_threads, threads_started_count->load(),
                          record_count->load(), key_count->load()));
    }
  }
}

void MatchDataLoader::FinalizeUpdate(
    absl::flat_hash_map<std::string, std::string> metric_labels,
    uint64_t record_count, uint64_t key_count, absl::Time start_time) noexcept {
  ExecutionResult finalize_result = match_data_storage_->FinalizeUpdate();
  if (!finalize_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, finalize_result,
              "Failed to finalize the match data update process.");
    metric_labels[kIsSuccessfulMetricKey] = kFalseMetricValue;
  } else {
    metric_labels[kIsSuccessfulMetricKey] = kTrueMetricValue;
    last_successful_data_load_sec_ = absl::ToUnixSeconds(absl::Now());
  }

  RecordMetric(kRecordCountMetricName, record_count, metric_labels);
  RecordMetric(kKeyCountMetricName, key_count, metric_labels);
  RecordMetric(kTableUpdateDurationMetricName, absl::Now() - start_time,
               metric_labels);
  RecordMetric(
      kDurationSinceLastRefreshName,
      absl::Now() - absl::FromUnixSeconds(last_successful_data_load_sec_),
      metric_labels);
}

ExecutionResultOr<DataExportInfo>
MatchDataLoader::GetDataExportInfo() noexcept {
  // TODO(b/271863149): Use asynchronous method to avoid blocking.
  GetDataExportInfoRequest request = {.cluster_group_id = cluster_group_id_,
                                      .cluster_id = cluster_id_};
  ExecutionResultOr<GetDataExportInfoResponse> export_info_or =
      orchestrator_client_->GetDataExportInfo(request);
  RETURN_IF_FAILURE(export_info_or.result());
  return *export_info_or->data_export_info;
}

ExecutionResultOr<ExportMetadata> MatchDataLoader::GetExportMetadata(
    const Location& location) noexcept {
  // TODO(b/271863149): Use asynchronous implementation to avoid blocking.
  std::promise<ExecutionResult> result_promise;
  std::promise<std::string> raw_export_metadata_promise;

  AsyncContext<Location, std::string> context;
  context.request = std::make_shared<Location>(location);
  context.callback = [&result_promise,
                      &raw_export_metadata_promise](auto& context) {
    result_promise.set_value(context.result);
    if (context.result.Successful()) {
      raw_export_metadata_promise.set_value(*context.response);
    }
  };

  ExecutionResult schedule_result = data_provider_->Get(context);
  if (!schedule_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, schedule_result,
              absl::StrFormat("Unable to schedule export metadata fetch. "
                              "(Bucket: '%s', Path: '%s')",
                              location.blob_storage_location().bucket_name(),
                              location.blob_storage_location().path()));
    return schedule_result;
  }

  ExecutionResult get_result = result_promise.get_future().get();
  if (!get_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, get_result,
              absl::StrFormat("Error while fetching export metadata. "
                              "(Bucket: '%s', Path: '%s')",
                              location.blob_storage_location().bucket_name(),
                              location.blob_storage_location().path()));
    return get_result;
  }

  ExecutionResultOr<ExportMetadata> export_metadata_or =
      ParseExportMetadata(raw_export_metadata_promise.get_future().get());
  RETURN_IF_FAILURE(export_metadata_or.result());

  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Retrieved export metadata. "
                           "(Bucket: '%s', Path: '%s')",
                           location.blob_storage_location().bucket_name(),
                           location.blob_storage_location().path()));

  return *export_metadata_or;
}

void MatchDataLoader::RecordMetric(
    absl::string_view name, uint64_t count,
    const absl::flat_hash_map<std::string, std::string>& labels) noexcept {
  ExecutionResult result = metric_client_->RecordMetric(
      name, std::to_string(count), MetricUnit::METRIC_UNIT_COUNT, labels);
  if (!result.Successful()) {
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to record count metric. (Key: %s, value: %d)",
                        name, count))
  }
}

void MatchDataLoader::RecordMetric(
    absl::string_view name, absl::Duration duration,
    const absl::flat_hash_map<std::string, std::string>& labels) noexcept {
  int64_t seconds = absl::ToInt64Seconds(duration);
  ExecutionResult result = metric_client_->RecordMetric(
      name, std::to_string(seconds), MetricUnit::METRIC_UNIT_SECONDS, labels);
  if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat(
                  "Failed to record duration metric. (Key: %s, value: %d)",
                  name, seconds))
  }
}

}  // namespace google::confidential_match::lookup_server
