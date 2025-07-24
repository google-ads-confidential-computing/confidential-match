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

#include "cc/lookup_server/match_data_provider/src/blob_storage_match_data_provider.h"

#include <stdint.h>

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "cc/core/common/concurrent_queue/src/concurrent_queue.h"
#include "cc/core/common/concurrent_queue/src/error_codes.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"
#include "cc/public/cpio/proto/blob_storage_service/v1/blob_storage_service.pb.h"
#include "re2/re2.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/streamed_match_data_provider_interface.h"
#include "cc/lookup_server/match_data_provider/src/error_codes.h"
#include "cc/lookup_server/parsers/src/match_data_file_parser.h"
#include "protos/lookup_server/backend/location.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobStreamRequest;
using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobStreamResponse;
using ::google::cmrt::sdk::blob_storage_service::v1::ListBlobsMetadataRequest;
using ::google::cmrt::sdk::blob_storage_service::v1::ListBlobsMetadataResponse;
using ::google::confidential_match::lookup_server::proto_backend::Location;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ConsumerStreamingContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::FinishStreamingContext;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::ConcurrentQueue;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::SC_CONCURRENT_QUEUE_CANNOT_DEQUEUE;
using ::google::scp::cpio::BlobStorageClientInterface;
using ::re2::LazyRE2;
using ::re2::RE2;
using ::std::placeholders::_1;
using ::std::placeholders::_2;

constexpr absl::string_view kComponentName = "BlobStorageMatchDataProvider";
// Pattern for the name of a shard fragment file.
// This is kept in sync with the name format defined in Match Data Exporter.
static constexpr LazyRE2 kShardFragmentFilePattern = {
    R"(fragment-(\d+-)?\d+-of-\d+.txt$)"};

/**
 * @brief Housekeeping object for a ConsumerStreamingContext that tracks
 * all related state required for concurrent-safe writing of match data.
 */
class BlobStorageMatchDataProvider::MatchDataContext {
 public:
  /** @brief Constructs a MatchDataContext. */
  explicit MatchDataContext(
      const ConsumerStreamingContext<Location, MatchDataBatch>& context)
      : context_(std::make_shared<
                 ConsumerStreamingContext<Location, MatchDataBatch>>(context)),
        marked_finished_(std::make_shared<std::atomic_bool>(false)) {}

  /**
   * @brief Pushes the match data batch onto the streaming context and signals
   * to the client callback that the message is available for processing.
   *
   * @return whether or not the push was successful
   */
  ExecutionResult Emit(const MatchDataBatch& match_data_batch) noexcept {
    RETURN_IF_FAILURE(context_->TryPushResponse(match_data_batch));
    context_->ProcessNextMessage();
    return SuccessExecutionResult();
  }

  /**
   * @brief Marks that the context is finished with the provided result.
   *
   * @param result the ExecutionResult to mark the context with
   * @return whether the finish operation was applied for the calling thread
   */
  bool Finish(ExecutionResult result) noexcept {
    // Ensure that the finish call is executed at most one time.
    // Multiple calls can happen when threads fail for example. Regardless,
    // the finish operation must only be performed once
    if (marked_finished_->exchange(true)) {
      return false;
    }
    context_->result = result;
    context_->MarkDone();
    context_->Finish();
    return true;
  }

  /** @brief Returns whether the context has been marked as finished. */
  bool IsFinished() const noexcept { return marked_finished_->load(); }

 private:
  std::shared_ptr<ConsumerStreamingContext<Location, MatchDataBatch>> context_;
  std::shared_ptr<std::atomic_bool> marked_finished_;
};

/**
 * @brief Housekeeping object tracking all relevant information required for
 * a concurrent match data fetch operation.
 */
class BlobStorageMatchDataProvider::GetMatchDataStreamTracker {
 public:
  GetMatchDataStreamTracker() = delete;

  /**
   * @brief Constructs a GetMatchDataStreamTracker.
   *
   * @param get_match_data_context the streaming context to which parsed match
   * data will be written
   * @param fragment_files a list of fragment files containing match data to be
   * fetched
   * @return a pointer to a stream tracker, or an error if could not be created
   */
  static ExecutionResultOr<std::unique_ptr<GetMatchDataStreamTracker>> Create(
      const MatchDataContext& get_match_data_context,
      absl::Span<const Location> fragment_files) noexcept {
    auto tracker = std::unique_ptr<GetMatchDataStreamTracker>(
        new GetMatchDataStreamTracker(get_match_data_context,
                                      fragment_files.size()));
    for (const auto& fragment_file : fragment_files) {
      RETURN_IF_FAILURE(
          tracker->remaining_fragment_files_.TryEnqueue(fragment_file));
    }
    return tracker;
  }

  /**
   * @brief Returns the next fragment file path to be processed.
   *
   * @return the location to the next file or an error if no files remain
   */
  ExecutionResultOr<Location> GetNextFragmentFile() noexcept {
    Location fragment_file;
    ExecutionResult result =
        remaining_fragment_files_.TryDequeue(fragment_file);
    if (result.status_code == SC_CONCURRENT_QUEUE_CANNOT_DEQUEUE) {
      return FailureExecutionResult(MATCH_DATA_PROVIDER_QUEUE_COMPLETED_ERROR);
    } else if (!result.Successful()) {
      return result;
    }

    return fragment_file;
  };

  /**
   * Emits a match data batch back to the original caller through the
   * provided match data streaming context.
   *
   * @param match_data_batch the match data batch to emit
   * @return whether or not the emit was successful
   */
  ExecutionResult Emit(const MatchDataBatch& match_data_batch) {
    return get_match_data_context_.Emit(match_data_batch);
  }

  /**
   * @brief Marks that a single stream has finished the data loading operation
   * for its fragment file.
   *
   * This should be called exactly once per file fragment fetch operation.
   *
   * This also handles marking the parent context as complete when all streams
   * have finished successfully or when any stream fails with an error.
   *
   * @param result whether or not the stream finished successfully
   */
  void MarkSubstreamFinished(const ExecutionResult& result) {
    // On error, immediately mark the parent stream as finished
    if (!result.Successful()) {
      get_match_data_context_.Finish(result);
      return;
    }

    // Otherwise if successful, only mark the parent stream as finished if
    // all other streams have also completed successfully
    size_t num_successful = ++num_successfully_completed_streams_;
    if (num_successful == total_stream_count_) {
      get_match_data_context_.Finish(SuccessExecutionResult());
    }
  }

 private:
  GetMatchDataStreamTracker(const MatchDataContext& get_match_data_context,
                            size_t num_fragment_files)
      : get_match_data_context_(get_match_data_context),
        remaining_fragment_files_(num_fragment_files),
        num_successfully_completed_streams_(0),
        total_stream_count_(num_fragment_files) {}

  // The get match data context to which match data will be written.
  MatchDataContext get_match_data_context_;
  // The fragment files that remain to be processed.
  ConcurrentQueue<Location> remaining_fragment_files_;
  // The number of file fetches that have successfully completed so far.
  // This is used (together with total_stream_count_) to determine when to make
  // the final finish call on the match data streaming context.
  std::atomic<size_t> num_successfully_completed_streams_;
  // The number of streams to be run to complete the data loading operation.
  size_t total_stream_count_;
};

BlobStorageMatchDataProvider::BlobStorageMatchDataProvider(
    std::shared_ptr<BlobStorageClientInterface> blob_storage_client,
    std::shared_ptr<DataProviderInterface> blob_storage_data_provider,
    uint64_t max_concurrent_file_reads)
    : blob_storage_client_(blob_storage_client),
      blob_storage_data_provider_(blob_storage_data_provider),
      max_concurrent_file_reads_(max_concurrent_file_reads) {}

ExecutionResult BlobStorageMatchDataProvider::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult BlobStorageMatchDataProvider::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult BlobStorageMatchDataProvider::Stop() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult BlobStorageMatchDataProvider::GetMatchData(
    ConsumerStreamingContext<Location, MatchDataBatch> get_match_data_context,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept {
  AsyncContext<Location, std::vector<Location>> list_context;
  list_context.request =
      std::make_shared<Location>(*get_match_data_context.request);
  list_context.callback = std::bind(
      &BlobStorageMatchDataProvider::HandleListFragmentFilesCallback, this, _1,
      MatchDataContext(get_match_data_context), data_encryption_key);
  return ListFragmentFiles(list_context);
}

void BlobStorageMatchDataProvider::HandleListFragmentFilesCallback(
    AsyncContext<Location, std::vector<Location>>& list_fragment_files_context,
    MatchDataContext get_match_data_context,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept {
  if (!list_fragment_files_context.result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, list_fragment_files_context.result,
              "Failed to list fragment files.");
    get_match_data_context.Finish(list_fragment_files_context.result);
    return;
  }
  std::shared_ptr<std::vector<Location>> locations =
      list_fragment_files_context.response;

  SCP_INFO(
      kComponentName, kZeroUuid,
      absl::StrFormat("Found %d match data shard fragment file%s.",
                      locations->size(), locations->size() == 1 ? "" : "s"));
  if (locations->empty()) {
    get_match_data_context.Finish(SuccessExecutionResult());
    return;
  }

  // Object managing state for concurrent file fetching and parsing operations
  ExecutionResultOr<std::unique_ptr<GetMatchDataStreamTracker>> tracker_or =
      GetMatchDataStreamTracker::Create(get_match_data_context, *locations);
  if (!tracker_or.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, tracker_or.result(),
              "Failed to initialize the stream tracker.");
    get_match_data_context.Finish(tracker_or.result());
    return;
  }
  std::shared_ptr<GetMatchDataStreamTracker> stream_tracker =
      std::move(*tracker_or);

  // Fire off asynchronous file fetching threads. Multiple files (amount set by
  // `max_concurrent_file_reads_`) will be concurrently fetched and parsed at a
  // time, until all files have been read
  for (uint64_t i = 0; i < max_concurrent_file_reads_; ++i) {
    ExecutionResult start_result =
        StartAsyncFileReader(stream_tracker, data_encryption_key);
    if (!start_result.Successful()) {
      SCP_ERROR(kComponentName, kZeroUuid, start_result,
                "Failed to start the async fetch operation.");
      get_match_data_context.Finish(start_result);
      return;
    }
  }

  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat(
               "Data loading process started using %d concurrent file readers.",
               max_concurrent_file_reads_));
}

ExecutionResult BlobStorageMatchDataProvider::StartAsyncFileReader(
    std::shared_ptr<GetMatchDataStreamTracker> stream_tracker,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept {
  // Pop the next file to be processed from the queue
  ExecutionResultOr<Location> location_or =
      stream_tracker->GetNextFragmentFile();
  if (location_or.result() ==
      FailureExecutionResult(MATCH_DATA_PROVIDER_QUEUE_COMPLETED_ERROR)) {
    // All files have already been processed, terminate since no work remains
    return SuccessExecutionResult();

  } else if (!location_or.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, location_or.result(),
              "Failed to dequeue next fragment file.");
    stream_tracker->MarkSubstreamFinished(location_or.result());
    return location_or.result();
  }

  AsyncContext<Location, std::string> file_fetch_context;
  file_fetch_context.request = std::make_shared<Location>(*location_or);
  file_fetch_context.callback =
      std::bind(&BlobStorageMatchDataProvider::HandleSingleFileCallback, this,
                _1, stream_tracker, data_encryption_key);
  ExecutionResult schedule_result =
      blob_storage_data_provider_->Get(file_fetch_context);
  if (!schedule_result.Successful()) {
    SCP_ERROR(
        kComponentName, kZeroUuid, schedule_result,
        absl::StrFormat(
            "Unable to start a fragment file fetch. (Bucket: '%s', Path: '%s')",
            location_or->blob_storage_location().bucket_name(),
            location_or->blob_storage_location().path()));
    stream_tracker->MarkSubstreamFinished(schedule_result);
    return schedule_result;
  }

  return SuccessExecutionResult();
}

void BlobStorageMatchDataProvider::HandleSingleFileCallback(
    AsyncContext<Location, std::string> file_fetch_context,
    std::shared_ptr<GetMatchDataStreamTracker> stream_tracker,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept {
  if (!file_fetch_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, file_fetch_context, file_fetch_context.result,
        absl::StrFormat(
            "Unable to fetch a fragment file. (Bucket: '%s', Path: '%s')",
            file_fetch_context.request->blob_storage_location().bucket_name(),
            file_fetch_context.request->blob_storage_location().path()));
    stream_tracker->MarkSubstreamFinished(file_fetch_context.result);
    return;
  }

  MatchDataBatch data_batch;
  ExecutionResult parse_result = ParseMatchDataFile(
      *file_fetch_context.response, data_encryption_key, data_batch);
  if (!parse_result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, file_fetch_context, parse_result,
                      "Failed to parse the fragment file.");
    stream_tracker->MarkSubstreamFinished(parse_result);
    return;
  }

  // Emit the match data up to the calling context
  ExecutionResult emit_result = stream_tracker->Emit(data_batch);
  if (!emit_result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, file_fetch_context, emit_result,
                      "Failed to emit match data batch to calling context.");
    stream_tracker->MarkSubstreamFinished(emit_result);
    return;
  }

  stream_tracker->MarkSubstreamFinished(SuccessExecutionResult());

  // Schedule the next file read job
  ExecutionResult next_start_result =
      StartAsyncFileReader(stream_tracker, data_encryption_key);
  if (!next_start_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, next_start_result,
              "Failed to schedule the next async fetch operation.");
  }
}

ExecutionResultOr<std::vector<Location>>
BlobStorageMatchDataProvider::ListFragmentFiles(
    const Location& location) noexcept {
  std::promise<std::vector<Location>> locations_promise;
  std::promise<ExecutionResult> result_promise;

  AsyncContext<Location, std::vector<Location>> list_context;
  list_context.request = std::make_shared<Location>(location);
  list_context.callback = [&locations_promise,
                           &result_promise](auto& context) -> void {
    if (context.result.Successful()) {
      locations_promise.set_value(*context.response);
    }
    result_promise.set_value(context.result);
  };

  ExecutionResult start_result = ListFragmentFiles(list_context);
  if (!start_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, start_result,
              "Failed to start the list blobs operation.");
    return FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
  }

  ExecutionResult list_result = result_promise.get_future().get();
  if (!list_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, list_result,
              "Error occurred while listing blob storage paths.");
    return FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
  }

  return locations_promise.get_future().get();
}

ExecutionResult BlobStorageMatchDataProvider::ListFragmentFiles(
    AsyncContext<Location, std::vector<Location>>& context) noexcept {
  return ListFragmentFilesImpl(context, "");
}

ExecutionResult BlobStorageMatchDataProvider::ListFragmentFilesImpl(
    AsyncContext<Location, std::vector<Location>>& parent_context,
    absl::string_view page_token) noexcept {
  AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse>
      list_context;
  list_context.request = std::make_shared<ListBlobsMetadataRequest>();
  list_context.request->mutable_blob_metadata()->set_bucket_name(
      parent_context.request->blob_storage_location().bucket_name());

  // SCP list API performs a prefix match when given a directory, eg.
  // listing directory "/shard-1" will match directories "/shard-1",
  // "/shard-10", ... Terminate directories with a slash to ensure correct
  // behavior
  std::string shard_path =
      parent_context.request->blob_storage_location().path();
  if (!absl::EndsWith(shard_path, ".txt") && !absl::EndsWith(shard_path, "/")) {
    absl::StrAppend(&shard_path, "/");
  }
  list_context.request->mutable_blob_metadata()->set_blob_name(shard_path);

  if (!page_token.empty()) {
    *list_context.request->mutable_page_token() = page_token;
  }

  list_context.callback =
      std::bind(&BlobStorageMatchDataProvider::HandleListBlobsMetadataCallback,
                this, parent_context, _1);

  blob_storage_client_->ListBlobsMetadata(list_context);
  return SuccessExecutionResult();
}

void BlobStorageMatchDataProvider::HandleListBlobsMetadataCallback(
    AsyncContext<Location, std::vector<Location>>& parent_context,
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse>&
        context) noexcept {
  if (!context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, context, context.result,
        absl::StrCat("Got error response from list blobs operation: ",
                     GetErrorMessage(context.result.status_code)));
    parent_context.result =
        FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
    parent_context.Finish();
    return;
  }

  if (parent_context.response == nullptr) {
    parent_context.response = std::make_shared<std::vector<Location>>();
  }
  for (auto& blob_metadata : *context.response->mutable_blob_metadatas()) {
    if (!RE2::PartialMatch(blob_metadata.blob_name(),
                           *kShardFragmentFilePattern)) {
      continue;
    }

    Location location;
    location.mutable_blob_storage_location()->set_allocated_bucket_name(
        blob_metadata.release_bucket_name());
    location.mutable_blob_storage_location()->set_allocated_path(
        blob_metadata.release_blob_name());
    parent_context.response->push_back(std::move(location));
  }

  if (context.response->next_page_token().empty()) {
    parent_context.result = SuccessExecutionResult();
    parent_context.Finish();
    return;
  }

  ExecutionResult next_result = ListFragmentFilesImpl(
      parent_context, context.response->next_page_token());
  if (!next_result.Successful()) {
    parent_context.result = context.result;
    parent_context.Finish();
    return;
  }
}

}  // namespace google::confidential_match::lookup_server
