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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_BLOB_STORAGE_MATCH_DATA_PROVIDER_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_BLOB_STORAGE_MATCH_DATA_PROVIDER_H_  // NOLINT(whitespace/line_length)

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/service_interface.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/data_provider_interface.h"
#include "cc/lookup_server/interface/streamed_match_data_provider_interface.h"
#include "protos/lookup_server/backend/location.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Provides data from object storage for use in matching.
 *
 * This concurrently fetches multiple files at a time, parsing data on a
 * per-file basis, and returns match data rows through a streaming context.
 */
class BlobStorageMatchDataProvider : public StreamedMatchDataProviderInterface {
 public:
  /** @brief Constructs a blob storage provider for match data.
   *
   * @param blob_storage_client the CPIO blob storage client
   * @param blob_storage_data_provider the generic blob storage data provider
   * @param max_concurrent_file_reads the max number of files to read at a time
   */
  explicit BlobStorageMatchDataProvider(
      std::shared_ptr<scp::cpio::BlobStorageClientInterface>
          blob_storage_client,
      std::shared_ptr<DataProviderInterface> blob_storage_data_provider,
      uint64_t max_concurrent_file_reads);

  scp::core::ExecutionResult Init() noexcept override;

  scp::core::ExecutionResult Run() noexcept override;

  scp::core::ExecutionResult Stop() noexcept override;

  /**
   * @brief Retrieves match data from the provided location.
   *
   * Accepts either a single file path or a directory.
   *
   * @param get_match_data_context the context supplying the match data
   * location and a callback to process the match data
   * @param data_encryption_key the key used to decrypt the encrypted match data
   * @return an ExecutionResult indicating whether the stream operation
   * was successfully scheduled
   */
  scp::core::ExecutionResult GetMatchData(
      scp::core::ConsumerStreamingContext<
          lookup_server::proto_backend::Location, MatchDataBatch>
          get_match_data_context,
      std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept
      override;

 private:
  /**
   * @brief Wrapper object for a ConsumerStreamingContext to which match data
   * is written, containing all state required for safe concurrent operation.
   */
  class MatchDataContext;

  /**
   * @brief Housekeeping object to manage and track state for a match data
   * fetch operation across multiple concurrent streams.
   */
  class GetMatchDataStreamTracker;

  /**
   * @brief Handles the async callback from listing all fragment files.
   *
   * @param list_fragment_files_context the context containing the list of files
   * or an error
   * @param get_match_data_context the parent context from the original request
   * to GetMatchData
   * @param data_encryption_key the key used to decrypt the encrypted match data
   */
  void HandleListFragmentFilesCallback(
      scp::core::AsyncContext<
          lookup_server::proto_backend::Location,
          std::vector<lookup_server::proto_backend::Location>>&
          list_fragment_files_context,
      MatchDataContext get_match_data_context,
      std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept;

  /**
   * @brief Starts an asynchronous operation to fetch and parse match data
   * for all files listed within the stream tracker.
   *
   * A single invocation processes one file at a time until all files have
   * been processed.
   *
   * This method can be invoked multiple times on the same stream tracker to
   * run multiple file fetch operations in parallel. In other words, up to N
   * files fetched and parsed concurrently until all files have been read with
   * N invocations.
   *
   * @param stream_tracker the helper object containing the fragment files to
   * fetch and the context that will be outputted to
   * @param data_encryption_key the key used to decrypt the encrypted match data
   * @return an ExecutionResult indicating whether the stream operation
   * was successfully scheduled
   */
  scp::core::ExecutionResult StartAsyncFileReader(
      std::shared_ptr<GetMatchDataStreamTracker> stream_tracker,
      std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept;

  /**
   * @brief Callback handler to processes results from a single file fetch.
   *
   * @param file_fetch_context the context containing the data for a single file
   * @param stream_tracker the helper object containing the fragment files to
   * fetch and the context that will be outputted to
   * @param data_encryption_key the key used to decrypt the encrypted match data
   */
  void HandleSingleFileCallback(
      scp::core::AsyncContext<lookup_server::proto_backend::Location,
                              std::string>
          file_fetch_context,
      std::shared_ptr<GetMatchDataStreamTracker> stream_tracker,
      std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept;

  /**
   * @brief Lists all fragment files within a directory path in blob storage.
   *
   * @param location the location containing the bucket name and directory
   * @return an ExecutionResult a list of items or a failure result on error
   */
  scp::core::ExecutionResultOr<
      std::vector<lookup_server::proto_backend::Location>>
  ListFragmentFiles(const proto_backend::Location& location) noexcept;

  /**
   * @brief Lists all fragment files within a directory path in blob storage
   * asynchronously.
   *
   * @param context the AsyncContext containing the request location, which
   * will be populated with a response vector
   * @return an ExecutionResult indicating if the operation started successfully
   */
  scp::core::ExecutionResult ListFragmentFiles(
      scp::core::AsyncContext<
          lookup_server::proto_backend::Location,
          std::vector<lookup_server::proto_backend::Location>>&
          context) noexcept;

  /**
   * @brief Implementation for ListFragmentFiles.
   *
   * @param context the AsyncContext containing the request location, which
   * will be populated with a response vector
   * @param page_token the token indicating the page to start listing files from
   * @return an ExecutionResult indicating if the operation started successfully
   */
  scp::core::ExecutionResult ListFragmentFilesImpl(
      scp::core::AsyncContext<
          lookup_server::proto_backend::Location,
          std::vector<lookup_server::proto_backend::Location>>& context,
      absl::string_view page_token) noexcept;

  /**
   * @brief Callback handler for ListBlobsMetadata.
   */
  void HandleListBlobsMetadataCallback(
      scp::core::AsyncContext<
          lookup_server::proto_backend::Location,
          std::vector<lookup_server::proto_backend::Location>>& parent_context,
      scp::core::AsyncContext<
          cmrt::sdk::blob_storage_service::v1::ListBlobsMetadataRequest,
          cmrt::sdk::blob_storage_service::v1::ListBlobsMetadataResponse>&
          context) noexcept;

  std::shared_ptr<scp::cpio::BlobStorageClientInterface> blob_storage_client_;
  std::shared_ptr<DataProviderInterface> blob_storage_data_provider_;
  // The max number of files to read concurrently per data loading operation.
  uint64_t max_concurrent_file_reads_;
};
}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_BLOB_STORAGE_MATCH_DATA_PROVIDER_H_
