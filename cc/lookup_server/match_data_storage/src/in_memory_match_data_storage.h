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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_SRC_IN_MEMORY_MATCH_DATA_STORAGE_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_SRC_IN_MEMORY_MATCH_DATA_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/core/interface/type_def.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/core/data_table/src/marked_data_table.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "cc/lookup_server/scheme_validator/src/scheme_validator.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Service responsible for storing match data and providing lookup
 * capabilities.
 */
class InMemoryMatchDataStorage : public MatchDataStorageInterface {
 public:
  InMemoryMatchDataStorage() : data_table_() {}

  /**
   * @brief Constructs storage using the provided number of hash buckets.
   */
  explicit InMemoryMatchDataStorage(uint64_t bucket_count)
      : data_table_(bucket_count) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  /**
   * @brief Retrieves a data row from the service.
   *
   * @param key the lookup key to match against
   * @return A MatchDataRow proto containing the match information or
   * a failure result on error.
   */
  scp::core::ExecutionResultOr<std::vector<proto_backend::MatchDataRow>> Get(
      absl::string_view key) noexcept override;

  /**
   * @brief Starts a data update, allowing loading of a new dataset.
   *
   * This allows the mutators (insert, erase) to be called.
   *
   * Once all data items have been added, the caller is expected invoke
   * `FinalizeUpdate()`.
   *
   * If another operation is currently in progress, a failure result with code
   * `MATCH_DATA_STORAGE_UPDATE_ALREADY_IN_PROGRESS` is returned.
   *
   * @param data_export_info metadata about the dataset that will be loaded
   * @return ExecutionResult the result of the start operation
   */
  scp::core::ExecutionResult StartUpdate(
      const proto_backend::DataExportInfo& data_export_info) noexcept override;

  /**
   * @brief Finalizes the dataset loading operation.
   *
   * This is called once all data from a new dataset has been stored.
   *
   * @return ExecutionResult the result of the finalize operation
   */
  scp::core::ExecutionResult FinalizeUpdate() noexcept override;

  /**
   * @brief Cancels the dataset loading operation.
   *
   * This is called after a data loading operation has failed.
   *
   * This leaves the dataset unchanged and unblocks a subsequent data loading
   * operation to retry the loading procedure.
   *
   * @return ExecutionResult the result of the cancellation
   */
  scp::core::ExecutionResult CancelUpdate() noexcept override;

  /**
   * @brief Inserts a match data row into storage.
   *
   * StartUpdate() must be called before this method can be used.
   */
  scp::core::ExecutionResult Insert(
      const proto_backend::MatchDataRow& row) noexcept override;

  /**
   * @brief Replaces all entries in storage for the given key with a new list
   * of rows matching the same key.
   *
   * StartUpdate() must be called before this method can be used.
   */
  scp::core::ExecutionResult Replace(
      absl::string_view key,
      absl::Span<const proto_backend::MatchDataRow> rows) noexcept override;

  /**
   * @brief Removes a match data row into storage.
   *
   * StartUpdate() must be called before this method can be used.
   */
  scp::core::ExecutionResult Remove(
      const proto_backend::MatchDataRow& row) noexcept override;

  /**
   * @brief Checks if an incoming request was sharded at a valid sharding
   * scheme that can be handled by this server's dataset.
   *
   * @param sharding_scheme the scheme used to shard the incoming request
   * @return whether the scheme is valid for requests to this server
   */
  bool IsValidRequestScheme(
      const proto_backend::ShardingScheme& sharding_scheme) noexcept override;

  /**
   * Gets the identifier for the dataset that's currently stored in the table.
   * @return the identifier for the primary dataset
   */
  std::string GetDatasetId() noexcept override;

  /**
   * Gets the identifier for the dataset that's currently being loaded into the
   * table (if any), or empty string otherwise.
   * @return the identifier for the pending dataset
   */
  std::string GetPendingDatasetId() noexcept override;

  /**
   * @brief Provides the status of the storage service.
   */
  lookup_server::proto_backend::ServiceStatus GetStatus() noexcept override;

 protected:
  MarkedDataTable data_table_;
  SchemeValidator scheme_validator_;
};
}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_SRC_IN_MEMORY_MATCH_DATA_STORAGE_H_
