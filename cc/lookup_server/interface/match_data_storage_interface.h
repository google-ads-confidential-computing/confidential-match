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

#ifndef CC_LOOKUP_SERVER_INTERFACE_MATCH_DATA_STORAGE_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_MATCH_DATA_STORAGE_INTERFACE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/status_provider_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface for service that stores data for use in matching. */
class MatchDataStorageInterface : public scp::core::ServiceInterface,
                                  public StatusProviderInterface {
 public:
  virtual ~MatchDataStorageInterface() = default;

  /**
   * @brief Retrieves a data row from the service.
   *
   * @param key the lookup key to match against
   * @return A MatchDataRow proto containing the match information or
   * a failure result on error.
   */
  virtual scp::core::ExecutionResultOr<std::vector<proto_backend::MatchDataRow>>
  Get(absl::string_view key) noexcept = 0;

  /**
   * @brief Starts a data update, allowing loading of a new dataset.
   *
   * This allows the mutators (insert, erase) to be called.
   *
   * Once all data items have been added, the caller is expected invoke
   * `FinalizeUpdate()`.
   *
   * @param data_export_info metadata about the dataset that will be loaded
   * @return ExecutionResult the result of the start operation
   */
  virtual scp::core::ExecutionResult StartUpdate(
      const proto_backend::DataExportInfo& data_export_info) noexcept = 0;

  /**
   * @brief Finalizes the dataset loading operation.
   *
   * This is called once all data from a new dataset has been stored.
   *
   * @return ExecutionResult the result of the finalize operation
   */
  virtual scp::core::ExecutionResult FinalizeUpdate() noexcept = 0;

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
  virtual scp::core::ExecutionResult CancelUpdate() noexcept = 0;

  /**
   * @brief Inserts a match data row into storage.
   *
   * StartUpdate() must be called before this method can be used.
   */
  virtual scp::core::ExecutionResult Insert(
      const proto_backend::MatchDataRow& row) noexcept = 0;

  /**
   * @brief Replaces all entries in storage for the given key with a new list
   * of rows matching the same key.
   *
   * StartUpdate() must be called before this method can be used.
   */
  virtual scp::core::ExecutionResult Replace(
      absl::string_view key,
      absl::Span<const proto_backend::MatchDataRow> rows) noexcept = 0;

  /**
   * @brief Removes a match data row into storage.
   *
   * StartUpdate() must be called before this method can be used.
   */
  virtual scp::core::ExecutionResult Remove(
      const proto_backend::MatchDataRow& row) noexcept = 0;

  /**
   * @brief Checks if an incoming request was sharded at a valid sharding
   * scheme that can be handled by this server's dataset.
   *
   * @param sharding_scheme the scheme used to shard the incoming request
   * @return whether the scheme is valid for requests to this server
   */
  virtual bool IsValidRequestScheme(
      const proto_backend::ShardingScheme& sharding_scheme) noexcept = 0;

  /**
   * Gets the identifier for the dataset that's currently stored in the table.
   * @return the identifier for the primary dataset
   */
  virtual std::string GetDatasetId() noexcept = 0;

  /**
   * Gets the identifier for the dataset that's currently being loaded into the
   * table (if any), or empty string otherwise.
   * @return the identifier for the pending dataset
   */
  virtual std::string GetPendingDatasetId() noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_MATCH_DATA_STORAGE_INTERFACE_H_
