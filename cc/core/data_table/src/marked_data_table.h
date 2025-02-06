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

#ifndef CC_CORE_DATA_TABLE_SRC_MARKED_DATA_TABLE_H_
#define CC_CORE_DATA_TABLE_SRC_MARKED_DATA_TABLE_H_

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <boost/bimap.hpp>

#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/core/data_table/src/internal/marked_data_table_storage.h"
#include "protos/core/data_value.pb.h"
#include "protos/core/data_value_internal.pb.h"

namespace google::confidential_match {

/**
 * @brief A concurrent-safe multimap used to store and retrieve match data.
 *
 * This is used to replace previous data tables with new datasets while keeping
 * memory usage under control and allowing lookups to be served concurrently.
 *
 * It achieves this by marking new and existing records in a new data export
 * with a version identifier, and then running a cleanup pass to remove all
 * records not marked with that version identifier.
 *
 * The data table update process involves the following:
 *  1. Calling StartUpdate(), then
 *  2. Adding (or removing) elements to the table by calling mutators
 *  3. Calling FinalizeUpdate()/CancelUpdate() at the end of the process.
 *
 * At most one data update process is allowed to run at a time, though the
 * calls to Insert(), Replace(), and Erase() can be done concurrently.
 */
class MarkedDataTable {
 public:
  MarkedDataTable();

  /** @brief Constructs a table with the provided number of hash buckets. */
  explicit MarkedDataTable(uint64_t bucket_count);

  /**
   * @brief Finds all data values for the provided key.
   *
   * Returns a failure result if the key does not exist in the table.
   *
   * @param key the key associated with the data value(s) (eg. SHA-256 hash)
   * @param out a vector to be appended with the data values for the key
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Find(absl::string_view key,
                                  std::vector<DataValue>& out) noexcept;

  /**
   * @brief Starts a new version of data, allowing loading of the next dataset.
   *
   * This allows the mutators (insert, replace, erase) to be called.
   *
   * Once all data items have been added, the caller is expected invoke
   * `FinalizeUpdate()` to drop the previous dataset, or `CancelUpdate()` to
   * retry a new data loading attempt if an error occurred.
   *
   * @param dataset_id the identifier for the dataset that will be loaded
   * @return ExecutionResult the result of the start operation
   */
  scp::core::ExecutionResult StartUpdate(absl::string_view dataset_id) noexcept;

  /**
   * @brief Finalizes the dataset loading operation.
   *
   * This drops the previous dataset, replacing the stored items to contain
   * only items provided in the new dataset.
   *
   * This is invoked only after all calls to mutators have finished.
   *
   * @return ExecutionResult the result of the finalize operation
   */
  scp::core::ExecutionResult FinalizeUpdate() noexcept;

  /**
   * @brief Cancels the dataset loading operation.
   *
   * This leaves the dataset unchanged and unblocks a subsequent data loading
   * operation to retry the loading procedure if the original loading operation
   * failed.
   *
   * This is invoked only after all calls to mutators have finished.
   *
   * @return ExecutionResult the result of the cancellation
   */
  scp::core::ExecutionResult CancelUpdate() noexcept;

  /**
   * @brief Inserts a data value into the data table.
   *
   * The caller must call `StartUpdate()` before inserting elements.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_value the data value to insert
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Insert(absl::string_view key,
                                    const DataValue& data_value) noexcept;

  /**
   * @brief Replaces all data values for a key with the provided values, or
   * creates a new entry if the key doesn't currently exist.
   *
   * The caller must call `StartUpdate()` before inserting elements.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_values the data values to replace with
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Replace(
      absl::string_view key, absl::Span<const DataValue> data_values) noexcept;

  /**
   * @brief Erases all data values for the key.
   *
   * Returns a failure result if the key does not exist in the table.
   *
   * The caller must call `StartUpdate()` before erasing elements.
   *
   * @param key the key associated with the data value(s) (eg. SHA-256 hash)
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Erase(absl::string_view key) noexcept;

  /**
   * @brief Erases a specific data value matching any version under the
   * provided key.
   *
   * Returns a failure result if the data value does not exist in the table
   * under the provided key.
   *
   * The caller must call `StartUpdate()` before erasing elements.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_value_to_erase the data value to erase
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Erase(
      absl::string_view key, const DataValue& data_value_to_erase) noexcept;

  /**
   * Gets the identifier for the dataset that's currently stored in the table.
   * @return the identifier for the primary dataset
   */
  std::string GetDatasetId() noexcept;

  /**
   * Gets the identifier for the dataset that's currently being loaded into the
   * table (if any), or empty string otherwise.
   *
   * If the last update was canceled, the dataset from that update is returned.
   *
   * @return the identifier for the pending dataset
   */
  std::string GetPendingDatasetId() noexcept;

  /**
   * @brief Returns true if an update is currently in progress, false otherwise.
   */
  bool IsUpdateInProgress() const noexcept;

 private:
  /**
   * @brief Represents the current state of data update operations in the table.
   */
  enum class UpdateState {
    kNoUpdateInProgress,
    kStartingUpdate,
    kUpdatingData,
    kFinalizingUpdate,
  };

  /**
   * @brief Helper to remove all table entries whose version ID doesn't match
   * that of the provided dataset.
   *
   * @param dataset_id_to_keep the dataset ID for the data that won't be deleted
   *
   * @return a count of the number of keys processed
   */
  uint64_t RemoveUnmatchedDataEntries(
      absl::string_view dataset_id_to_keep) noexcept;

  /**
   * @brief Gets the existing version ID for a particular dataset ID if one
   * exists, otherwise assigns an unused version ID to the dataset ID.
   *
   * @return the version ID associated with the dataset ID
   */
  uint32_t GetOrCreateVersionId(absl::string_view dataset_id) noexcept;

  /**
   * @brief Clears all tracked version ID to dataset ID mappings except for
   * the provided dataset ID, and resets the next potential version ID counter.
   *
   * This is used after a dataset update is finalized and all other datasets
   * have been dropped.
   *
   * @param dataset_id_to_keep the dataset ID whose version ID mapping will not
   * be removed
   *
   * @return the version ID associated with the dataset ID
   */
  void ClearAllVersionDatasetIdMappingsExcept(
      absl::string_view dataset_id_to_keep) noexcept;

  // Table used to store match data.
  MarkedDataTableStorage data_table_;

  // Tracks the current update state and enforces that at most one data table
  // update process occurs at any time.
  std::atomic<UpdateState> update_state_ = UpdateState::kNoUpdateInProgress;

  // Tracks which version ID corresponds to which dataset.
  // We avoid concurrent writing while reading by performing the write
  // operations in separate phases as tracked by update_state_, so a mutex
  // is not needed.
  boost::bimap<uint32_t, std::string> version_id_to_dataset_id_;
  // Tracks the next potentially available version ID to associate with a
  // dataset ID. Still need to check if the ID is in use before associating it.
  // Using lower numbers reduces space utilization when the proto is serialized.
  uint32_t next_potential_version_id_ = 0;

  // Lock used to protect changes to the dataset identifier values.
  absl::Mutex dataset_id_mutex_;
  // The dataset identifier for the data that is stored in the primary table,
  // or empty string if none is provided.
  std::string dataset_id_ ABSL_GUARDED_BY(dataset_id_mutex_);
  // The dataset identifier for the data that is currently being loaded,
  // or empty string if none is provided.
  std::string pending_dataset_id_ ABSL_GUARDED_BY(dataset_id_mutex_);
};

}  // namespace google::confidential_match

#endif  // CC_CORE_DATA_TABLE_SRC_INTERNAL_MARKED_DATA_TABLE_H_
