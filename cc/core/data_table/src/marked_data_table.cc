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

#include "cc/core/data_table/src/marked_data_table.h"

#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/data_table/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"
#include "google/protobuf/util/message_differencer.h"

#include "protos/core/data_value.pb.h"
#include "protos/core/data_value_internal.pb.h"

namespace google::confidential_match {
namespace {

using ::google::confidential_match::DataValue;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "MarkedDataTable";
// Controls how often to log when removing outdated versions from table.
constexpr uint32_t kLogTableCleaningProgressEveryN = 10000000;

// Helper to construct an internal data value.
DataValueInternal BuildDataValueInternal(const DataValue& data_value,
                                         uint32_t version_id) {
  DataValueInternal data_value_internal;
  data_value_internal.mutable_data_value()->CopyFrom(data_value);
  data_value_internal.set_version_id(version_id);
  return data_value_internal;
}

// Checks if a vector of serialized internal data values all contain the same
// version identifier as the one provided.
bool CheckAllDataMatchesVersion(
    const std::vector<std::string>& serialized_data_value_internals,
    uint32_t target_version_id) {
  for (const auto& serialized : serialized_data_value_internals) {
    DataValueInternal data_value_internal;
    data_value_internal.ParseFromString(serialized);
    if (data_value_internal.version_id() != target_version_id) {
      return false;
    }
  }
  return true;
}

}  // namespace

MarkedDataTable::MarkedDataTable() : data_table_() {
  version_id_to_dataset_id_.insert(
      boost::bimap<uint32_t, std::string>::value_type(0, ""));
}

MarkedDataTable::MarkedDataTable(uint64_t bucket_count)
    : data_table_(bucket_count) {
  version_id_to_dataset_id_.insert(
      boost::bimap<uint32_t, std::string>::value_type(0, ""));
}

ExecutionResult MarkedDataTable::Find(absl::string_view key,
                                      std::vector<DataValue>& out) noexcept {
  return data_table_.Find(key, out);
}

ExecutionResult MarkedDataTable::StartUpdate(
    absl::string_view dataset_id) noexcept {
  UpdateState expected_previous_state = UpdateState::kNoUpdateInProgress;
  if (!update_state_.compare_exchange_strong(expected_previous_state,
                                             UpdateState::kStartingUpdate)) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_UPDATE_ALREADY_IN_PROGRESS);
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to start update for dataset '%s', another "
                        "update is currently in progress (id: '%s').",
                        dataset_id, GetPendingDatasetId()));
    return result;
  }

  // Associate the dataset id to a new version ID if one does not yet exist
  GetOrCreateVersionId(dataset_id);

  {  // Write-locked scope
    absl::WriterMutexLock dataset_id_lock(&dataset_id_mutex_);

    if (pending_dataset_id_.empty()) {
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat("Starting new table update for dataset '%s'.",
                               dataset_id));
    } else if (pending_dataset_id_ == dataset_id) {
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat(
                   "Resuming previously started update for dataset '%s'.",
                   dataset_id));
    } else {
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat("Overwriting failed update for dataset '%s' "
                               "with new update for dataset '%s'.",
                               pending_dataset_id_, dataset_id));
    }

    pending_dataset_id_ = dataset_id;
  }

  expected_previous_state = UpdateState::kStartingUpdate;
  CHECK(update_state_.compare_exchange_strong(expected_previous_state,
                                              UpdateState::kUpdatingData))
      << absl::StrFormat(
             "Unexpected previous update state during StartUpdate: %d",
             static_cast<std::underlying_type<UpdateState>::type>(
                 update_state_.load()));
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTable::FinalizeUpdate() noexcept {
  UpdateState expected_previous_state = UpdateState::kUpdatingData;
  if (!update_state_.compare_exchange_strong(expected_previous_state,
                                             UpdateState::kFinalizingUpdate)) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat(
                  "Failed to finalize update, no update in progress or not "
                  "in correct state. (Current state: '%d')",
                  static_cast<std::underlying_type<UpdateState>::type>(
                      update_state_.load())));
    return result;
  }
  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Finalizing table update for dataset '%s'...",
                           GetPendingDatasetId()));

  uint64_t num_keys_removed = 0;
  if (GetDatasetId().empty() && version_id_to_dataset_id_.size() == 2) {
    // Optimization: Skip the table cleanup pass if (1) this is the startup
    // data loading operation, and (2) the table does not contain data from a
    // previously failed loading attempt from a different dataset.
    SCP_INFO(kComponentName, kZeroUuid,
             "Skipping table cleanup operation for initial data load.");
  } else {
    num_keys_removed = RemoveUnmatchedDataEntries(GetPendingDatasetId());
  }

  {  // Write-locked scope
    absl::WriterMutexLock dataset_id_lock(&dataset_id_mutex_);
    dataset_id_ = pending_dataset_id_;
    pending_dataset_id_ = "";
    ClearAllVersionDatasetIdMappingsExcept(dataset_id_);
  }

  expected_previous_state = UpdateState::kFinalizingUpdate;
  CHECK(update_state_.compare_exchange_strong(expected_previous_state,
                                              UpdateState::kNoUpdateInProgress))
      << absl::StrFormat(
             "Unexpected previous update state during FinalizeUpdate: %d",
             static_cast<std::underlying_type<UpdateState>::type>(
                 update_state_.load()));

  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Table update finalized. Keys cleaned: %d",
                           num_keys_removed));
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTable::CancelUpdate() noexcept {
  UpdateState expected_previous_state = UpdateState::kUpdatingData;
  if (!update_state_.compare_exchange_strong(
          expected_previous_state, UpdateState::kNoUpdateInProgress)) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS);
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to cancel update, no update in progress or not "
                        "in correct state. (Current state: '%d')",
                        static_cast<std::underlying_type<UpdateState>::type>(
                            update_state_.load())));
    return result;
  }

  SCP_INFO(
      kComponentName, kZeroUuid,
      absl::StrFormat("Canceled data table update process for dataset '%s'.",
                      GetPendingDatasetId()));
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTable::Insert(absl::string_view key,
                                        const DataValue& data_value) noexcept {
  if (update_state_.load() != UpdateState::kUpdatingData) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS);
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to insert data, update not in progress or not "
                        "in correct state. (Current state: '%d')",
                        static_cast<std::underlying_type<UpdateState>::type>(
                            update_state_.load())));
    return result;
  }

  uint32_t version_id = version_id_to_dataset_id_.right.at(pending_dataset_id_);
  return data_table_.Insert(key,
                            BuildDataValueInternal(data_value, version_id));
}

ExecutionResult MarkedDataTable::Replace(
    absl::string_view key, absl::Span<const DataValue> data_values) noexcept {
  if (update_state_.load() != UpdateState::kUpdatingData) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS);
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to insert data, update not in progress or not "
                        "in correct state. (Current state: '%d')",
                        static_cast<std::underlying_type<UpdateState>::type>(
                            update_state_.load())));
    return result;
  }

  uint32_t version_id = version_id_to_dataset_id_.right.at(pending_dataset_id_);

  std::vector<DataValueInternal> internal_data_values;
  internal_data_values.reserve(data_values.size());
  for (const auto& data_value : data_values) {
    internal_data_values.push_back(
        BuildDataValueInternal(data_value, version_id));
  }
  return data_table_.Replace(key, internal_data_values);
}

ExecutionResult MarkedDataTable::Erase(absl::string_view key) noexcept {
  if (update_state_.load() != UpdateState::kUpdatingData) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS);
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to erase data, update not in progress or not "
                        "in correct state. (Current state: '%d')",
                        static_cast<std::underlying_type<UpdateState>::type>(
                            update_state_.load())));
    return result;
  }

  return data_table_.Erase(key);
}

ExecutionResult MarkedDataTable::Erase(
    absl::string_view key, const DataValue& data_value_to_erase) noexcept {
  if (update_state_.load() != UpdateState::kUpdatingData) {
    ExecutionResult result =
        FailureExecutionResult(VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS);
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Failed to erase data, update not in progress or not "
                        "in correct state. (Current state: '%d')",
                        static_cast<std::underlying_type<UpdateState>::type>(
                            update_state_.load())));
    return result;
  }

  return data_table_.Erase(key, data_value_to_erase);
}

std::string MarkedDataTable::GetDatasetId() noexcept {
  absl::ReaderMutexLock dataset_id_lock(&dataset_id_mutex_);
  return dataset_id_;
}

std::string MarkedDataTable::GetPendingDatasetId() noexcept {
  absl::ReaderMutexLock dataset_id_lock(&dataset_id_mutex_);
  return pending_dataset_id_;
}

bool MarkedDataTable::IsUpdateInProgress() const noexcept {
  return update_state_ != UpdateState::kNoUpdateInProgress;
}

uint64_t MarkedDataTable::RemoveUnmatchedDataEntries(
    absl::string_view dataset_id_to_keep) noexcept {
  CHECK(update_state_.load() == UpdateState::kFinalizingUpdate)
      << "Outdated entries should only be removed when finalizing an update.";

  uint32_t version_id_to_keep =
      version_id_to_dataset_id_.right.at(std::string(dataset_id_to_keep));
  std::vector<std::array<char, kMarkedDataTableKeyLength>> keys_to_remove;

  // Loop and collect keys with old data in a separate pass from the actual
  // data removal pass. Required because making updates while iterating is not
  // concurrent-safe
  uint64_t num_keys_searched = 0;
  for (auto it = data_table_.Begin(); it != data_table_.End(); ++it) {
    if (!CheckAllDataMatchesVersion(it->second, version_id_to_keep)) {
      keys_to_remove.push_back(it->first);
    }
    ++num_keys_searched;

    if (num_keys_searched % kLogTableCleaningProgressEveryN == 0) {
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat("Searching for outdated entries... "
                               "(checked: %d, outdated: %d)",
                               num_keys_searched, keys_to_remove.size()));
    }
  }
  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Finished searching %d keys, found %d outdated. "
                           "Starting cleanup operation.",
                           num_keys_searched, keys_to_remove.size()));

  for (size_t i = 0; i < keys_to_remove.size(); ++i) {
    if (i % kLogTableCleaningProgressEveryN == 0) {
      SCP_INFO(kComponentName, kZeroUuid,
               absl::StrFormat("Cleaning table key %d of %d...", i + 1,
                               keys_to_remove.size()));
    }
    std::string key(std::begin(keys_to_remove[i]), std::end(keys_to_remove[i]));
    data_table_.EraseUnmatchedVersions(key, version_id_to_keep);
  }
  SCP_INFO(kComponentName, kZeroUuid, "Finished cleaning outdated records.");

  return keys_to_remove.size();
}

uint32_t MarkedDataTable::GetOrCreateVersionId(
    absl::string_view dataset_id) noexcept {
  CHECK(update_state_.load() == UpdateState::kStartingUpdate)
      << "Get/create version ID should only be called when starting an update.";

  if (auto it = version_id_to_dataset_id_.right.find(std::string(dataset_id));
      it != version_id_to_dataset_id_.right.end()) {
    return it->second;
  }

  // Find the next available version ID. This loops at most once.
  // We loop when the version matches that of the currently stored dataset ID.
  uint32_t version_id = next_potential_version_id_;
  while (version_id_to_dataset_id_.left.count(version_id)) {
    ++version_id;
  }

  version_id_to_dataset_id_.insert(
      boost::bimap<uint32_t, std::string>::value_type(version_id,
                                                      std::string(dataset_id)));
  next_potential_version_id_ = version_id + 1;
  return version_id;
}

void MarkedDataTable::ClearAllVersionDatasetIdMappingsExcept(
    absl::string_view dataset_id_to_keep) noexcept {
  CHECK(update_state_.load() == UpdateState::kFinalizingUpdate)
      << "Clearing the version to dataset ID mapping should only be called "
         "when finalizing an update.";

  for (uint32_t i = 0; i <= next_potential_version_id_; ++i) {
    auto it = version_id_to_dataset_id_.left.find(i);
    if (it == version_id_to_dataset_id_.left.end()) {
      continue;
    }
    if (it->second != dataset_id_to_keep) {
      version_id_to_dataset_id_.left.erase(i);
    }
  }
  next_potential_version_id_ = 0;
}

}  // namespace google::confidential_match
