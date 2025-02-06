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

#include "cc/lookup_server/match_data_storage/src/in_memory_match_data_storage.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/core/data_table/src/error_codes.h"
#include "cc/lookup_server/match_data_storage/src/error_codes.h"
#include "protos/core/data_value.pb.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    DataExportInfo;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::confidential_match::lookup_server::proto_backend::ServiceStatus;
using ::google::confidential_match::lookup_server::proto_backend::
    ShardingScheme;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::ExecutionStatus;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "InMemoryMatchDataStorage";

// Builds a DataValue object from a match data row.
DataValue BuildDataValue(const MatchDataRow& row) {
  DataValue data_value;
  data_value.mutable_associated_data()->CopyFrom(row.associated_data());
  return data_value;
}

}  // namespace

ExecutionResult InMemoryMatchDataStorage::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult InMemoryMatchDataStorage::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult InMemoryMatchDataStorage::Stop() noexcept {
  return SuccessExecutionResult();
}

ExecutionResultOr<std::vector<MatchDataRow>> InMemoryMatchDataStorage::Get(
    absl::string_view key) noexcept {
  std::vector<DataValue> data_values;

  std::string raw_key;
  if (!absl::Base64Unescape(key, &raw_key)) {
    // Valid lookup keys are base-64 encoded, return no match if decoding fails.
    return std::vector<MatchDataRow>();
  }

  ExecutionResult result = data_table_.Find(raw_key, data_values);
  if (result == FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)) {
    return std::vector<MatchDataRow>();
  }
  if (result.status == ExecutionStatus::Retry) {
    return result;
  }
  if (!result.Successful()) {
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Error retrieving key from in-memory storage table: %s",
                        key));
    return FailureExecutionResult(MATCH_DATA_STORAGE_FETCH_ERROR);
  }

  std::vector<MatchDataRow> matches;
  for (DataValue& data_value : data_values) {
    MatchDataRow match;
    *match.mutable_key() = key;
    *match.mutable_associated_data() =
        std::move(*data_value.mutable_associated_data());
    matches.push_back(std::move(match));
  }

  return matches;
}

ExecutionResult InMemoryMatchDataStorage::StartUpdate(
    const DataExportInfo& data_export_info) noexcept {
  ExecutionResult start_update_result =
      data_table_.StartUpdate(data_export_info.data_export_id());
  if (start_update_result.status_code ==
      VERSIONED_DATA_TABLE_UPDATE_ALREADY_IN_PROGRESS) {
    SCP_WARNING(kComponentName, kZeroUuid,
                "Not starting update since one is currently in progress.")
    return FailureExecutionResult(
        MATCH_DATA_STORAGE_UPDATE_ALREADY_IN_PROGRESS);
  } else if (!start_update_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, start_update_result,
              "Failed to start the dataset update.")
    return start_update_result;
  }

  ExecutionResult scheme_validator_result =
      scheme_validator_.SetPendingScheme(data_export_info.sharding_scheme());
  if (!scheme_validator_result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, scheme_validator_result,
                 "Failed to set the pending scheme for the started update.")
    return scheme_validator_result;
  }

  return SuccessExecutionResult();
}

ExecutionResult InMemoryMatchDataStorage::FinalizeUpdate() noexcept {
  ExecutionResult apply_result = scheme_validator_.ApplyPendingScheme();
  if (!apply_result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, apply_result,
                 "Failed to apply the pending scheme to the scheme validator.")
    return apply_result;
  }

  ExecutionResult finalize_result = data_table_.FinalizeUpdate();
  if (!finalize_result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, finalize_result,
                 "Failed to finalize the dataset update.")
    return finalize_result;
  }

  return SuccessExecutionResult();
}

ExecutionResult InMemoryMatchDataStorage::CancelUpdate() noexcept {
  return data_table_.CancelUpdate();
}

ExecutionResult InMemoryMatchDataStorage::Insert(
    const proto_backend::MatchDataRow& row) noexcept {
  DataValue data_value = BuildDataValue(row);

  std::string raw_key;
  if (!absl::Base64Unescape(row.key(), &raw_key)) {
    return FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR);
  }

  ExecutionResult result = data_table_.Insert(raw_key, data_value);
  if (result == FailureExecutionResult(DATA_TABLE_ENTRY_ALREADY_EXISTS)) {
    // Duplicate entries may be added while the table is being loaded, ignore
    return SuccessExecutionResult();
  } else if (result == FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE)) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat("Error inserting row to in-memory storage table "
                              "due to key size. Key: %s",
                              row.key()));
    return FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR);
  } else if (result.status == ExecutionStatus::Retry) {
    return result;
  } else if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat(
                  "Error inserting row to in-memory storage table. Key: %s",
                  row.key()));
    return FailureExecutionResult(MATCH_DATA_STORAGE_INSERT_ERROR);
  }
  return SuccessExecutionResult();
}

ExecutionResult InMemoryMatchDataStorage::Replace(
    absl::string_view key, absl::Span<const MatchDataRow> rows) noexcept {
  std::vector<DataValue> data_values;
  data_values.reserve(rows.size());
  for (const auto& match_data_row : rows) {
    if (match_data_row.key() != key) {
      ExecutionResult result =
          FailureExecutionResult(MATCH_DATA_STORAGE_REPLACE_KEY_MISMATCH);
      SCP_ERROR(kComponentName, kZeroUuid, result,
                absl::StrFormat("Found data row with non-matching key. "
                                "Got '%s' but expected '%s'.",
                                match_data_row.key(), key));
      return result;
    }
    data_values.push_back(BuildDataValue(match_data_row));
  }

  std::string raw_key;
  if (!absl::Base64Unescape(key, &raw_key)) {
    return FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR);
  }
  ExecutionResult result = data_table_.Replace(raw_key, data_values);

  if (result == FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE)) {
    SCP_ERROR(
        kComponentName, kZeroUuid, result,
        absl::StrFormat("Error replacing row within in-memory storage table "
                        "due to key size. Key: %s",
                        key));
    return FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR);
  } else if (result.status == ExecutionStatus::Retry) {
    return result;
  } else if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat(
                  "Error replacing row within in-memory storage table. Key: %s",
                  key));
    return FailureExecutionResult(MATCH_DATA_STORAGE_REPLACE_ERROR);
  }
  return SuccessExecutionResult();
}

ExecutionResult InMemoryMatchDataStorage::Remove(
    const proto_backend::MatchDataRow& row) noexcept {
  DataValue data_value = BuildDataValue(row);

  std::string raw_key;
  if (!absl::Base64Unescape(row.key(), &raw_key)) {
    return FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR);
  }

  ExecutionResult result = data_table_.Erase(raw_key, data_value);
  if (result == FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)) {
    // When reprocessing a diff, we may remove more than once. Safe to ignore
    return SuccessExecutionResult();
  }
  if (result.status == ExecutionStatus::Retry) {
    return result;
  }
  if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat(
                  "Error removing row from in-memory storage table. Key: %s",
                  row.key()));
    return FailureExecutionResult(MATCH_DATA_STORAGE_REMOVE_ERROR);
  }
  return SuccessExecutionResult();
}

bool InMemoryMatchDataStorage::IsValidRequestScheme(
    const ShardingScheme& sharding_scheme) noexcept {
  return scheme_validator_.IsValidRequestScheme(sharding_scheme);
}

std::string InMemoryMatchDataStorage::GetDatasetId() noexcept {
  return data_table_.GetDatasetId();
}

std::string InMemoryMatchDataStorage::GetPendingDatasetId() noexcept {
  return data_table_.GetPendingDatasetId();
}

ServiceStatus InMemoryMatchDataStorage::GetStatus() noexcept {
  // Pending if no full dataset has been loaded yet, ok otherwise
  return GetDatasetId().empty() ? ServiceStatus::PENDING : ServiceStatus::OK;
}

}  // namespace google::confidential_match::lookup_server
