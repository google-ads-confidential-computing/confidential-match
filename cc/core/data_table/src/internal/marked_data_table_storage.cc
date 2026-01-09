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

#include "cc/core/data_table/src/internal/marked_data_table_storage.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "cc/core/data_table/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"
#include "google/protobuf/util/message_differencer.h"

#include "protos/core/data_value.pb.h"
#include "protos/core/data_value_internal.pb.h"

namespace google::confidential_match {
namespace {

using ::google::confidential_match::DataValue;
using ::google::confidential_match::DataValueInternal;
using ::google::protobuf::util::MessageDifferencer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

// Used to resize the vector when its capacity exceeds the number of held
// elements by the scaling factor.
constexpr int32_t kVectorDownscalingFactor = 2;

// Searches a vector of internal data values for a matching data value, and
// updates the matched entry with the provided version ID if a match is
// found.
// Returns true if a matched record was found, false otherwise.
bool UpdateVersionIdForMatchedRecord(
    std::vector<std::string>& internal_data_values,
    const DataValue& data_value_to_match, uint32_t new_version_id) {
  for (size_t i = 0; i < internal_data_values.size(); ++i) {
    DataValueInternal data_value_internal;
    data_value_internal.ParseFromString(internal_data_values[i]);

    if (!MessageDifferencer::Equals(data_value_internal.data_value(),
                                    data_value_to_match)) {
      continue;
    }

    // Found a matched record (there is at most one such record). Update
    // only if needed
    if (data_value_internal.version_id() == new_version_id) {
      return true;
    } else {
      data_value_internal.set_version_id(new_version_id);
      internal_data_values[i] = data_value_internal.SerializeAsString();
      return true;
    }
  }

  return false;
}

}  // namespace

ExecutionResult MarkedDataTableStorage::Insert(
    absl::string_view key,
    const DataValueInternal& data_value_internal) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE);
  }

  MarkedDataTableImpl::accessor write_accessor;

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  bool is_new = concurrent_map_.insert(write_accessor, key_array);

  if (is_new) {
    write_accessor->second.push_back(data_value_internal.SerializeAsString());
    write_accessor.release();
    return SuccessExecutionResult();
  }

  // If an matching data value is present in the table, update the version ID
  bool found_existing_data_value = UpdateVersionIdForMatchedRecord(
      write_accessor->second, data_value_internal.data_value(),
      data_value_internal.version_id());
  if (found_existing_data_value) {
    write_accessor.release();
    return SuccessExecutionResult();
  }

  // Data entry wasn't found, add it
  write_accessor->second.push_back(data_value_internal.SerializeAsString());
  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Replace(
    absl::string_view key,
    absl::Span<const DataValueInternal> data_values) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE);
  }

  std::vector<std::string> new_data_values;
  new_data_values.reserve(data_values.size());
  for (const auto& data_value_internal : data_values) {
    new_data_values.push_back(data_value_internal.SerializeAsString());
  }

  MarkedDataTableImpl::accessor write_accessor;

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  concurrent_map_.insert(write_accessor, key_array);

  write_accessor->second = std::move(new_data_values);
  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Find(
    absl::string_view key, std::vector<DataValue>& out) const noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  MarkedDataTableImpl::const_accessor read_accessor;

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  if (!concurrent_map_.find(read_accessor, key_array)) {
    read_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  out.reserve(read_accessor->second.size());
  for (const std::string& data_value_str : read_accessor->second) {
    DataValueInternal data_value_internal;
    data_value_internal.ParseFromString(data_value_str);
    out.push_back(data_value_internal.data_value());
  }

  read_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Find(
    absl::string_view key, std::vector<DataValueInternal>& out) const noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  MarkedDataTableImpl::const_accessor read_accessor;

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  if (!concurrent_map_.find(read_accessor, key_array)) {
    read_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  out.reserve(read_accessor->second.size());
  for (const std::string& data_value_str : read_accessor->second) {
    DataValueInternal data_value_internal;
    data_value_internal.ParseFromString(data_value_str);
    out.push_back(std::move(data_value_internal));
  }

  read_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::EraseUnmatchedVersions(
    absl::string_view key, uint32_t version_id_to_keep) noexcept {
  DataValue data_value_to_erase;
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  MarkedDataTableImpl::accessor write_accessor;

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  if (!concurrent_map_.find(write_accessor, key_array)) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  std::vector<std::string> filtered_data_values;
  filtered_data_values.reserve(write_accessor->second.size());

  std::vector<std::string>::iterator iter = write_accessor->second.begin();
  while (iter != write_accessor->second.end()) {
    DataValueInternal data_value_internal;
    data_value_internal.ParseFromString(*iter);
    if (data_value_internal.version_id() == version_id_to_keep) {
      filtered_data_values.push_back(*iter);
    }

    ++iter;
  }

  if (filtered_data_values.size() != 0) {
    filtered_data_values.shrink_to_fit();
    write_accessor->second = std::move(filtered_data_values);
  } else {
    concurrent_map_.erase(write_accessor);
  }

  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Erase(absl::string_view key) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  if (!concurrent_map_.erase(key_array)) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Erase(
    absl::string_view key, const DataValue& data_value_to_erase) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  MarkedDataTableImpl::accessor write_accessor;

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());
  if (!concurrent_map_.find(write_accessor, key_array)) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  bool found = false;
  std::vector<std::string>::iterator iter = write_accessor->second.begin();
  while (iter != write_accessor->second.end()) {
    DataValueInternal data_value;
    data_value.ParseFromString(*iter);
    if (MessageDifferencer::Equals(data_value.data_value(),
                                   data_value_to_erase)) {
      found = true;
      write_accessor->second.erase(iter);
      // Since data values are unique (at most one exists), terminate once found
      break;
    }
    ++iter;
  }

  if (!found) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  if (write_accessor->second.size() == 0) {
    // Remove the entry from the map if no elements remain
    concurrent_map_.erase(write_accessor);
  } else if (write_accessor->second.size() * kVectorDownscalingFactor <
             write_accessor->second.capacity()) {
    // Periodically resize the vector downwards as needed since we are running
    // within a memory-constrained environment
    write_accessor->second.shrink_to_fit();
  }

  write_accessor.release();
  return SuccessExecutionResult();
}

MarkedDataTableStorage::MarkedDataTableImpl::iterator
MarkedDataTableStorage::Begin() noexcept {
  return concurrent_map_.begin();
}

MarkedDataTableStorage::MarkedDataTableImpl::iterator
MarkedDataTableStorage::End() noexcept {
  return concurrent_map_.end();
}

}  // namespace google::confidential_match
