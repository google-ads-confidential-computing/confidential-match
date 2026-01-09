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

#include "cc/core/data_table/src/data_table.h"

#include <algorithm>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "cc/core/data_table/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"
#include "google/protobuf/util/message_differencer.h"

#include "protos/core/data_value.pb.h"

namespace google::confidential_match {
namespace {

using ::google::confidential_match::DataValue;
using ::google::protobuf::util::MessageDifferencer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

// Used to resize the vector when its capacity exceeds the number of held
// elements by the scaling factor.
constexpr int32_t kVectorDownscalingFactor = 2;

/** Checks if a vector of data values contains a given data value. */
bool ContainsRecord(const std::vector<std::string>& data_values,
                    const DataValue& data_value_to_check) {
  for (const auto& data_value_str : data_values) {
    DataValue data_value;
    data_value.ParseFromString(data_value_str);
    if (MessageDifferencer::Equals(data_value, data_value_to_check)) {
      return true;
    }
  }
  return false;
}

}  // namespace

ExecutionResult DataTable::Insert(absl::string_view key,
                                  const DataValue& data_value) {
  if (key.size() != kDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE);
  }

  DataTableImpl::accessor write_accessor;

  std::array<char, kDataTableKeyLength> char_array;
  std::copy(key.begin(), key.end(), char_array.data());

  bool is_new = concurrent_map_.insert(write_accessor, char_array);

  if (!is_new && ContainsRecord(write_accessor->second, data_value)) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_ALREADY_EXISTS);
  }

  write_accessor->second.push_back(data_value.SerializeAsString());
  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult DataTable::Find(absl::string_view key,
                                std::vector<DataValue>& out) const {
  if (key.size() != kDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  DataTableImpl::const_accessor read_accessor;

  std::array<char, kDataTableKeyLength> char_array;
  std::copy(key.begin(), key.end(), char_array.data());

  if (!concurrent_map_.find(read_accessor, char_array)) {
    read_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  out.reserve(read_accessor->second.size());
  for (const std::string& data_value_str : read_accessor->second) {
    DataValue data_value;
    data_value.ParseFromString(data_value_str);
    out.push_back(data_value);
  }

  read_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult DataTable::Erase(absl::string_view key) {
  if (key.size() != kDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  std::array<char, kDataTableKeyLength> char_array;
  std::copy(key.begin(), key.end(), char_array.data());

  if (!concurrent_map_.erase(char_array)) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }
  return SuccessExecutionResult();
}

ExecutionResult DataTable::Erase(absl::string_view key,
                                 const DataValue& data_value_to_erase) {
  if (key.size() != kDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  DataTableImpl::accessor write_accessor;

  std::array<char, kDataTableKeyLength> char_array;
  std::copy(key.begin(), key.end(), char_array.data());

  if (!concurrent_map_.find(write_accessor, char_array)) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  bool found = false;
  std::vector<std::string>::iterator iter = write_accessor->second.begin();
  while (iter != write_accessor->second.end()) {
    DataValue data_value;
    data_value.ParseFromString(*iter);
    if (MessageDifferencer::Equals(data_value, data_value_to_erase)) {
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

}  // namespace google::confidential_match
