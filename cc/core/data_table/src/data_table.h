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

#ifndef CC_CORE_DATA_TABLE_SRC_DATA_TABLE_H_
#define CC_CORE_DATA_TABLE_SRC_DATA_TABLE_H_

#include <algorithm>
#include <array>
#include <string>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "oneapi/tbb/concurrent_hash_map.h"

#include "protos/core/data_value.pb.h"

namespace google::confidential_match {

// The required string size of the key (after base-64 decoding).
inline constexpr size_t kDataTableKeyLength = 32;

/**
 * @brief A concurrent-safe multimap used to store and retrieve match data.
 */
class DataTable {
  // Defines hashing and comparison operations for a character array key.
  // This is used by the underlying concurrent hash map.
  struct CharArrayHashCompare {
    static size_t hash(
        const std::array<char, kDataTableKeyLength>& char_array) {
      absl::string_view hash_key(char_array.data(), char_array.size());
      absl::Hash<absl::string_view> hasher;
      return hasher(hash_key);
    }

    static bool equal(
        const std::array<char, kDataTableKeyLength>& char_array,
        const std::array<char, kDataTableKeyLength>& other_char_array) {
      return char_array == other_char_array;
    }
  };

  // The underlying concurrent hash map used to store match data.
  // Uses a fixed size character array as the key to reduce memory usage.
  using DataTableImpl =
      oneapi::tbb::concurrent_hash_map<std::array<char, kDataTableKeyLength>,
                                       std::vector<std::string>,
                                       CharArrayHashCompare>;

 public:
  DataTable() : concurrent_map_() {}

  /** @brief Constructs a table with the provided number of hash buckets. */
  explicit DataTable(uint64_t bucket_count) : concurrent_map_(bucket_count) {}

  /**
   * @brief Inserts a data value into the data table.
   *
   * Returns a failure result if the element already exists in the table for
   * the given key.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_value the data value to insert
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Insert(absl::string_view key,
                                    const DataValue& data_value);

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
                                  std::vector<DataValue>& out) const;

  /**
   * @brief Erases all data values for the provided key.
   *
   * Returns a failure result if the key does not exist in the table.
   *
   * @param key the key associated with the data value(s) (eg. SHA-256 hash)
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Erase(absl::string_view key);

  /**
   * @brief Erases a specific data value under the provided key.
   *
   * Returns a failure result if the data value does not exist in the table
   * under the provided key.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_value_to_erase the data value to erase
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Erase(absl::string_view key,
                                   const DataValue& data_value_to_erase);

 private:
  // Table used to store match data.
  DataTableImpl concurrent_map_;
};

}  // namespace google::confidential_match

#endif  // CC_CORE_DATA_TABLE_SRC_DATA_TABLE_H_
