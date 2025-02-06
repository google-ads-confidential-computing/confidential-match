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

#ifndef CC_CORE_DATA_TABLE_SRC_INTERNAL_MARKED_DATA_TABLE_STORAGE_H_
#define CC_CORE_DATA_TABLE_SRC_INTERNAL_MARKED_DATA_TABLE_STORAGE_H_

#include <algorithm>
#include <array>
#include <string>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/public/core/interface/execution_result.h"
#include "oneapi/tbb/concurrent_hash_map.h"

#include "protos/core/data_value.pb.h"
#include "protos/core/data_value_internal.pb.h"

namespace google::confidential_match {

// The required string size of the key (after base-64 decoding).
inline constexpr size_t kMarkedDataTableKeyLength = 32;

/**
 * @brief A concurrent-safe multimap used to store and retrieve match data.
 *
 * This is used to replace previous data tables with new datasets while keeping
 * memory usage under control and allowing lookups to be served concurrently.
 *
 * This is achieved by marking new and existing records in a new data export
 * with a version identifier, and then running a cleanup pass to remove all
 * records not marked with that version identifier.
 */
class MarkedDataTableStorage {
  // Defines hashing and comparison operations for a character array key.
  // This is used by the underlying concurrent hash map.
  struct CharArrayHashCompare {
    static size_t hash(
        const std::array<char, kMarkedDataTableKeyLength>& char_array) {
      absl::string_view hash_key(char_array.data(), char_array.size());
      absl::Hash<absl::string_view> hasher;
      return hasher(hash_key);
    }

    static bool equal(
        const std::array<char, kMarkedDataTableKeyLength>& char_array,
        const std::array<char, kMarkedDataTableKeyLength>& other_char_array) {
      return char_array == other_char_array;
    }
  };

  // The underlying concurrent hash map used to store match data.
  // Uses a fixed size character array as the key to reduce memory usage.
  using MarkedDataTableImpl = oneapi::tbb::concurrent_hash_map<
      std::array<char, kMarkedDataTableKeyLength>, std::vector<std::string>,
      CharArrayHashCompare>;

 public:
  MarkedDataTableStorage() : concurrent_map_() {}

  /** @brief Constructs a table with the provided number of hash buckets. */
  explicit MarkedDataTableStorage(uint64_t bucket_count)
      : concurrent_map_(bucket_count) {}

  /**
   * @brief Inserts a data value into the data table.
   *
   * If an entry with the same data value already exists, the version of
   * that entry is updated to the version of the provided data value and
   * success is returned.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_value the internal data value to insert
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Insert(
      absl::string_view key, const DataValueInternal& data_value) noexcept;

  /**
   * @brief Replaces all data values for a key with the provided values, or
   * creates a new entry if the key doesn't currently exist.
   *
   * @param key the key associated with the data value (eg. SHA-256 hash)
   * @param data_values the internal data values to replace with
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Replace(
      absl::string_view key,
      absl::Span<const DataValueInternal> data_values) noexcept;

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
                                  std::vector<DataValue>& out) const noexcept;

  /**
   * @brief Finds all internal data values for the provided key.
   *
   * Returns a failure result if the key does not exist in the table.
   *
   * @param key the key associated with the data value(s) (eg. SHA-256 hash)
   * @param out a vector appended with the internal data values for the key
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Find(
      absl::string_view key,
      std::vector<DataValueInternal>& out) const noexcept;

  /**
   * @brief Erases all data values for the key whose value does not
   * match the provided version.
   *
   * Returns a failure result if the key does not exist in the table.
   *
   * @param key the key associated with the data value(s) (eg. SHA-256 hash)
   * @param version_id_to_keep the version id for data records that will not be
   * erased
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult EraseUnmatchedVersions(
      absl::string_view key, uint32_t version_id_to_keep) noexcept;

  /**
   * @brief Erases all data values for the key.
   *
   * Returns a failure result if the key does not exist in the table.
   *
   * @param key the key associated with the data value(s) (eg. SHA-256 hash)
   * @return ExecutionResult the result of the operation
   */
  scp::core::ExecutionResult Erase(absl::string_view key) noexcept;

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
  scp::core::ExecutionResult Erase(
      absl::string_view key, const DataValue& data_value_to_erase) noexcept;

  /**
   * @brief Provides an iterator to the first element in the underlying
   * hash map.
   *
   * Iteration is not concurrent-safe, and is only guaranteed to be safe if
   * iterating when no updates are performed.
   */
  MarkedDataTableImpl::iterator Begin() noexcept;

  /**
   * @brief Provides an iterator to the element past the end of the underlying
   * hash map.
   *
   * Iteration is not concurrent-safe, and is only guaranteed to be safe if
   * iterating when no updates are performed.
   */
  MarkedDataTableImpl::iterator End() noexcept;

 private:
  // Table used to store match data.
  MarkedDataTableImpl concurrent_map_;
};

}  // namespace google::confidential_match

#endif  // CC_CORE_DATA_TABLE_SRC_INTERNAL_MARKED_DATA_TABLE_STORAGE_H_
