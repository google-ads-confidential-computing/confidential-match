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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/core/data_table/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"
#include "google/protobuf/util/message_differencer.h"
#include "protos/core/data_value.pb.h"
#include "protos/core/data_value_internal.pb.h"

namespace google::confidential_match {
namespace {

using ::google::confidential_match::CompactAssociatedData;
using ::google::confidential_match::DataValue;
using ::google::confidential_match::DataValueInternal;
using ::google::confidential_match::KeyValue;
using ::google::confidential_match::PackedDataValueGroup;
using ::google::confidential_match::PiiType;
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

constexpr absl::string_view kPiiTypeKey = "pii_type";
constexpr absl::string_view kEncryptedGaiaIdKey = "encrypted_gaia_id";
constexpr absl::string_view kPiiTypePhoneNumber = "P";
constexpr absl::string_view kPiiTypeEmail = "E";
constexpr absl::string_view kPiiTypeAddress = "A";

// Translates a generic DataValue into a compact schema-specific proto.
CompactAssociatedData ToCompactAssociatedData(const DataValue& data_value) {
  CompactAssociatedData compact;
  for (const KeyValue& kv : data_value.associated_data()) {
    if (kv.key() == kPiiTypeKey) {
      if (kv.string_value() == kPiiTypePhoneNumber) {
        compact.set_pii_type(PII_TYPE_PHONE_NUMBER);
      } else if (kv.string_value() == kPiiTypeEmail) {
        compact.set_pii_type(PII_TYPE_EMAIL);
      } else if (kv.string_value() == kPiiTypeAddress) {
        compact.set_pii_type(PII_TYPE_ADDRESS);
      } else {
        compact.set_pii_type(PII_TYPE_UNSPECIFIED);
      }
    } else if (kv.key() == kEncryptedGaiaIdKey) {
      compact.add_encrypted_gaia_id(kv.bytes_value());
    }
  }
  return compact;
}

// Reconstructs a generic DataValue from a CompactAssociatedData proto.
DataValue FromCompactAssociatedData(const CompactAssociatedData& compact) {
  DataValue data_value;
  switch (compact.pii_type()) {
    case PII_TYPE_PHONE_NUMBER: {
      KeyValue* kv = data_value.add_associated_data();
      kv->set_key(std::string(kPiiTypeKey));
      kv->set_string_value(std::string(kPiiTypePhoneNumber));
      break;
    }
    case PII_TYPE_EMAIL: {
      KeyValue* kv = data_value.add_associated_data();
      kv->set_key(std::string(kPiiTypeKey));
      kv->set_string_value(std::string(kPiiTypeEmail));
      break;
    }
    case PII_TYPE_ADDRESS: {
      KeyValue* kv = data_value.add_associated_data();
      kv->set_key(std::string(kPiiTypeKey));
      kv->set_string_value(std::string(kPiiTypeAddress));
      break;
    }
    default:
      break;
  }
  for (const std::string& gaia_id : compact.encrypted_gaia_id()) {
    KeyValue* kv = data_value.add_associated_data();
    kv->set_key(std::string(kEncryptedGaiaIdKey));
    kv->set_bytes_value(gaia_id);
  }
  return data_value;
}

// Serializes a PackedDataValueGroup into a contiguous PackedValueBuffer.
PackedValueBuffer SerializeGroupToBuffer(const PackedDataValueGroup& group,
                                         uint32_t version_id) {
  PackedValueBuffer buffer;
  const size_t byte_size = group.ByteSizeLong();
  buffer.size = static_cast<uint32_t>(byte_size);
  buffer.version_id = version_id;
  if (byte_size > 0) {
    buffer.data = std::make_unique<char[]>(byte_size);
    group.SerializeWithCachedSizesToArray(
        reinterpret_cast<uint8_t*>(buffer.data.get()));
  }
  return buffer;
}

// Parses a PackedDataValueGroup from a PackedValueBuffer.
void ParseGroupFromBuffer(const PackedValueBuffer& buffer,
                          PackedDataValueGroup& group) {
  if (buffer.size == 0 || buffer.data == nullptr) {
    group.Clear();
    return;
  }
  if (!group.ParseFromArray(buffer.data.get(), static_cast<int>(buffer.size))) {
    group.Clear();
  }
}

// Extracts per-record version IDs for a parsed PackedDataValueGroup.
std::vector<uint32_t> ExtractRecordVersions(const PackedDataValueGroup& group,
                                            uint32_t buffer_version_id) {
  std::vector<uint32_t> versions;
  versions.reserve(group.records_size());
  for (int i = 0; i < group.records_size(); ++i) {
    const uint32_t record_version = (i < group.record_version_ids_size())
                                        ? group.record_version_ids(i)
                                        : buffer_version_id;
    versions.push_back(record_version);
  }
  return versions;
}

// Packs a group and its per-record version IDs into a PackedValueBuffer.
// When all records share the same version ID, record_version_ids is left empty
// (0 wire bytes) and the version is stored in PackedValueBuffer::version_id.
PackedValueBuffer PackWithRecordVersions(PackedDataValueGroup& group,
                                         absl::Span<const uint32_t> versions) {
  if (versions.empty()) {
    group.clear_record_version_ids();
    return SerializeGroupToBuffer(group, 0);
  }

  const uint32_t first_version = versions[0];
  bool all_same_version = true;
  for (uint32_t v : versions) {
    if (v != first_version) {
      all_same_version = false;
      break;
    }
  }

  if (all_same_version) {
    group.clear_record_version_ids();
    return SerializeGroupToBuffer(group, first_version);
  }

  group.clear_record_version_ids();
  group.mutable_record_version_ids()->Reserve(versions.size());
  for (uint32_t v : versions) {
    group.add_record_version_ids(v);
  }
  return SerializeGroupToBuffer(group, kMixedVersionId);
}

}  // namespace

ExecutionResult MarkedDataTableStorage::Insert(
    absl::string_view key,
    const DataValueInternal& data_value_internal) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    LegacyMarkedDataTableImpl::accessor write_accessor;
    bool is_new = legacy_concurrent_map_.insert(write_accessor, key_array);
    if (is_new) {
      write_accessor->second.push_back(data_value_internal.SerializeAsString());
      write_accessor.release();
      return SuccessExecutionResult();
    }
    bool found_existing_data_value = UpdateVersionIdForMatchedRecord(
        write_accessor->second, data_value_internal.data_value(),
        data_value_internal.version_id());
    if (found_existing_data_value) {
      write_accessor.release();
      return SuccessExecutionResult();
    }
    write_accessor->second.push_back(data_value_internal.SerializeAsString());
    write_accessor.release();
    return SuccessExecutionResult();
  }

  CompactAssociatedData compact_record =
      ToCompactAssociatedData(data_value_internal.data_value());

  MarkedDataTableImpl::accessor write_accessor;
  bool is_new = concurrent_map_.insert(write_accessor, key_array);

  if (is_new) {
    PackedDataValueGroup group;
    *group.add_records() = std::move(compact_record);
    write_accessor->second =
        SerializeGroupToBuffer(group, data_value_internal.version_id());
    write_accessor.release();
    return SuccessExecutionResult();
  }

  PackedDataValueGroup group;
  ParseGroupFromBuffer(write_accessor->second, group);
  std::vector<uint32_t> versions =
      ExtractRecordVersions(group, write_accessor->second.version_id);

  // If a matching data value is present in the table, update its version ID.
  for (int i = 0; i < group.records_size(); ++i) {
    if (MessageDifferencer::Equals(group.records(i), compact_record)) {
      if (versions[i] != data_value_internal.version_id()) {
        versions[i] = data_value_internal.version_id();
        write_accessor->second = PackWithRecordVersions(group, versions);
      }
      write_accessor.release();
      return SuccessExecutionResult();
    }
  }

  // Data entry wasn't found, append it.
  *group.add_records() = std::move(compact_record);
  versions.push_back(data_value_internal.version_id());
  write_accessor->second = PackWithRecordVersions(group, versions);
  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Replace(
    absl::string_view key, absl::Span<const DataValue> data_values,
    uint32_t version_id) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    std::vector<std::string> new_data_values;
    new_data_values.reserve(data_values.size());
    for (const DataValue& data_value : data_values) {
      DataValueInternal data_value_internal;
      *data_value_internal.mutable_data_value() = data_value;
      data_value_internal.set_version_id(version_id);
      new_data_values.push_back(data_value_internal.SerializeAsString());
    }
    LegacyMarkedDataTableImpl::accessor write_accessor;
    legacy_concurrent_map_.insert(write_accessor, key_array);
    write_accessor->second = std::move(new_data_values);
    write_accessor.release();
    return SuccessExecutionResult();
  }

  PackedDataValueGroup group;
  group.mutable_records()->Reserve(data_values.size());
  for (const DataValue& data_value : data_values) {
    *group.add_records() = ToCompactAssociatedData(data_value);
  }
  PackedValueBuffer new_buffer = SerializeGroupToBuffer(group, version_id);

  MarkedDataTableImpl::accessor write_accessor;
  concurrent_map_.insert(write_accessor, key_array);

  write_accessor->second = std::move(new_buffer);
  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Replace(
    absl::string_view key,
    absl::Span<const DataValueInternal> data_values) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    std::vector<std::string> new_data_values;
    new_data_values.reserve(data_values.size());
    for (const auto& data_value_internal : data_values) {
      new_data_values.push_back(data_value_internal.SerializeAsString());
    }
    LegacyMarkedDataTableImpl::accessor write_accessor;
    legacy_concurrent_map_.insert(write_accessor, key_array);
    write_accessor->second = std::move(new_data_values);
    write_accessor.release();
    return SuccessExecutionResult();
  }

  PackedDataValueGroup group;
  group.mutable_records()->Reserve(data_values.size());
  std::vector<uint32_t> versions;
  versions.reserve(data_values.size());
  for (const auto& data_value_internal : data_values) {
    *group.add_records() =
        ToCompactAssociatedData(data_value_internal.data_value());
    versions.push_back(data_value_internal.version_id());
  }
  PackedValueBuffer new_buffer = PackWithRecordVersions(group, versions);

  MarkedDataTableImpl::accessor write_accessor;
  concurrent_map_.insert(write_accessor, key_array);

  write_accessor->second = std::move(new_buffer);
  write_accessor.release();
  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Find(
    absl::string_view key, std::vector<DataValue>& out) const noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    LegacyMarkedDataTableImpl::const_accessor read_accessor;
    if (!legacy_concurrent_map_.find(read_accessor, key_array)) {
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

  MarkedDataTableImpl::const_accessor read_accessor;
  if (!concurrent_map_.find(read_accessor, key_array)) {
    read_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  PackedDataValueGroup group;
  ParseGroupFromBuffer(read_accessor->second, group);
  read_accessor.release();

  out.reserve(out.size() + group.records_size());
  for (const CompactAssociatedData& record : group.records()) {
    out.push_back(FromCompactAssociatedData(record));
  }

  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::Find(
    absl::string_view key, std::vector<DataValueInternal>& out) const noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    LegacyMarkedDataTableImpl::const_accessor read_accessor;
    if (!legacy_concurrent_map_.find(read_accessor, key_array)) {
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

  MarkedDataTableImpl::const_accessor read_accessor;
  if (!concurrent_map_.find(read_accessor, key_array)) {
    read_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  PackedDataValueGroup group;
  ParseGroupFromBuffer(read_accessor->second, group);
  const uint32_t buffer_version_id = read_accessor->second.version_id;
  read_accessor.release();

  out.reserve(out.size() + group.records_size());
  for (int i = 0; i < group.records_size(); ++i) {
    DataValueInternal data_value_internal;
    *data_value_internal.mutable_data_value() =
        FromCompactAssociatedData(group.records(i));
    const uint32_t record_version = (i < group.record_version_ids_size())
                                        ? group.record_version_ids(i)
                                        : buffer_version_id;
    data_value_internal.set_version_id(record_version);
    out.push_back(std::move(data_value_internal));
  }

  return SuccessExecutionResult();
}

ExecutionResult MarkedDataTableStorage::EraseUnmatchedVersions(
    absl::string_view key, uint32_t version_id_to_keep) noexcept {
  if (key.size() != kMarkedDataTableKeyLength) {
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    LegacyMarkedDataTableImpl::accessor write_accessor;
    if (!legacy_concurrent_map_.find(write_accessor, key_array)) {
      write_accessor.release();
      return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
    }
    std::vector<std::string> filtered_data_values;
    filtered_data_values.reserve(write_accessor->second.size());
    for (const std::string& serialized : write_accessor->second) {
      DataValueInternal data_value_internal;
      data_value_internal.ParseFromString(serialized);
      if (data_value_internal.version_id() == version_id_to_keep) {
        filtered_data_values.push_back(serialized);
      }
    }
    if (!filtered_data_values.empty()) {
      filtered_data_values.shrink_to_fit();
      write_accessor->second = std::move(filtered_data_values);
    } else {
      legacy_concurrent_map_.erase(write_accessor);
    }
    write_accessor.release();
    return SuccessExecutionResult();
  }

  MarkedDataTableImpl::accessor write_accessor;
  if (!concurrent_map_.find(write_accessor, key_array)) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  if (write_accessor->second.version_id == version_id_to_keep) {
    write_accessor.release();
    return SuccessExecutionResult();
  }

  if (write_accessor->second.version_id != kMixedVersionId) {
    concurrent_map_.erase(write_accessor);
    write_accessor.release();
    return SuccessExecutionResult();
  }

  PackedDataValueGroup group;
  ParseGroupFromBuffer(write_accessor->second, group);
  PackedDataValueGroup filtered_group;
  for (int i = 0; i < group.records_size(); ++i) {
    const uint32_t record_version = (i < group.record_version_ids_size())
                                        ? group.record_version_ids(i)
                                        : write_accessor->second.version_id;
    if (record_version == version_id_to_keep) {
      *filtered_group.add_records() = std::move(*group.mutable_records(i));
    }
  }

  if (filtered_group.records_size() != 0) {
    write_accessor->second =
        SerializeGroupToBuffer(filtered_group, version_id_to_keep);
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
  if (!enable_packed_storage_) {
    if (!legacy_concurrent_map_.erase(key_array)) {
      return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
    }
    return SuccessExecutionResult();
  }
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

  std::array<char, kMarkedDataTableKeyLength> key_array;
  std::copy(key.begin(), key.end(), key_array.data());

  if (!enable_packed_storage_) {
    LegacyMarkedDataTableImpl::accessor write_accessor;
    if (!legacy_concurrent_map_.find(write_accessor, key_array)) {
      write_accessor.release();
      return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
    }
    bool found = false;
    auto iter = write_accessor->second.begin();
    while (iter != write_accessor->second.end()) {
      DataValueInternal data_value;
      data_value.ParseFromString(*iter);
      if (MessageDifferencer::Equals(data_value.data_value(),
                                     data_value_to_erase)) {
        found = true;
        write_accessor->second.erase(iter);
        break;
      }
      ++iter;
    }
    if (!found) {
      write_accessor.release();
      return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
    }
    if (write_accessor->second.empty()) {
      legacy_concurrent_map_.erase(write_accessor);
    } else if (write_accessor->second.size() * kVectorDownscalingFactor <
               write_accessor->second.capacity()) {
      write_accessor->second.shrink_to_fit();
    }
    write_accessor.release();
    return SuccessExecutionResult();
  }

  MarkedDataTableImpl::accessor write_accessor;
  if (!concurrent_map_.find(write_accessor, key_array)) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  CompactAssociatedData target_record =
      ToCompactAssociatedData(data_value_to_erase);

  PackedDataValueGroup group;
  ParseGroupFromBuffer(write_accessor->second, group);
  std::vector<uint32_t> versions =
      ExtractRecordVersions(group, write_accessor->second.version_id);

  bool found = false;
  PackedDataValueGroup updated_group;
  std::vector<uint32_t> updated_versions;
  for (int i = 0; i < group.records_size(); ++i) {
    if (!found && MessageDifferencer::Equals(group.records(i), target_record)) {
      found = true;
      continue;
    }
    *updated_group.add_records() = std::move(*group.mutable_records(i));
    updated_versions.push_back(versions[i]);
  }

  if (!found) {
    write_accessor.release();
    return FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST);
  }

  if (updated_group.records_size() == 0) {
    concurrent_map_.erase(write_accessor);
  } else {
    write_accessor->second =
        PackWithRecordVersions(updated_group, updated_versions);
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

MarkedDataTableStorage::LegacyMarkedDataTableImpl::iterator
MarkedDataTableStorage::LegacyBegin() noexcept {
  return legacy_concurrent_map_.begin();
}

MarkedDataTableStorage::LegacyMarkedDataTableImpl::iterator
MarkedDataTableStorage::LegacyEnd() noexcept {
  return legacy_concurrent_map_.end();
}

}  // namespace google::confidential_match
