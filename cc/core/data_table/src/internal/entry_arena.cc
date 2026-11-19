// Copyright 2026 Google LLC
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

#include "cc/core/data_table/src/internal/entry_arena.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match {

void EntryArena::SetCompactionTargetBytes(size_t exact_live_bytes) noexcept {
  CHECK(slabs_.empty());
  compaction_target_bytes_ = exact_live_bytes;
}

char* EntryArena::AllocateRawBytes(size_t byte_size, uint64_t* encoded_offset) {
  CHECK_GT(byte_size, 0u);
  CHECK_LE(byte_size, kEntryArenaMaxSlabSize);

  if (slabs_.empty() ||
      byte_size > current_slab_capacity_ - current_slab_offset_) {
    if (!slabs_.empty() && current_slab_offset_ < current_slab_capacity_) {
      slab_end_waste_bytes_ += (current_slab_capacity_ - current_slab_offset_);
    }
    size_t slab_size = 0;
    if (compaction_target_bytes_ > live_bytes_) {
      const size_t remaining = compaction_target_bytes_ - live_bytes_;
      slab_size = std::min<size_t>(remaining, kEntryArenaMaxSlabSize);
      if (slab_size < byte_size) {
        slab_size = std::max<size_t>(byte_size, next_growth_slab_size_);
      }
    } else {
      while (next_growth_slab_size_ < byte_size &&
             next_growth_slab_size_ < kEntryArenaMaxSlabSize) {
        next_growth_slab_size_ = std::min<size_t>(next_growth_slab_size_ * 2,
                                                  kEntryArenaMaxSlabSize);
      }
      slab_size = std::max<size_t>(byte_size, next_growth_slab_size_);
      next_growth_slab_size_ =
          std::min<size_t>(next_growth_slab_size_ * 2, kEntryArenaMaxSlabSize);
    }
    // Use default-initialized new char[] rather than std::make_unique<char[]>
    // to avoid value-initializing (zero-filling) untouched slab pages.
    slabs_.push_back(std::unique_ptr<char[]>(new char[slab_size]));
    current_slab_offset_ = 0;
    current_slab_capacity_ = slab_size;
    total_allocated_slab_bytes_ += slab_size;
  }

  const uint64_t slab_index = slabs_.size() - 1;
  CHECK_LT(slab_index, 1ULL << (40 - kEntryArenaSlabOffsetBits));
  *encoded_offset =
      (slab_index << kEntryArenaSlabOffsetBits) | current_slab_offset_;
  char* dst = slabs_.back().get() + current_slab_offset_;
  current_slab_offset_ += byte_size;
  live_bytes_ += byte_size;
  if (compaction_target_bytes_ > 0 && live_bytes_ >= compaction_target_bytes_) {
    compaction_target_bytes_ = 0;
  }
  return dst;
}

uint64_t EntryArena::AllocateEntry(absl::string_view key,
                                   absl::string_view payload) {
  CHECK_EQ(key.size(), kEntryArenaKeySize);
  CHECK_LE(payload.size(), std::numeric_limits<uint16_t>::max());
  compaction_target_bytes_ = 0;

  uint64_t encoded_offset = 0;
  char* dst =
      AllocateRawBytes(kEntryArenaKeySize + payload.size(), &encoded_offset);
  std::memcpy(dst, key.data(), kEntryArenaKeySize);
  if (!payload.empty()) {
    std::memcpy(dst + kEntryArenaKeySize, payload.data(), payload.size());
  }
  return encoded_offset;
}

uint64_t EntryArena::CopyEntrySlice(absl::string_view entry_slice) {
  CHECK_GE(entry_slice.size(), kEntryArenaKeySize);
  CHECK_LE(entry_slice.size(),
           kEntryArenaKeySize + std::numeric_limits<uint16_t>::max());

  uint64_t encoded_offset = 0;
  char* dst = AllocateRawBytes(entry_slice.size(), &encoded_offset);
  std::memcpy(dst, entry_slice.data(), entry_slice.size());
  return encoded_offset;
}

void EntryArena::OverwriteEntryPayload(uint64_t encoded_offset,
                                       absl::string_view payload,
                                       uint16_t old_payload_size) noexcept {
  CHECK_LE(payload.size(), old_payload_size);
  const size_t slab_index =
      static_cast<size_t>(encoded_offset >> kEntryArenaSlabOffsetBits);
  CHECK_LT(slab_index, slabs_.size());
  const size_t slab_offset =
      static_cast<size_t>(encoded_offset & kEntryArenaSlabOffsetMask);
  const size_t slab_bound = (slab_index + 1 == slabs_.size())
                                ? current_slab_offset_
                                : kEntryArenaMaxSlabSize;
  CHECK_LE(slab_offset + kEntryArenaKeySize + old_payload_size, slab_bound);

  if (!payload.empty()) {
    std::memcpy(slabs_[slab_index].get() + slab_offset + kEntryArenaKeySize,
                payload.data(), payload.size());
  }
  const size_t reclaimed = old_payload_size - payload.size();
  CHECK_GE(live_bytes_, reclaimed);
  live_bytes_ -= reclaimed;
  dead_bytes_ += reclaimed;
}

void EntryArena::RecordDeadEntry(uint16_t old_payload_size) noexcept {
  const size_t entry_bytes = kEntryArenaKeySize + old_payload_size;
  CHECK_GE(live_bytes_, entry_bytes);
  live_bytes_ -= entry_bytes;
  dead_bytes_ += entry_bytes;
}

absl::string_view EntryArena::GetKeySlice(
    uint64_t encoded_offset) const noexcept {
  const size_t slab_index =
      static_cast<size_t>(encoded_offset >> kEntryArenaSlabOffsetBits);
  CHECK_LT(slab_index, slabs_.size());
  const size_t slab_offset =
      static_cast<size_t>(encoded_offset & kEntryArenaSlabOffsetMask);
  const size_t slab_bound = (slab_index + 1 == slabs_.size())
                                ? current_slab_offset_
                                : kEntryArenaMaxSlabSize;
  CHECK_LE(slab_offset + kEntryArenaKeySize, slab_bound);
  return absl::string_view(slabs_[slab_index].get() + slab_offset,
                           kEntryArenaKeySize);
}

absl::string_view EntryArena::GetPayloadSlice(
    uint64_t encoded_offset, uint16_t payload_size) const noexcept {
  const size_t slab_index =
      static_cast<size_t>(encoded_offset >> kEntryArenaSlabOffsetBits);
  CHECK_LT(slab_index, slabs_.size());
  const size_t slab_offset =
      static_cast<size_t>(encoded_offset & kEntryArenaSlabOffsetMask);
  const size_t slab_bound = (slab_index + 1 == slabs_.size())
                                ? current_slab_offset_
                                : kEntryArenaMaxSlabSize;
  CHECK_LE(slab_offset + kEntryArenaKeySize + payload_size, slab_bound);
  if (payload_size == 0) {
    return absl::string_view();
  }
  return absl::string_view(
      slabs_[slab_index].get() + slab_offset + kEntryArenaKeySize,
      payload_size);
}

absl::string_view EntryArena::GetEntrySlice(
    uint64_t encoded_offset, uint16_t payload_size) const noexcept {
  const size_t slab_index =
      static_cast<size_t>(encoded_offset >> kEntryArenaSlabOffsetBits);
  CHECK_LT(slab_index, slabs_.size());
  const size_t slab_offset =
      static_cast<size_t>(encoded_offset & kEntryArenaSlabOffsetMask);
  const size_t slab_bound = (slab_index + 1 == slabs_.size())
                                ? current_slab_offset_
                                : kEntryArenaMaxSlabSize;
  CHECK_LE(slab_offset + kEntryArenaKeySize + payload_size, slab_bound);
  return absl::string_view(slabs_[slab_index].get() + slab_offset,
                           kEntryArenaKeySize + payload_size);
}

void EntryArena::TrimTailSlab() {
  if (slabs_.empty() || current_slab_offset_ == current_slab_capacity_) {
    return;
  }
  const size_t unused_tail = current_slab_capacity_ - current_slab_offset_;
  // Use default-initialized new char[] since the entire buffer is immediately
  // overwritten via memcpy.
  std::unique_ptr<char[]> shrunk(new char[current_slab_offset_]);
  std::memcpy(shrunk.get(), slabs_.back().get(), current_slab_offset_);
  slabs_.back() = std::move(shrunk);
  total_allocated_slab_bytes_ -= unused_tail;
  current_slab_capacity_ = current_slab_offset_;
}

void EntryArena::Clear() noexcept {
  slabs_.clear();
  current_slab_offset_ = 0;
  current_slab_capacity_ = 0;
  next_growth_slab_size_ = kEntryArenaInitialSlabSize;
  compaction_target_bytes_ = 0;
  live_bytes_ = 0;
  total_allocated_slab_bytes_ = 0;
  dead_bytes_ = 0;
  slab_end_waste_bytes_ = 0;
}

}  // namespace google::confidential_match
