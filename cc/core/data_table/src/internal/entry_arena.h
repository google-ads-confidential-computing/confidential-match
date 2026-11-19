/*
 * Copyright 2026 Google LLC
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

#ifndef CC_CORE_DATA_TABLE_SRC_INTERNAL_ENTRY_ARENA_H_
#define CC_CORE_DATA_TABLE_SRC_INTERNAL_ENTRY_ARENA_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match {

// Length of the 32-byte key prefix stored at the start of each arena entry.
inline constexpr size_t kEntryArenaKeySize = 32;
// Maximum size of a single contiguous arena slab (128 KiB = 2^17 bytes).
// 128 KiB is an exact power-of-2 jemalloc size class (zero internal
// size-class rounding waste) while accommodating any 32-byte key + uint16_t
// payload (up to 65,567 bytes) in a single slab.
inline constexpr size_t kEntryArenaMaxSlabSize = 128 * 1024;
// Initial slab size for incremental growth (4 KiB), doubling up to 128 KiB.
inline constexpr size_t kEntryArenaInitialSlabSize = 4 * 1024;
inline constexpr size_t kEntryArenaSlabOffsetBits = 17;
inline constexpr uint64_t kEntryArenaSlabOffsetMask =
    (1ULL << kEntryArenaSlabOffsetBits) - 1;

static_assert(kEntryArenaMaxSlabSize == (1ULL << kEntryArenaSlabOffsetBits));

// Compact 8-byte lookup index entry referencing a `[key + payload]` slice in a
// shard's EntryArena slabs.
// Note: All three `mutable` bitfields share a single 64-bit word; mutating any
// field requires an exclusive shard lock (never under `ReaderMutexLock`), and
// `encoded_offset` may only be reassigned to an identical 32-byte key.
struct ArenaIndexEntry {
  constexpr ArenaIndexEntry() noexcept
      : encoded_offset(0), payload_size(0), version_id(0) {}

  constexpr ArenaIndexEntry(uint64_t encoded_offset, uint16_t payload_size,
                            uint8_t version_id) noexcept
      : encoded_offset(encoded_offset),
        payload_size(payload_size),
        version_id(version_id) {}

  // Encoded byte offset in the shard's EntryArena slabs:
  // upper 23 bits = slab index, lower 17 bits = byte offset within the slab.
  mutable uint64_t encoded_offset : 40;
  // Serialized PackedDataValueGroup byte length (max 65535 bytes; excludes the
  // 32-byte inline key prefix).
  mutable uint64_t payload_size : 16;
  // Dataset snapshot version identifier (0..255).
  mutable uint64_t version_id : 8;
};

static_assert(sizeof(ArenaIndexEntry) == 8);

/**
 * @brief Bump-pointer slab allocator storing inline `[32-byte key][payload]`
 * entries in contiguous slabs growing from 4 KiB up to 128 KiB.
 *
 * Eliminates per-entry heap allocator metadata, size-class rounding overhead,
 * and empty hash-table key slot overhead. Dead space from overwritten or
 * deleted entries is reclaimed via shard-by-shard arena compaction during
 * FinalizeUpdate().
 */
class EntryArena {
 public:
  EntryArena() = default;
  EntryArena(EntryArena&&) noexcept = default;
  EntryArena& operator=(EntryArena&&) noexcept = default;

  EntryArena(const EntryArena&) = delete;
  EntryArena& operator=(const EntryArena&) = delete;

  /**
   * @brief Sets the exact number of live bytes expected to be written into an
   * empty compaction arena so slabs are sized without tail waste.
   */
  void SetCompactionTargetBytes(size_t exact_live_bytes) noexcept;

  /**
   * @brief Allocates `kEntryArenaKeySize + payload.size()` bytes in the arena,
   * copies the 32-byte `key` followed by `payload`, and returns the 40-bit
   * encoded offset.
   */
  uint64_t AllocateEntry(absl::string_view key, absl::string_view payload);

  /**
   * @brief Copies a raw `[32-byte key][payload]` entry slice into the arena
   * (used during shard compaction) and returns the new 40-bit encoded offset.
   */
  uint64_t CopyEntrySlice(absl::string_view entry_slice);

  /**
   * @brief Overwrites the payload portion of an existing entry at
   * `encoded_offset` in-place when `payload.size() <= old_payload_size`.
   */
  void OverwriteEntryPayload(uint64_t encoded_offset, absl::string_view payload,
                             uint16_t old_payload_size) noexcept;

  /**
   * @brief Marks an existing entry of `old_payload_size` bytes (plus its
   * 32-byte key) as dead space to be reclaimed on compaction.
   */
  void RecordDeadEntry(uint16_t old_payload_size) noexcept;

  /**
   * @brief Returns a 32-byte string_view of the key stored at `encoded_offset`.
   */
  absl::string_view GetKeySlice(uint64_t encoded_offset) const noexcept
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  /**
   * @brief Returns a string_view of the serialized payload stored after the
   * 32-byte key at `encoded_offset`.
   */
  absl::string_view GetPayloadSlice(uint64_t encoded_offset,
                                    uint16_t payload_size) const noexcept
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  /**
   * @brief Returns a string_view of the full `[32-byte key][payload]` entry at
   * `encoded_offset`.
   */
  absl::string_view GetEntrySlice(uint64_t encoded_offset,
                                  uint16_t payload_size) const noexcept
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

  /**
   * @brief Reallocates the final active tail slab to its exact used offset
   * without moving prior slabs or invalidating encoded offsets.
   *
   * Note: Reallocating `slabs_.back()` invalidates any `absl::string_view`
   * slices pointing into the tail slab.
   */
  void TrimTailSlab();

  /** @brief Releases all allocated slabs and resets usage counters. */
  void Clear() noexcept;

  /** @brief Returns the number of slabs currently allocated. */
  size_t NumSlabs() const noexcept { return slabs_.size(); }

  /** @brief Returns total live entry bytes currently stored in this arena. */
  size_t LiveBytes() const noexcept { return live_bytes_; }

  /** @brief Returns total allocated slab capacity in bytes. */
  size_t TotalAllocatedSlabBytes() const noexcept {
    return total_allocated_slab_bytes_;
  }

  /** @brief Returns bytes invalidated by erased or overwritten entries. */
  size_t DeadBytes() const noexcept { return dead_bytes_; }

  /** @brief Returns unused bytes at the end of full non-tail slabs. */
  size_t SlabEndWasteBytes() const noexcept { return slab_end_waste_bytes_; }

 private:
  char* AllocateRawBytes(size_t byte_size, uint64_t* encoded_offset);

  std::vector<std::unique_ptr<char[]>> slabs_;
  size_t current_slab_offset_ = 0;
  size_t current_slab_capacity_ = 0;
  size_t next_growth_slab_size_ = kEntryArenaInitialSlabSize;
  size_t compaction_target_bytes_ = 0;
  size_t live_bytes_ = 0;
  size_t total_allocated_slab_bytes_ = 0;
  size_t dead_bytes_ = 0;
  size_t slab_end_waste_bytes_ = 0;
};

}  // namespace google::confidential_match

#endif  // CC_CORE_DATA_TABLE_SRC_INTERNAL_ENTRY_ARENA_H_
