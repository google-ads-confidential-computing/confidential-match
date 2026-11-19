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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "include/gtest/gtest.h"

namespace google::confidential_match {
namespace {

constexpr absl::string_view kKey1 = "0123456789abcdef0123456789abcdef";
constexpr absl::string_view kKey2 = "fedcba9876543210fedcba9876543210";

TEST(EntryArenaTest, ArenaIndexEntrySizeAndConstructors) {
  EXPECT_EQ(sizeof(ArenaIndexEntry), 8u);

  constexpr ArenaIndexEntry default_entry;
  EXPECT_EQ(default_entry.encoded_offset, 0u);
  EXPECT_EQ(default_entry.payload_size, 0u);
  EXPECT_EQ(default_entry.version_id, 0u);

  constexpr uint64_t kMax40BitOffset = (1ULL << 40) - 1;
  constexpr ArenaIndexEntry initialized_entry(kMax40BitOffset, 65535u, 255u);
  EXPECT_EQ(initialized_entry.encoded_offset, kMax40BitOffset);
  EXPECT_EQ(initialized_entry.payload_size, 65535u);
  EXPECT_EQ(initialized_entry.version_id, 255u);
}

TEST(EntryArenaTest, AllocateEntryAndGettersRoundTripWithBoundaryPayloads) {
  EntryArena arena;
  EXPECT_EQ(arena.NumSlabs(), 0u);
  EXPECT_EQ(arena.TotalAllocatedSlabBytes(), 0u);
  EXPECT_EQ(arena.LiveBytes(), 0u);

  // 0-byte empty payload.
  const uint64_t empty_offset = arena.AllocateEntry(kKey1, "");
  EXPECT_EQ(arena.GetKeySlice(empty_offset), kKey1);
  EXPECT_TRUE(arena.GetPayloadSlice(empty_offset, 0).empty());
  EXPECT_EQ(arena.GetEntrySlice(empty_offset, 0), kKey1);
  EXPECT_EQ(arena.LiveBytes(), kEntryArenaKeySize);

  // Maximum uint16_t payload (65,535 bytes).
  const std::string max_payload(std::numeric_limits<uint16_t>::max(), 'x');
  const uint64_t max_offset = arena.AllocateEntry(kKey2, max_payload);
  EXPECT_EQ(arena.GetKeySlice(max_offset), kKey2);
  EXPECT_EQ(arena.GetPayloadSlice(max_offset,
                                  static_cast<uint16_t>(max_payload.size())),
            max_payload);
  EXPECT_EQ(arena.GetEntrySlice(max_offset,
                                static_cast<uint16_t>(max_payload.size())),
            std::string(kKey2) + max_payload);

  // Re-verify first entry in earlier slab after slab growth.
  EXPECT_EQ(arena.GetKeySlice(empty_offset), kKey1);
  EXPECT_TRUE(arena.GetPayloadSlice(empty_offset, 0).empty());

  arena.Clear();
  EXPECT_EQ(arena.NumSlabs(), 0u);
  EXPECT_EQ(arena.TotalAllocatedSlabBytes(), 0u);
  EXPECT_EQ(arena.LiveBytes(), 0u);
  EXPECT_EQ(arena.DeadBytes(), 0u);
  EXPECT_EQ(arena.SlabEndWasteBytes(), 0u);
}

TEST(EntryArenaTest, ExponentialSlabGrowthAndSlabEndWaste) {
  EntryArena arena;

  // First allocation fits in initial 4 KiB slab (4096 bytes), leaving 96 bytes
  // unused at the end of slab 0 when the next entry exceeds 96 bytes.
  constexpr uint16_t kFirstPayloadSize =
      kEntryArenaInitialSlabSize - kEntryArenaKeySize - 96;
  const std::string first_payload(kFirstPayloadSize, 'a');
  const uint64_t offset0 = arena.AllocateEntry(kKey1, first_payload);
  EXPECT_EQ(arena.NumSlabs(), 1u);
  EXPECT_EQ(arena.TotalAllocatedSlabBytes(), kEntryArenaInitialSlabSize);
  EXPECT_EQ(arena.SlabEndWasteBytes(), 0u);

  // Second allocation requires 100 + 32 = 132 bytes (> 96 bytes remaining in
  // slab 0), triggering an 8 KiB slab and recording 96 bytes of slab-end waste.
  const std::string second_payload(100, 'b');
  const uint64_t offset1 = arena.AllocateEntry(kKey2, second_payload);
  EXPECT_EQ(arena.NumSlabs(), 2u);
  EXPECT_EQ(arena.TotalAllocatedSlabBytes(),
            kEntryArenaInitialSlabSize + 2 * kEntryArenaInitialSlabSize);
  EXPECT_EQ(arena.SlabEndWasteBytes(), 96u);

  // Allocate 100 entries of 50,000 bytes each (~5 MB across 128 KiB max slabs).
  constexpr uint16_t kLargePayloadSize = 50000;
  constexpr size_t kNumLargeEntries = 100;
  std::vector<uint64_t> offsets;
  offsets.reserve(kNumLargeEntries);
  for (size_t i = 0; i < kNumLargeEntries; ++i) {
    std::string payload(kLargePayloadSize, static_cast<char>('a' + (i % 26)));
    offsets.push_back(arena.AllocateEntry(kKey1, payload));
  }

  EXPECT_GT(arena.NumSlabs(), 20u);
  EXPECT_EQ(arena.GetPayloadSlice(offset0, kFirstPayloadSize), first_payload);
  EXPECT_EQ(arena.GetPayloadSlice(offset1, 100), second_payload);
  for (size_t i = 0; i < kNumLargeEntries; ++i) {
    absl::string_view slice =
        arena.GetPayloadSlice(offsets[i], kLargePayloadSize);
    EXPECT_EQ(slice.size(), kLargePayloadSize);
    EXPECT_EQ(slice.front(), static_cast<char>('a' + (i % 26)));
    EXPECT_EQ(slice.back(), static_cast<char>('a' + (i % 26)));
  }
}

TEST(EntryArenaTest, OverwriteEntryPayloadAndRecordDeadEntryUpdateAccounting) {
  EntryArena arena;
  const std::string initial_payload(100, 'x');
  const uint64_t offset1 = arena.AllocateEntry(kKey1, initial_payload);
  const uint64_t offset2 = arena.AllocateEntry(kKey2, initial_payload);

  const size_t single_entry_bytes = kEntryArenaKeySize + initial_payload.size();
  EXPECT_EQ(arena.LiveBytes(), 2 * single_entry_bytes);
  EXPECT_EQ(arena.DeadBytes(), 0u);

  // Shrink entry 1 payload from 100 bytes down to 40 bytes in-place.
  const std::string smaller_payload(40, 'y');
  arena.OverwriteEntryPayload(offset1, smaller_payload,
                              static_cast<uint16_t>(initial_payload.size()));
  EXPECT_EQ(arena.GetKeySlice(offset1), kKey1);
  EXPECT_EQ(arena.GetPayloadSlice(
                offset1, static_cast<uint16_t>(smaller_payload.size())),
            smaller_payload);
  // Adjacent entry 2 must remain intact.
  EXPECT_EQ(arena.GetKeySlice(offset2), kKey2);
  EXPECT_EQ(arena.GetPayloadSlice(
                offset2, static_cast<uint16_t>(initial_payload.size())),
            initial_payload);

  EXPECT_EQ(arena.LiveBytes(),
            (kEntryArenaKeySize + smaller_payload.size()) + single_entry_bytes);
  EXPECT_EQ(arena.DeadBytes(), initial_payload.size() - smaller_payload.size());

  // Mark entry 2 as dead.
  arena.RecordDeadEntry(static_cast<uint16_t>(initial_payload.size()));
  EXPECT_EQ(arena.LiveBytes(), kEntryArenaKeySize + smaller_payload.size());
  EXPECT_EQ(
      arena.DeadBytes(),
      (initial_payload.size() - smaller_payload.size()) + single_entry_bytes);
}

TEST(EntryArenaTest, TrimTailSlabReclaimsUnusedTailCapacity) {
  EntryArena arena;
  const std::string payload(200, 'z');
  const uint64_t offset = arena.AllocateEntry(kKey1, payload);

  const size_t expected_used = kEntryArenaKeySize + payload.size();
  EXPECT_EQ(arena.TotalAllocatedSlabBytes(), kEntryArenaInitialSlabSize);
  EXPECT_EQ(arena.LiveBytes(), expected_used);

  arena.TrimTailSlab();
  EXPECT_EQ(arena.TotalAllocatedSlabBytes(), expected_used);
  EXPECT_EQ(arena.LiveBytes(), expected_used);
  EXPECT_EQ(arena.GetKeySlice(offset), kKey1);
  EXPECT_EQ(
      arena.GetPayloadSlice(offset, static_cast<uint16_t>(payload.size())),
      payload);
}

TEST(EntryArenaTest,
     SetCompactionTargetBytesAndCopyEntrySliceFollowedByPostCompactionGrowth) {
  EntryArena source_arena;
  const std::string payload1(120, '1');
  const std::string payload2(180, '2');
  const uint64_t src_offset1 = source_arena.AllocateEntry(kKey1, payload1);
  const uint64_t src_offset2 = source_arena.AllocateEntry(kKey2, payload2);

  const size_t exact_live_bytes = source_arena.LiveBytes();
  EntryArena compacted_arena;
  compacted_arena.SetCompactionTargetBytes(exact_live_bytes);

  const uint64_t dst_offset1 =
      compacted_arena.CopyEntrySlice(source_arena.GetEntrySlice(
          src_offset1, static_cast<uint16_t>(payload1.size())));
  const uint64_t dst_offset2 =
      compacted_arena.CopyEntrySlice(source_arena.GetEntrySlice(
          src_offset2, static_cast<uint16_t>(payload2.size())));

  // Compaction pre-sizing allocates a single slab of exact_live_bytes with zero
  // tail waste.
  EXPECT_EQ(compacted_arena.NumSlabs(), 1u);
  EXPECT_EQ(compacted_arena.LiveBytes(), exact_live_bytes);
  EXPECT_EQ(compacted_arena.TotalAllocatedSlabBytes(), exact_live_bytes);
  EXPECT_EQ(compacted_arena.GetKeySlice(dst_offset1), kKey1);
  EXPECT_EQ(compacted_arena.GetPayloadSlice(
                dst_offset1, static_cast<uint16_t>(payload1.size())),
            payload1);
  EXPECT_EQ(compacted_arena.GetKeySlice(dst_offset2), kKey2);
  EXPECT_EQ(compacted_arena.GetPayloadSlice(
                dst_offset2, static_cast<uint16_t>(payload2.size())),
            payload2);

  // Simulate post-compaction erase (dropping LiveBytes() below the original
  // compaction target) followed by a new AllocateEntry():
  // compaction_target_bytes_ must already be reset so the new slab uses normal
  // 4 KiB initial growth rather than a tiny `remaining`-byte micro-slab.
  compacted_arena.RecordDeadEntry(static_cast<uint16_t>(payload2.size()));
  const std::string payload3(50, '3');
  const uint64_t dst_offset3 = compacted_arena.AllocateEntry(kKey2, payload3);
  EXPECT_EQ(compacted_arena.NumSlabs(), 2u);
  EXPECT_EQ(compacted_arena.TotalAllocatedSlabBytes(),
            exact_live_bytes + kEntryArenaInitialSlabSize);
  EXPECT_EQ(compacted_arena.GetPayloadSlice(
                dst_offset3, static_cast<uint16_t>(payload3.size())),
            payload3);
}

TEST(EntryArenaTest, MultiSlabCompactionTargetBytesSizesSlabsWithoutTailWaste) {
  EntryArena source_arena;
  constexpr uint16_t kPayloadSize = 50000;
  const std::string payload1(kPayloadSize, 'a');
  const std::string payload2(kPayloadSize, 'b');
  const std::string payload3(kPayloadSize, 'c');

  const uint64_t src_offset1 = source_arena.AllocateEntry(kKey1, payload1);
  const uint64_t src_offset2 = source_arena.AllocateEntry(kKey2, payload2);
  const uint64_t src_offset3 = source_arena.AllocateEntry(kKey1, payload3);

  const size_t single_entry_bytes = kEntryArenaKeySize + kPayloadSize;
  const size_t exact_live_bytes = 3 * single_entry_bytes;
  ASSERT_GT(exact_live_bytes, kEntryArenaMaxSlabSize);

  EntryArena compacted_arena;
  compacted_arena.SetCompactionTargetBytes(exact_live_bytes);

  const uint64_t dst_offset1 = compacted_arena.CopyEntrySlice(
      source_arena.GetEntrySlice(src_offset1, kPayloadSize));
  const uint64_t dst_offset2 = compacted_arena.CopyEntrySlice(
      source_arena.GetEntrySlice(src_offset2, kPayloadSize));
  const uint64_t dst_offset3 = compacted_arena.CopyEntrySlice(
      source_arena.GetEntrySlice(src_offset3, kPayloadSize));

  // First two 50,032-byte entries fit in the 128 KiB first slab (leaving
  // 131,072 - 100,064 = 31,008 bytes of slab-end waste), and the third entry
  // allocates a second slab sized to the exact remaining 50,032 bytes with zero
  // tail waste.
  EXPECT_EQ(compacted_arena.NumSlabs(), 2u);
  EXPECT_EQ(compacted_arena.LiveBytes(), exact_live_bytes);
  EXPECT_EQ(compacted_arena.SlabEndWasteBytes(),
            kEntryArenaMaxSlabSize - 2 * single_entry_bytes);
  EXPECT_EQ(compacted_arena.TotalAllocatedSlabBytes(),
            kEntryArenaMaxSlabSize + single_entry_bytes);
  EXPECT_EQ(compacted_arena.GetPayloadSlice(dst_offset1, kPayloadSize),
            payload1);
  EXPECT_EQ(compacted_arena.GetPayloadSlice(dst_offset2, kPayloadSize),
            payload2);
  EXPECT_EQ(compacted_arena.GetPayloadSlice(dst_offset3, kPayloadSize),
            payload3);
}

}  // namespace
}  // namespace google::confidential_match
