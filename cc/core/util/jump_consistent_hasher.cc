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

#include "cc/core/util/jump_consistent_hasher.h"

#include <algorithm>
#include <cstdint>
#include <string>

namespace google::confidential_match {

namespace {

// Matches Guava's HashCode.fromBytes().padToLong() logic used in
// Hashing.consistentHash function
uint64_t StableHash64(const std::string& input) {
  uint64_t h = 0;
  size_t len = std::min(input.size(), size_t(8));

  for (size_t i = 0; i < len; ++i) {
    // Cast to uint8_t to ensure correct bitwise operations
    uint64_t byte_val = static_cast<uint8_t>(input[i]);
    h |= (byte_val << (i * 8));
  }
  return h;
}

}  // namespace

int32_t JumpConsistentHash(const std::string& input, int32_t num_buckets) {
  uint64_t key = StableHash64(input);
  int64_t b = -1, j = 0;
  while (j < num_buckets) {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (b + 1) *
        (static_cast<double>(1LL << 31) / static_cast<double>((key >> 33) + 1));
  }
  return b;
}

}  // namespace google::confidential_match
