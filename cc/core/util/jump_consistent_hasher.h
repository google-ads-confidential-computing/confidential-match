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

#ifndef CC_CORE_UTIL_JUMP_CONSISTENT_HASHER_H_
#define CC_CORE_UTIL_JUMP_CONSISTENT_HASHER_H_

#include <cstdint>
#include <string>

namespace google::confidential_match {

// Jump Consistent Hash.  Maps a string (which is hashed to a 64-bit int) to a
// bucket in the range [0, num_buckets).
//
// Reference: "A Fast, Minimal Memory, Consistent Hash Algorithm"
// (Lamping & Veach, 2014)
int32_t JumpConsistentHash(const std::string& input, int32_t num_buckets);

}  // namespace google::confidential_match

#endif  // CC_CORE_UTIL_JUMP_CONSISTENT_HASHER_H_
