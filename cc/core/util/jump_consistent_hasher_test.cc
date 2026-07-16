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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <map>
#include <random>
#include <string>
#include <vector>

#include "absl/random/random.h"

namespace google::confidential_match {
namespace {

using ::testing::AllOf;
using ::testing::AnyOf;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::Le;

class JumpConsistentHasherTest : public ::testing::Test {
 protected:
  uint64_t random_seed = 0;

  std::vector<std::string> GetListWithRandomValues(int size) {
    std::vector<std::string> items;
    items.reserve(size);

    std::seed_seq seq{random_seed};
    absl::BitGen absl_rng(seq);

    for (int i = 0; i < size; ++i) {
      uint64_t num = absl::Uniform<uint64_t>(absl_rng);
      items.push_back(std::to_string(num) + "@gmail.com");
    }
    return items;
  }
};

std::map<std::string, int> CreateBucketMap(
    int num_buckets, const std::vector<std::string>& items) {
  std::map<std::string, int> buckets;
  for (const auto& item : items) {
    buckets[item] = JumpConsistentHash(item, num_buckets);
  }
  return buckets;
}

TEST_F(JumpConsistentHasherTest, AddingBucketDoesNotReshuffleToOldBuckets) {
  auto items = GetListWithRandomValues(10000);
  auto old_buckets = CreateBucketMap(10, items);
  // Add a bucket
  auto new_buckets = CreateBucketMap(11, items);

  for (const auto& [key, old_bucket] : old_buckets) {
    int new_bucket = new_buckets[key];
    // Key should either be in its old bucket or in the newly added bucket
    EXPECT_THAT(new_bucket, AnyOf(Eq(old_bucket), Eq(10)))
        << "randomSeed: " << random_seed;
  }
}

TEST_F(JumpConsistentHasherTest, RemovingBucketOnlyMovesItemsInRemovedBucket) {
  auto items = GetListWithRandomValues(10000);
  auto old_buckets = CreateBucketMap(10, items);
  // Remove a bucket
  auto new_buckets = CreateBucketMap(9, items);
  for (const auto& [key, old_bucket] : old_buckets) {
    int new_bucket = new_buckets[key];
    // Keys that used to be in the removed bucket can be in any bucket 0-8.
    // Every other key should have stayed in the same bucket.
    if (old_bucket == 9) {
      EXPECT_THAT(new_bucket, AllOf(Ge(0), Le(8)))
          << "randomSeed: " << random_seed;
    } else {
      EXPECT_EQ(new_bucket, old_bucket) << "randomSeed: " << random_seed;
    }
  }
}

TEST_F(JumpConsistentHasherTest, BucketSizesAreSimilar) {
  int num_buckets = 20;
  auto items = GetListWithRandomValues(100000);
  auto buckets = CreateBucketMap(num_buckets, items);
  // Count frequencies
  std::map<int, int> frequencies;
  for (const auto& [item, bucket] : buckets) {
    frequencies[bucket]++;
  }
  // Check distribution. Expect around 5000 items per bucket
  for (int i = 0; i < num_buckets; ++i) {
    int size = frequencies[i];
    EXPECT_THAT(size, AllOf(Ge(4500), Le(5500)))
        << "randomSeed: " << random_seed;
  }
  // Verify max bucket index
  int max_bucket = frequencies.rbegin()->first;
  EXPECT_EQ(max_bucket, num_buckets - 1) << "randomSeed: " << random_seed;
}

TEST_F(JumpConsistentHasherTest, KnownHashValue) {
  std::string known_hash = "oxjCQhbe/iBv7rc+9b4AAz+pxKdNC5Z/ZTKibKWQbTs=";
  EXPECT_EQ(JumpConsistentHash(known_hash, 150), 144);
}
}  // namespace
}  // namespace google::confidential_match
