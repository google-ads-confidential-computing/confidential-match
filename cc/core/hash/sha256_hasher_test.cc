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

#include "cc/core/hash/sha256_hasher.h"

#include <string>

#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "cc/core/hash/mock_hasher.h"

namespace google::confidential_match {
namespace {

using ::absl_testing::IsOkAndHolds;

class Sha256HasherTest : public ::testing::Test {
 protected:
  Sha256HasherTest() : sha256_hasher_() {}

  Sha256Hasher sha256_hasher_;
};

TEST_F(Sha256HasherTest, Base64EncodedHashEmptyStringSuccess) {
  absl::StatusOr<std::string> result = sha256_hasher_.Base64EncodedHash("");
  EXPECT_THAT(result,
              IsOkAndHolds("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="));
}

TEST_F(Sha256HasherTest, Base64EncodedHashEmailSuccess) {
  absl::StatusOr<std::string> result =
      sha256_hasher_.Base64EncodedHash("match.service@cfm.google.com");
  EXPECT_THAT(result,
              IsOkAndHolds("QEN4TkLL/QDOn9Fz09arwqJsdSh9oI5tfuhgy+WMmKA="));
}
}  // namespace
}  // namespace google::confidential_match
