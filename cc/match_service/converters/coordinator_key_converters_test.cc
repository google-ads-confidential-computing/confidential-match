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

#include "cc/match_service/converters/coordinator_key_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(CoordinatorKeyConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::CoordinatorKey in;
  backend::CoordinatorKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_id(), "");
}

TEST(CoordinatorKeyConvertersTest, ToBackendConvertsKeyId) {
  api::v1::CoordinatorKey in;
  in.set_key_id("sample_key_id");
  backend::CoordinatorKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_id(), "sample_key_id");
}

TEST(CoordinatorKeyConvertersTest, ToApiConvertsEmptyObject) {
  backend::CoordinatorKey in;
  api::v1::CoordinatorKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_id(), "");
}

TEST(CoordinatorKeyConvertersTest, ToApiConvertsKeyId) {
  backend::CoordinatorKey in;
  in.set_key_id("sample_key_id");
  api::v1::CoordinatorKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_id(), "sample_key_id");
}

}  // namespace google::confidential_match::match_service
