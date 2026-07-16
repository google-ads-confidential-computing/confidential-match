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

#include "cc/match_service/converters/data_record_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(DataRecordConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::DataRecord in;
  backend::DataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.match_keys_size(), 0);
  EXPECT_FALSE(out.has_encryption_key());
  EXPECT_EQ(out.metadata_size(), 0);
}

TEST(DataRecordConvertersTest, ToBackendConvertsMatchKeys) {
  api::v1::DataRecord in;
  in.add_match_keys();
  backend::DataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.match_keys_size(), 1);
}

TEST(DataRecordConvertersTest, ToBackendConvertsEncryptionKey) {
  api::v1::DataRecord in;
  in.mutable_encryption_key();
  backend::DataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_encryption_key());
}

TEST(DataRecordConvertersTest, ToBackendConvertsMetadata) {
  api::v1::DataRecord in;
  in.add_metadata();
  backend::DataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

TEST(DataRecordConvertersTest, ToApiConvertsEmptyObject) {
  backend::DataRecord in;
  api::v1::DataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.match_keys_size(), 0);
  EXPECT_FALSE(out.has_encryption_key());
  EXPECT_EQ(out.metadata_size(), 0);
}

TEST(DataRecordConvertersTest, ToApiConvertsMatchKeys) {
  backend::DataRecord in;
  in.add_match_keys();
  api::v1::DataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.match_keys_size(), 1);
}

TEST(DataRecordConvertersTest, ToApiConvertsEncryptionKey) {
  backend::DataRecord in;
  in.mutable_encryption_key();
  api::v1::DataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_encryption_key());
}

TEST(DataRecordConvertersTest, ToApiConvertsMetadata) {
  backend::DataRecord in;
  in.add_metadata();
  api::v1::DataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

}  // namespace google::confidential_match::match_service
