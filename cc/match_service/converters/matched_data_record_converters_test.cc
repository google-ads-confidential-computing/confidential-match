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

#include "cc/match_service/converters/matched_data_record_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(MatchedDataRecordConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::MatchedDataRecord in;
  backend::MatchedDataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_keys_size(), 0);
  EXPECT_EQ(out.metadata_size(), 0);
  EXPECT_FALSE(out.has_data_record_associated_data());
}

TEST(MatchedDataRecordConvertersTest, ToBackendConvertsMatchedKeys) {
  api::v1::MatchedDataRecord in;
  in.add_matched_keys();
  backend::MatchedDataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_keys_size(), 1);
}

TEST(MatchedDataRecordConvertersTest, ToBackendConvertsMetadata) {
  api::v1::MatchedDataRecord in;
  in.add_metadata();
  backend::MatchedDataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

TEST(MatchedDataRecordConvertersTest,
     ToBackendConvertsDataRecordAssociatedData) {
  api::v1::MatchedDataRecord in;
  in.mutable_data_record_associated_data()->set_selected_first_party_identifier(
      "id");
  backend::MatchedDataRecord out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.data_record_associated_data().selected_first_party_identifier(),
            "id");
}

TEST(MatchedDataRecordConvertersTest, ToApiConvertsEmptyObject) {
  backend::MatchedDataRecord in;
  api::v1::MatchedDataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_keys_size(), 0);
  EXPECT_EQ(out.metadata_size(), 0);
  EXPECT_FALSE(out.has_data_record_associated_data());
}

TEST(MatchedDataRecordConvertersTest, ToApiConvertsMatchedKeys) {
  backend::MatchedDataRecord in;
  in.add_matched_keys();
  api::v1::MatchedDataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_keys_size(), 1);
}

TEST(MatchedDataRecordConvertersTest, ToApiConvertsMetadata) {
  backend::MatchedDataRecord in;
  in.add_metadata();
  api::v1::MatchedDataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

TEST(MatchedDataRecordConvertersTest, ToApiConvertsDataRecordAssociatedData) {
  backend::MatchedDataRecord in;
  in.mutable_data_record_associated_data()->set_selected_first_party_identifier(
      "id");
  api::v1::MatchedDataRecord out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.data_record_associated_data().selected_first_party_identifier(),
            "id");
}

}  // namespace google::confidential_match::match_service
