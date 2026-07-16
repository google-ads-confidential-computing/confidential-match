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

#include "cc/match_service/converters/match_response_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(MatchResponseConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::MatchResponse in;
  backend::MatchResponse out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_data_records_size(), 0);
  EXPECT_EQ(out.matched_associated_data_size(), 0);
  EXPECT_EQ(out.metadata_size(), 0);
}

TEST(MatchResponseConvertersTest, ToBackendConvertsMatchedDataRecords) {
  api::v1::MatchResponse in;
  in.add_matched_data_records();
  backend::MatchResponse out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_data_records_size(), 1);
}

TEST(MatchResponseConvertersTest, ToBackendConvertsMatchedAssociatedData) {
  api::v1::MatchResponse in;
  in.add_matched_associated_data();
  backend::MatchResponse out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_associated_data_size(), 1);
}

TEST(MatchResponseConvertersTest, ToBackendConvertsMetadata) {
  api::v1::MatchResponse in;
  in.add_metadata();
  backend::MatchResponse out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

TEST(MatchResponseConvertersTest, ToApiConvertsEmptyObject) {
  backend::MatchResponse in;
  api::v1::MatchResponse out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_data_records_size(), 0);
  EXPECT_EQ(out.matched_associated_data_size(), 0);
  EXPECT_EQ(out.metadata_size(), 0);
}

TEST(MatchResponseConvertersTest, ToApiConvertsMatchedDataRecords) {
  backend::MatchResponse in;
  in.add_matched_data_records();
  api::v1::MatchResponse out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_data_records_size(), 1);
}

TEST(MatchResponseConvertersTest, ToApiConvertsMatchedAssociatedData) {
  backend::MatchResponse in;
  in.add_matched_associated_data();
  api::v1::MatchResponse out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_associated_data_size(), 1);
}

TEST(MatchResponseConvertersTest, ToApiConvertsMetadata) {
  backend::MatchResponse in;
  in.add_metadata();
  api::v1::MatchResponse out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

}  // namespace google::confidential_match::match_service
