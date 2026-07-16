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

#include "cc/match_service/converters/associated_data_converter.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(AssociatedDataConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::AssociatedData in;
  backend::AssociatedData out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_FALSE(out.has_first_party_identifier());
}

TEST(AssociatedDataConvertersTest, ToBackendConvertsFirstPartyIdentifier) {
  api::v1::AssociatedData in;
  in.mutable_first_party_identifier()->set_id("test-id");
  backend::AssociatedData out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_first_party_identifier());
  EXPECT_EQ(out.first_party_identifier().id(), "test-id");
}

TEST(AssociatedDataConvertersTest, ToApiConvertsEmptyObject) {
  backend::AssociatedData in;
  api::v1::AssociatedData out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_FALSE(out.has_first_party_identifier());
}

TEST(AssociatedDataConvertersTest, ToApiConvertsFirstPartyIdentifier) {
  backend::AssociatedData in;
  in.mutable_first_party_identifier()->set_id("test-id");
  api::v1::AssociatedData out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_first_party_identifier());
  EXPECT_EQ(out.first_party_identifier().id(), "test-id");
}

}  // namespace google::confidential_match::match_service
