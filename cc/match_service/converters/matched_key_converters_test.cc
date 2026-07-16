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

#include "cc/match_service/converters/matched_key_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(MatchedKeyConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::MatchedKey in;
  backend::MatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_info_case(), backend::MatchedKey::FIELD_INFO_NOT_SET);
  EXPECT_EQ(out.metadata_size(), 0);
}

TEST(MatchedKeyConvertersTest, ToBackendConvertsField) {
  api::v1::MatchedKey in;
  in.mutable_field()->set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field().status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedKeyConvertersTest, ToBackendConvertsCompositeField) {
  api::v1::MatchedKey in;
  in.mutable_composite_field()->set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.composite_field().status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedKeyConvertersTest, ToBackendConvertsFanOutKeyField) {
  api::v1::MatchedKey in;
  in.mutable_fan_out_key_field()->set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.fan_out_key_field().status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedKeyConvertersTest, ToBackendConvertsMetadata) {
  api::v1::MatchedKey in;
  in.add_metadata()->set_string_value("value");
  backend::MatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
  EXPECT_EQ(out.metadata(0).string_value(), "value");
}

TEST(MatchedKeyConvertersTest, ToApiConvertsEmptyObject) {
  backend::MatchedKey in;
  api::v1::MatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_info_case(), api::v1::MatchedKey::FIELD_INFO_NOT_SET);
  EXPECT_EQ(out.metadata_size(), 0);
}

TEST(MatchedKeyConvertersTest, ToApiConvertsField) {
  backend::MatchedKey in;
  in.mutable_field()->set_status(backend::STATUS_SUCCESS_UNMATCHED);
  api::v1::MatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field().status(), api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(MatchedKeyConvertersTest, ToApiConvertsCompositeField) {
  backend::MatchedKey in;
  in.mutable_composite_field()->set_status(backend::STATUS_SUCCESS_UNMATCHED);
  api::v1::MatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.composite_field().status(), api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(MatchedKeyConvertersTest, ToApiConvertsFanOutKeyField) {
  backend::MatchedKey in;
  in.mutable_fan_out_key_field()->set_status(backend::STATUS_SUCCESS_UNMATCHED);
  api::v1::MatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.fan_out_key_field().status(),
            api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(MatchedKeyConvertersTest, ToApiConvertsMetadata) {
  backend::MatchedKey in;
  in.add_metadata()->set_int_value(123);
  api::v1::MatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
  EXPECT_EQ(out.metadata(0).int_value(), 123);
}

}  // namespace google::confidential_match::match_service
