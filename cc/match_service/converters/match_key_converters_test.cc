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

#include "cc/match_service/converters/match_key_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"

#include "gtest/gtest.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(MatchKeyConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::MatchKey in;
  backend::MatchKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
}

TEST(MatchKeyConvertersTest, ToBackendConvertsField) {
  api::v1::MatchKey in;
  in.mutable_field()->set_type(api::v1::FIELD_TYPE_EMAIL);
  in.mutable_field()->set_value("test@google.com");
  backend::MatchKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field().type(), backend::FIELD_TYPE_EMAIL);
  EXPECT_EQ(out.field().value(), "test@google.com");
}

TEST(MatchKeyConvertersTest, ToBackendConvertsCompositeField) {
  api::v1::MatchKey in;
  in.mutable_composite_field()->set_type(api::v1::COMPOSITE_FIELD_TYPE_ADDRESS);
  backend::MatchKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.composite_field().type(),
            backend::COMPOSITE_FIELD_TYPE_ADDRESS);
}

TEST(MatchKeyConvertersTest, ToBackendConvertsFanOutField) {
  api::v1::MatchKey in;
  in.mutable_fan_out_key_field()->set_type(api::v1::FAN_OUT_FIELD_TYPE_GTAG);
  in.mutable_fan_out_key_field()->set_value("test_value");
  backend::MatchKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.fan_out_key_field().type(), backend::FAN_OUT_FIELD_TYPE_GTAG);
  EXPECT_EQ(out.fan_out_key_field().value(), "test_value");
}

TEST(MatchKeyConvertersTest, ToBackendConvertsEncryptionKey) {
  api::v1::MatchKey in;
  in.mutable_field()->set_type(api::v1::FIELD_TYPE_EMAIL);
  in.mutable_encryption_key()->mutable_coordinator_key()->set_key_id(
      "test_key_id");
  backend::MatchKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.encryption_key().coordinator_key().key_id(), "test_key_id");
}

TEST(MatchKeyConvertersTest, ToBackendConvertsMetadata) {
  api::v1::MatchKey in;
  in.mutable_field()->set_type(api::v1::FIELD_TYPE_EMAIL);
  api::v1::KeyValue* metadata = in.add_metadata();
  metadata->set_key("test_key");
  metadata->set_string_value("test_value");
  backend::MatchKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
  EXPECT_EQ(out.metadata(0).key(), "test_key");
  EXPECT_EQ(out.metadata(0).string_value(), "test_value");
}

TEST(MatchKeyConvertersTest, ToApiConvertsEmptyObject) {
  backend::MatchKey in;
  api::v1::MatchKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
}

TEST(MatchKeyConvertersTest, ToApiConvertsField) {
  backend::MatchKey in;
  in.mutable_field()->set_type(backend::FIELD_TYPE_PHONE);
  in.mutable_field()->set_value("+16505551234");
  api::v1::MatchKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field().type(), api::v1::FIELD_TYPE_PHONE);
  EXPECT_EQ(out.field().value(), "+16505551234");
}

TEST(MatchKeyConvertersTest, ToApiConvertsCompositeField) {
  backend::MatchKey in;
  in.mutable_composite_field()->set_type(backend::COMPOSITE_FIELD_TYPE_ADDRESS);
  api::v1::MatchKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.composite_field().type(),
            api::v1::COMPOSITE_FIELD_TYPE_ADDRESS);
}

TEST(MatchKeyConvertersTest, ToApiConvertsFanOutField) {
  backend::MatchKey in;
  in.mutable_fan_out_key_field()->set_type(backend::FAN_OUT_FIELD_TYPE_GTAG);
  in.mutable_fan_out_key_field()->set_value("test_value");
  api::v1::MatchKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.fan_out_key_field().type(), api::v1::FAN_OUT_FIELD_TYPE_GTAG);
  EXPECT_EQ(out.fan_out_key_field().value(), "test_value");
}

TEST(MatchKeyConvertersTest, ToApiConvertsEncryptionKey) {
  backend::MatchKey in;
  in.mutable_field()->set_type(backend::FIELD_TYPE_EMAIL);
  in.mutable_encryption_key()->mutable_coordinator_key()->set_key_id(
      "test_key_id");
  api::v1::MatchKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.encryption_key().coordinator_key().key_id(), "test_key_id");
}

TEST(MatchKeyConvertersTest, ToApiConvertsMetadata) {
  backend::MatchKey in;
  in.mutable_field()->set_type(backend::FIELD_TYPE_EMAIL);
  backend::KeyValue* metadata = in.add_metadata();
  metadata->set_key("test_key");
  metadata->set_string_value("test_value");
  api::v1::MatchKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
  EXPECT_EQ(out.metadata(0).key(), "test_key");
  EXPECT_EQ(out.metadata(0).string_value(), "test_value");
}

}  // namespace google::confidential_match::match_service
