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

#include "cc/match_service/converters/field_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(FieldConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::Field in;
  backend::Field out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), backend::FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.value(), "");
}

TEST(FieldConvertersTest, ToBackendConvertsType) {
  api::v1::Field in;
  in.set_type(api::v1::FIELD_TYPE_EMAIL);
  backend::Field out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), backend::FIELD_TYPE_EMAIL);
}

TEST(FieldConvertersTest, ToBackendConvertsValue) {
  api::v1::Field in;
  in.set_value("test@google.com");
  backend::Field out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.value(), "test@google.com");
}

TEST(FieldConvertersTest, ToApiConvertsEmptyObject) {
  backend::Field in;
  api::v1::Field out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), api::v1::FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.value(), "");
}

TEST(FieldConvertersTest, ToApiConvertsType) {
  backend::Field in;
  in.set_type(backend::FIELD_TYPE_PHONE);
  api::v1::Field out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), api::v1::FIELD_TYPE_PHONE);
}

TEST(FieldConvertersTest, ToApiConvertsValue) {
  backend::Field in;
  in.set_value("+16505551234");
  api::v1::Field out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.value(), "+16505551234");
}

TEST(FieldConvertersTest, ToBackendConvertsEmptyCompositeField) {
  api::v1::CompositeField in;
  backend::CompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), backend::COMPOSITE_FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.values_size(), 0);
}

TEST(FieldConvertersTest, ToBackendConvertsCompositeFieldType) {
  api::v1::CompositeField in;
  in.set_type(api::v1::COMPOSITE_FIELD_TYPE_ADDRESS);
  backend::CompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), backend::COMPOSITE_FIELD_TYPE_ADDRESS);
}

TEST(FieldConvertersTest, ToBackendConvertsCompositeFieldValues) {
  api::v1::CompositeField in;
  api::v1::Field* value = in.add_values();
  value->set_type(api::v1::FIELD_TYPE_FIRST_NAME);
  value->set_value("Larry");
  backend::CompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.values_size(), 1);
  EXPECT_EQ(out.values(0).type(), backend::FIELD_TYPE_FIRST_NAME);
  EXPECT_EQ(out.values(0).value(), "Larry");
}

TEST(FieldConvertersTest, ToApiConvertsEmptyCompositeField) {
  backend::CompositeField in;
  api::v1::CompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), api::v1::COMPOSITE_FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.values_size(), 0);
}

TEST(FieldConvertersTest, ToApiConvertsCompositeFieldType) {
  backend::CompositeField in;
  in.set_type(backend::COMPOSITE_FIELD_TYPE_ADDRESS);
  api::v1::CompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), api::v1::COMPOSITE_FIELD_TYPE_ADDRESS);
}

TEST(FieldConvertersTest, ToApiConvertsCompositeFieldValues) {
  backend::CompositeField in;
  backend::Field* value = in.add_values();
  value->set_type(backend::FIELD_TYPE_LAST_NAME);
  value->set_value("Page");
  api::v1::CompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.values_size(), 1);
  EXPECT_EQ(out.values(0).type(), api::v1::FIELD_TYPE_LAST_NAME);
  EXPECT_EQ(out.values(0).value(), "Page");
}

TEST(FieldConvertersTest, ToBackendConvertsEmptyFanOutField) {
  api::v1::FanOutField in;
  backend::FanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), backend::FAN_OUT_FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.value(), "");
}

TEST(FieldConvertersTest, ToBackendConvertsFanOutFieldType) {
  api::v1::FanOutField in;
  in.set_type(api::v1::FAN_OUT_FIELD_TYPE_GTAG);
  backend::FanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), backend::FAN_OUT_FIELD_TYPE_GTAG);
}

TEST(FieldConvertersTest, ToBackendConvertsFanOutFieldValue) {
  api::v1::FanOutField in;
  in.set_value("test_value");
  backend::FanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.value(), "test_value");
}

TEST(FieldConvertersTest, ToApiConvertsEmptyFanOutField) {
  backend::FanOutField in;
  api::v1::FanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), api::v1::FAN_OUT_FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.value(), "");
}

TEST(FieldConvertersTest, ToApiConvertsFanOutFieldType) {
  backend::FanOutField in;
  in.set_type(backend::FAN_OUT_FIELD_TYPE_GTAG);
  api::v1::FanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.type(), api::v1::FAN_OUT_FIELD_TYPE_GTAG);
}

TEST(FieldConvertersTest, ToApiConvertsFanOutFieldValue) {
  backend::FanOutField in;
  in.set_value("test_value");
  api::v1::FanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.value(), "test_value");
}

}  // namespace google::confidential_match::match_service
