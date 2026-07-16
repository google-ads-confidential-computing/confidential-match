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

#include "cc/match_service/converters/field_type_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(FieldTypeConvertersTest, ToBackendConvertsUnspecified) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_UNSPECIFIED;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_UNSPECIFIED);
}

TEST(FieldTypeConvertersTest, ToApiConvertsUnspecified) {
  backend::FieldType in = backend::FIELD_TYPE_UNSPECIFIED;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_UNSPECIFIED);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsEmail) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_EMAIL;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_EMAIL);
}

TEST(FieldTypeConvertersTest, ToApiConvertsEmail) {
  backend::FieldType in = backend::FIELD_TYPE_EMAIL;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_EMAIL);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsPhone) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_PHONE;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_PHONE);
}

TEST(FieldTypeConvertersTest, ToApiConvertsPhone) {
  backend::FieldType in = backend::FIELD_TYPE_PHONE;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_PHONE);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsFirstName) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_FIRST_NAME;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_FIRST_NAME);
}

TEST(FieldTypeConvertersTest, ToApiConvertsFirstName) {
  backend::FieldType in = backend::FIELD_TYPE_FIRST_NAME;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_FIRST_NAME);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsLastName) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_LAST_NAME;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_LAST_NAME);
}

TEST(FieldTypeConvertersTest, ToApiConvertsLastName) {
  backend::FieldType in = backend::FIELD_TYPE_LAST_NAME;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_LAST_NAME);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsCountryCode) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_COUNTRY_CODE;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_COUNTRY_CODE);
}

TEST(FieldTypeConvertersTest, ToApiConvertsCountryCode) {
  backend::FieldType in = backend::FIELD_TYPE_COUNTRY_CODE;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_COUNTRY_CODE);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsZipCode) {
  api::v1::FieldType in = api::v1::FIELD_TYPE_ZIP_CODE;
  backend::FieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FIELD_TYPE_ZIP_CODE);
}

TEST(FieldTypeConvertersTest, ToApiConvertsZipCode) {
  backend::FieldType in = backend::FIELD_TYPE_ZIP_CODE;
  api::v1::FieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FIELD_TYPE_ZIP_CODE);
}

TEST(FieldTypeConvertersTest, ToBackendFailsOnInvalid) {
  backend::FieldType out;
  absl::Status status = ToBackend(static_cast<api::v1::FieldType>(-1), out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(FieldTypeConvertersTest, ToApiFailsOnInvalid) {
  api::v1::FieldType out;
  absl::Status status = ToApi(static_cast<backend::FieldType>(-1), out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(FieldTypeConvertersTest, ToBackendConvertsCompositeUnspecified) {
  api::v1::CompositeFieldType in = api::v1::COMPOSITE_FIELD_TYPE_UNSPECIFIED;
  backend::CompositeFieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::COMPOSITE_FIELD_TYPE_UNSPECIFIED);
}

TEST(FieldTypeConvertersTest, ToApiConvertsCompositeUnspecified) {
  backend::CompositeFieldType in = backend::COMPOSITE_FIELD_TYPE_UNSPECIFIED;
  api::v1::CompositeFieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::COMPOSITE_FIELD_TYPE_UNSPECIFIED);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsAddress) {
  api::v1::CompositeFieldType in = api::v1::COMPOSITE_FIELD_TYPE_ADDRESS;
  backend::CompositeFieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::COMPOSITE_FIELD_TYPE_ADDRESS);
}

TEST(FieldTypeConvertersTest, ToApiConvertsAddress) {
  backend::CompositeFieldType in = backend::COMPOSITE_FIELD_TYPE_ADDRESS;
  api::v1::CompositeFieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::COMPOSITE_FIELD_TYPE_ADDRESS);
}

TEST(FieldTypeConvertersTest, ToBackendFailsOnInvalidComposite) {
  backend::CompositeFieldType out;
  absl::Status status =
      ToBackend(static_cast<api::v1::CompositeFieldType>(-1), out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(FieldTypeConvertersTest, ToApiFailsOnInvalidComposite) {
  api::v1::CompositeFieldType out;
  absl::Status status =
      ToApi(static_cast<backend::CompositeFieldType>(-1), out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(FieldTypeConvertersTest, ToBackendConvertsFanOutUnspecified) {
  api::v1::FanOutFieldType in = api::v1::FAN_OUT_FIELD_TYPE_UNSPECIFIED;
  backend::FanOutFieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FAN_OUT_FIELD_TYPE_UNSPECIFIED);
}

TEST(FieldTypeConvertersTest, ToApiConvertsFanOutUnspecified) {
  backend::FanOutFieldType in = backend::FAN_OUT_FIELD_TYPE_UNSPECIFIED;
  api::v1::FanOutFieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FAN_OUT_FIELD_TYPE_UNSPECIFIED);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsGtag) {
  api::v1::FanOutFieldType in = api::v1::FAN_OUT_FIELD_TYPE_GTAG;
  backend::FanOutFieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FAN_OUT_FIELD_TYPE_GTAG);
}

TEST(FieldTypeConvertersTest, ToApiConvertsGtag) {
  backend::FanOutFieldType in = backend::FAN_OUT_FIELD_TYPE_GTAG;
  api::v1::FanOutFieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FAN_OUT_FIELD_TYPE_GTAG);
}

TEST(FieldTypeConvertersTest, ToBackendConvertsEncryptedGtag) {
  api::v1::FanOutFieldType in = api::v1::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG;
  backend::FanOutFieldType out;
  absl::Status status = ToBackend(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG);
}

TEST(FieldTypeConvertersTest, ToApiConvertsEncryptedGtag) {
  backend::FanOutFieldType in = backend::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG;
  api::v1::FanOutFieldType out;
  absl::Status status = ToApi(in, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG);
}

TEST(FieldTypeConvertersTest, ToBackendFailsOnInvalidFanOut) {
  backend::FanOutFieldType out;
  absl::Status status =
      ToBackend(static_cast<api::v1::FanOutFieldType>(-1), out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(FieldTypeConvertersTest, ToApiFailsOnInvalidFanOut) {
  api::v1::FanOutFieldType out;
  absl::Status status = ToApi(static_cast<backend::FanOutFieldType>(-1), out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
