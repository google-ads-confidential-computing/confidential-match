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

#include "cc/match_service/converters/key_value_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(KeyValueConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::KeyValue in;
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key(), "");
  EXPECT_EQ(out.value_case(), backend::KeyValue::VALUE_NOT_SET);
}

TEST(KeyValueConvertersTest, ToBackendConvertsKey) {
  api::v1::KeyValue in;
  in.set_key("sample_key");
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key(), "sample_key");
}

TEST(KeyValueConvertersTest, ToBackendConvertsStringValue) {
  api::v1::KeyValue in;
  in.set_string_value("string");
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.string_value(), "string");
}

TEST(KeyValueConvertersTest, ToBackendConvertsIntValue) {
  api::v1::KeyValue in;
  in.set_int_value(1);
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.int_value(), 1);
}

TEST(KeyValueConvertersTest, ToBackendConvertsDoubleValue) {
  api::v1::KeyValue in;
  in.set_double_value(1.0);
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.double_value(), 1.0);
}

TEST(KeyValueConvertersTest, ToBackendConvertsBoolValue) {
  api::v1::KeyValue in;
  in.set_bool_value(true);
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.bool_value());
}

TEST(KeyValueConvertersTest, ToBackendConvertsBytesValue) {
  api::v1::KeyValue in;
  in.set_bytes_value("bytes");
  backend::KeyValue out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.bytes_value(), "bytes");
}

TEST(KeyValueConvertersTest, ToApiConvertsEmptyObject) {
  backend::KeyValue in;
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key(), "");
  EXPECT_EQ(out.value_case(), api::v1::KeyValue::VALUE_NOT_SET);
}

TEST(KeyValueConvertersTest, ToApiConvertsKey) {
  backend::KeyValue in;
  in.set_key("sample_key");
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key(), "sample_key");
}

TEST(KeyValueConvertersTest, ToApiConvertsStringValue) {
  backend::KeyValue in;
  in.set_string_value("string");
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.string_value(), "string");
}

TEST(KeyValueConvertersTest, ToApiConvertsIntValue) {
  backend::KeyValue in;
  in.set_int_value(1);
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.int_value(), 1);
}

TEST(KeyValueConvertersTest, ToApiConvertsDoubleValue) {
  backend::KeyValue in;
  in.set_double_value(1.0);
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.double_value(), 1.0);
}

TEST(KeyValueConvertersTest, ToApiConvertsBoolValue) {
  backend::KeyValue in;
  in.set_bool_value(true);
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.bool_value());
}

TEST(KeyValueConvertersTest, ToApiConvertsBytesValue) {
  backend::KeyValue in;
  in.set_bytes_value("bytes");
  api::v1::KeyValue out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.bytes_value(), "bytes");
}

}  // namespace google::confidential_match::match_service
