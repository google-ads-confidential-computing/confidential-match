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

#include "cc/match_service/converters/match_key_encoding_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(MatchKeyEncodingConvertersTest, ToBackendConvertsUnspecified) {
  api::v1::MatchKeyEncoding in = api::v1::MATCH_KEY_ENCODING_UNSPECIFIED;
  backend::MatchKeyEncoding out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::MATCH_KEY_ENCODING_UNSPECIFIED);
}

TEST(MatchKeyEncodingConvertersTest, ToBackendConvertsBase64) {
  api::v1::MatchKeyEncoding in = api::v1::MATCH_KEY_ENCODING_BASE64;
  backend::MatchKeyEncoding out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::MATCH_KEY_ENCODING_BASE64);
}

TEST(MatchKeyEncodingConvertersTest, ToBackendConvertsBase64WebSafe) {
  api::v1::MatchKeyEncoding in = api::v1::MATCH_KEY_ENCODING_BASE64_WEB_SAFE;
  backend::MatchKeyEncoding out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE);
}

TEST(MatchKeyEncodingConvertersTest, ToBackendConvertsHex) {
  api::v1::MatchKeyEncoding in = api::v1::MATCH_KEY_ENCODING_HEX;
  backend::MatchKeyEncoding out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::MATCH_KEY_ENCODING_HEX);
}

TEST(MatchKeyEncodingConvertersTest, ToBackendFailsOnInvalidInput) {
  api::v1::MatchKeyEncoding in = static_cast<api::v1::MatchKeyEncoding>(-1);
  backend::MatchKeyEncoding out;
  EXPECT_THAT(ToBackend(in, out), StatusIs(StatusCode::kInvalidArgument));
}

TEST(MatchKeyEncodingConvertersTest, ToApiConvertsUnspecified) {
  backend::MatchKeyEncoding in = backend::MATCH_KEY_ENCODING_UNSPECIFIED;
  api::v1::MatchKeyEncoding out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::MATCH_KEY_ENCODING_UNSPECIFIED);
}

TEST(MatchKeyEncodingConvertersTest, ToApiConvertsBase64) {
  backend::MatchKeyEncoding in = backend::MATCH_KEY_ENCODING_BASE64;
  api::v1::MatchKeyEncoding out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::MATCH_KEY_ENCODING_BASE64);
}

TEST(MatchKeyEncodingConvertersTest, ToApiConvertsBase64WebSafe) {
  backend::MatchKeyEncoding in = backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE;
  api::v1::MatchKeyEncoding out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::MATCH_KEY_ENCODING_BASE64_WEB_SAFE);
}

TEST(MatchKeyEncodingConvertersTest, ToApiConvertsHex) {
  backend::MatchKeyEncoding in = backend::MATCH_KEY_ENCODING_HEX;
  api::v1::MatchKeyEncoding out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::MATCH_KEY_ENCODING_HEX);
}

TEST(MatchKeyEncodingConvertersTest, ToApiFailsOnInvalidInput) {
  backend::MatchKeyEncoding in = static_cast<backend::MatchKeyEncoding>(-1);
  api::v1::MatchKeyEncoding out;
  EXPECT_THAT(ToApi(in, out), StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
