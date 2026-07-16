// Copyright 2026 Google LLC
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

#include "cc/match_service/validators/match_key_encoding_validator.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(MatchKeyEncodingValidatorTest,
     ValidateMatchKeyEncodingUnspecifiedReturnsError) {
  EXPECT_THAT(ValidateMatchKeyEncoding(backend::MATCH_KEY_ENCODING_UNSPECIFIED,
                                       "example"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(MatchKeyEncodingValidatorTest, ValidateMatchKeyEncodingBase64Valid) {
  EXPECT_THAT(
      ValidateMatchKeyEncoding(backend::MATCH_KEY_ENCODING_BASE64, "Pl8+"),
      IsOk());
}

TEST(MatchKeyEncodingValidatorTest, ValidateMatchKeyEncodingBase64Invalid) {
  EXPECT_THAT(
      ValidateMatchKeyEncoding(backend::MATCH_KEY_ENCODING_BASE64, "Pl8-"),
      StatusIs(StatusCode::kInvalidArgument));
}

TEST(MatchKeyEncodingValidatorTest,
     ValidateMatchKeyEncodingBase64WebSafeValid) {
  EXPECT_THAT(ValidateMatchKeyEncoding(
                  backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE, "Pl8-"),
              IsOk());
}

TEST(MatchKeyEncodingValidatorTest,
     ValidateMatchKeyEncodingBase64WebSafeInvalid) {
  EXPECT_THAT(ValidateMatchKeyEncoding(
                  backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE, "Pl8+"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST(MatchKeyEncodingValidatorTest, ValidateMatchKeyEncodingHexValid) {
  EXPECT_THAT(
      ValidateMatchKeyEncoding(backend::MATCH_KEY_ENCODING_HEX, "90091e"),
      IsOk());
}

TEST(MatchKeyEncodingValidatorTest, ValidateMatchKeyEncodingHexInvalid) {
  EXPECT_THAT(
      ValidateMatchKeyEncoding(backend::MATCH_KEY_ENCODING_HEX, "google"),
      StatusIs(StatusCode::kInvalidArgument));
}

TEST(MatchKeyEncodingValidatorTest,
     ValidateMatchKeyEncodingUnknownEncodingInvalid) {
  EXPECT_THAT(ValidateMatchKeyEncoding(
                  static_cast<backend::MatchKeyEncoding>(-1), "example"),
              StatusIs(StatusCode::kInternal));
}

}  // namespace google::confidential_match::match_service
