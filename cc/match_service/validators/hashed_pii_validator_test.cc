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

#include "cc/match_service/validators/hashed_pii_validator.h"

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/match_service/error/error.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::Error;
using ::testing::HasSubstr;
using ::testing::Not;

// SHA-256 of the empty string in the encodings accepted by IsValidHash.
constexpr char kBase64Hash[] = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
constexpr char kWebSafeBase64HashUnpadded[] =
    "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";
constexpr char kHexHash[] =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

void ExpectInvalidMatchKeyField(const absl::Status& status) {
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(GetBackendErrorReason(status), Error::INVALID_MATCH_KEY_FIELD);
}

void ExpectInvalidHashedValue(const absl::Status& status) {
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(GetBackendErrorReason(status), Error::INVALID_HASHED_VALUE);
}

TEST(HashedPiiValidatorTest, SingleFieldValidHashesAreValid) {
  for (const char* hash : {kBase64Hash, kWebSafeBase64HashUnpadded, kHexHash}) {
    EXPECT_THAT(ValidateHashedSingleField(hash), IsOk()) << hash;
  }
}

TEST(HashedPiiValidatorTest, SingleFieldPlaintextIsInvalid) {
  ExpectInvalidHashedValue(ValidateHashedSingleField("user@example.com"));
  ExpectInvalidHashedValue(ValidateHashedSingleField("+16505550123"));
}

TEST(HashedPiiValidatorTest, SingleFieldEmptyIsInvalid) {
  ExpectInvalidHashedValue(ValidateHashedSingleField(""));
}

TEST(HashedPiiValidatorTest, CompositeAddressValidHashedNamesAreValid) {
  std::vector<std::string> parts = {kBase64Hash, kHexHash, "US", "94043"};
  EXPECT_THAT(ValidateHashedAddressField(parts), IsOk());
}

TEST(HashedPiiValidatorTest, CompositeAddressEmptyNamesAreNotValidated) {
  std::vector<std::string> parts = {"", kBase64Hash, "US", "94043"};
  EXPECT_THAT(ValidateHashedAddressField(parts), IsOk());
}

TEST(HashedPiiValidatorTest, CompositeAddressPlaintextFirstNameIsInvalid) {
  std::vector<std::string> parts = {"John", kBase64Hash, "US", "94043"};
  absl::Status status = ValidateHashedAddressField(parts);
  ExpectInvalidHashedValue(status);
  EXPECT_THAT(status.message(), HasSubstr("first name"));
  EXPECT_THAT(status.message(), Not(HasSubstr("John")));
}

TEST(HashedPiiValidatorTest, CompositeAddressPlaintextLastNameIsInvalid) {
  std::vector<std::string> parts = {kBase64Hash, "Doe", "US", "94043"};
  absl::Status status = ValidateHashedAddressField(parts);
  ExpectInvalidHashedValue(status);
  EXPECT_THAT(status.message(), HasSubstr("last name"));
  EXPECT_THAT(status.message(), Not(HasSubstr("Doe")));
}

TEST(HashedPiiValidatorTest,
     CompositeAddressEmptyFirstNamePlaintextLastNameIsInvalid) {
  std::vector<std::string> parts = {"", "Doe", "US", "94043"};
  absl::Status status = ValidateHashedAddressField(parts);
  ExpectInvalidHashedValue(status);
  EXPECT_THAT(status.message(), HasSubstr("last name"));
}

TEST(HashedPiiValidatorTest, CompositeAddressPlaintextNamesReportFirstName) {
  std::vector<std::string> parts = {"John", "Doe", "US", "94043"};
  absl::Status status = ValidateHashedAddressField(parts);
  ExpectInvalidHashedValue(status);
  EXPECT_THAT(status.message(), HasSubstr("first name"));
}

TEST(HashedPiiValidatorTest, CompositeAddressWrongPartCountIsInvalid) {
  std::vector<std::string> parts = {kBase64Hash, kBase64Hash};
  ExpectInvalidMatchKeyField(ValidateHashedAddressField(parts));
}

}  // namespace
}  // namespace google::confidential_match::match_service
