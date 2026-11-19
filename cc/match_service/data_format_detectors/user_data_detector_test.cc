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

#include "cc/match_service/data_format_detectors/user_data_detector.h"

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/match_service/error/error.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/detector.h"
#include "gtest/gtest.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::cpio::UserDataFormat;

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

class UserDataDetectorTest : public ::testing::Test {
 protected:
  UserDataDetector detector_;
};

TEST_F(UserDataDetectorTest, SingleFieldHashesAreHashed) {
  for (const char* hash : {kBase64Hash, kWebSafeBase64HashUnpadded, kHexHash}) {
    EXPECT_THAT(detector_.DetectSingleField(backend::FIELD_TYPE_EMAIL, hash),
                IsOkAndHolds(UserDataFormat::kHashed))
        << hash;
    EXPECT_THAT(detector_.DetectSingleField(backend::FIELD_TYPE_PHONE, hash),
                IsOkAndHolds(UserDataFormat::kHashed))
        << hash;
  }
}

TEST_F(UserDataDetectorTest, SingleFieldPlaintextEmailIsUnhashed) {
  EXPECT_THAT(detector_.DetectSingleField(backend::FIELD_TYPE_EMAIL,
                                          "user@example.com"),
              IsOkAndHolds(UserDataFormat::kUnhashed));
}

TEST_F(UserDataDetectorTest, SingleFieldMalformedEmailIsUnknown) {
  EXPECT_THAT(
      detector_.DetectSingleField(backend::FIELD_TYPE_EMAIL, "not-an-email"),
      IsOkAndHolds(UserDataFormat::kUnknown));
}

TEST_F(UserDataDetectorTest, SingleFieldPlaintextPhoneIsUnhashed) {
  EXPECT_THAT(
      detector_.DetectSingleField(backend::FIELD_TYPE_PHONE, "+16505550123"),
      IsOkAndHolds(UserDataFormat::kUnhashed));
}

TEST_F(UserDataDetectorTest, SingleFieldPhoneWithoutDigitsIsUnknown) {
  EXPECT_THAT(detector_.DetectSingleField(backend::FIELD_TYPE_PHONE, "abc"),
              IsOkAndHolds(UserDataFormat::kUnknown));
}

TEST_F(UserDataDetectorTest, SingleFieldUnsupportedTypeIsInvalid) {
  ExpectInvalidMatchKeyField(
      detector_.DetectSingleField(backend::FIELD_TYPE_FIRST_NAME, kBase64Hash)
          .status());
}

TEST_F(UserDataDetectorTest, CompositeAddressHashedNamesAreHashed) {
  std::vector<std::string> parts = {kBase64Hash, kBase64Hash, "US", "94043"};
  EXPECT_THAT(detector_.DetectCompositeAddress(parts),
              IsOkAndHolds(UserDataFormat::kHashed));
}

TEST_F(UserDataDetectorTest,
       CompositeAddressHashesOfDifferentLengthsAreUnknown) {
  std::vector<std::string> parts = {kBase64Hash, kHexHash, "US", "94043"};
  EXPECT_THAT(detector_.DetectCompositeAddress(parts),
              IsOkAndHolds(UserDataFormat::kUnknown));
}

TEST_F(UserDataDetectorTest, CompositeAddressPlaintextNameIsUnhashed) {
  std::vector<std::string> parts = {"John", kBase64Hash, "US", "94043"};
  EXPECT_THAT(detector_.DetectCompositeAddress(parts),
              IsOkAndHolds(UserDataFormat::kUnhashed));
}

TEST_F(UserDataDetectorTest, CompositeAddressWithOnlyNamesIsDetected) {
  std::vector<std::string> parts = {kBase64Hash, kBase64Hash};
  EXPECT_THAT(detector_.DetectCompositeAddress(parts),
              IsOkAndHolds(UserDataFormat::kHashed));
}

TEST_F(UserDataDetectorTest, CompositeAddressWithoutLastNameIsInvalid) {
  std::vector<std::string> parts = {kBase64Hash};
  ExpectInvalidMatchKeyField(detector_.DetectCompositeAddress(parts).status());
}

TEST_F(UserDataDetectorTest, CompositeAddressWithNoPartsIsInvalid) {
  std::vector<std::string> parts;
  ExpectInvalidMatchKeyField(detector_.DetectCompositeAddress(parts).status());
}

}  // namespace
}  // namespace google::confidential_match::match_service
