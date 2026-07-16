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

#include "cc/match_service/converters/lookup_associated_data_converter.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"

#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::AssociatedData;
using ::google::confidential_match::match_service::backend::AssociatedDataType;
using ::google::confidential_match::match_service::backend::KeyValue;
using ::google::confidential_match::match_service::backend::
    LookupMatchedDataRecord;
using ::google::protobuf::TextFormat;
using ::google::scp::core::test::EqualsProto;

TEST(LookupAssociatedDataConvertersTest, ToLookupConvertsFirstPartyIdentifier) {
  std::string out;
  absl::Status status = ToLookup(
      AssociatedDataType::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER, out);
  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, "encrypted_gaia_id");
}

TEST(LookupAssociatedDataConvertersTest, ToLookupConvertsUnspecified) {
  std::string out;
  absl::Status status =
      ToLookup(AssociatedDataType::ASSOCIATED_DATA_TYPE_UNSPECIFIED, out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(LookupAssociatedDataConvertersTest, ToLookupConvertsPrivacyInfo) {
  std::string out;
  absl::Status status =
      ToLookup(AssociatedDataType::ASSOCIATED_DATA_TYPE_PRIVACY_INFO, out);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(LookupAssociatedDataConvertersTest,
     ToMatchServiceConvertsFirstPartyIdentifier) {
  LookupMatchedDataRecord lookup_record;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        associated_data {
          key: "encrypted_gaia_id"
          bytes_value: "encrypted_gaia_bytes"
        }
      )pb",
      &lookup_record));
  AssociatedData out;

  absl::Status status = ToMatchService(lookup_record.associated_data(), out);

  EXPECT_THAT(status, IsOk());
  AssociatedData expected_associated_data;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        first_party_identifier { id: "ZW5jcnlwdGVkX2dhaWFfYnl0ZXM=" }
      )pb",
      &expected_associated_data));
  EXPECT_THAT(out, EqualsProto(expected_associated_data));
}

TEST(LookupAssociatedDataConvertersTest,
     ToMatchServiceWithMultipleFirstPartyIdentifiersReturnsError) {
  LookupMatchedDataRecord lookup_record;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        associated_data {
          key: "encrypted_gaia_id"
          bytes_value: "encrypted_gaia_bytes"
        }
        associated_data {
          key: "encrypted_gaia_id"
          bytes_value: "encrypted_gaia_bytes2"
        }
      )pb",
      &lookup_record));
  AssociatedData out;

  absl::Status status = ToMatchService(lookup_record.associated_data(), out);

  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(LookupAssociatedDataConvertersTest,
     ToMatchServiceConvertsUnsupportedAssociatedDataKey) {
  LookupMatchedDataRecord lookup_record;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        associated_data { key: "unsupported" int_value: -1 }
      )pb",
      &lookup_record));
  AssociatedData out;

  absl::Status status = ToMatchService(lookup_record.associated_data(), out);

  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
