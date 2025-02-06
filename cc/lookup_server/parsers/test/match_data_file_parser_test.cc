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

#include "cc/lookup_server/parsers/src/match_data_file_parser.h"

#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/crypto_client/mock/fake_crypto_key.h"
#include "cc/lookup_server/crypto_client/mock/mock_crypto_key.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/parsers/src/error_codes.h"
#include "cc/lookup_server/types/match_data_group.h"
#include "protos/lookup_server/backend/exporter_data_row.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    ExporterDataRow;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::testing::ElementsAre;
using ::testing::InvokeArgument;
using ::testing::IsEmpty;
using ::testing::Return;
using ::testing::UnorderedElementsAre;

// Serialized ExporterDataRow containing the following data:
//
// data_key: "key"
// associated_data {
//   metadata {
//     key: "type"
//     string_value: "P"
//   }
//   metadata {
//     key: "encrypted_user_id"
//     bytes_value: "user_id"
//   }
// }
constexpr absl::string_view kSerializedData =
    "CgNrZXkSKQoJCgR0eXBlEgFQChwKEWVuY3J5cHRlZF91c2VyX2lkMgd1c2VyX2lk";

// Serialized ExporterDataRow containing the following data:
//
// data_key: "key2"
// associated_data {
//   metadata {
//     key: "type"
//     string_value: "E"
//   }
//   metadata {
//     key: "encrypted_user_id"
//     bytes_value: "user_id"
//   }
// }
// associated_data {
//   metadata {
//     key: "type"
//     string_value: "E"
//   }
//   metadata {
//     key: "encrypted_user_id"
//     bytes_value: "user_id2"
//   }
// }
constexpr absl::string_view kSerializedData2 =
    "CgRrZXkyEikKCQoEdHlwZRIBRQocChFlbmNyeXB0ZWRfdXNlcl9pZDIHdXNlcl9pZBIqCgkKBH"
    "R5cGUSAUUKHQoRZW5jcnlwdGVkX3VzZXJfaWQyCHVzZXJfaWQy";

// Returns a match data row corresponding to the record stored within
// kSerializedData.
MatchDataRow GetExpectedMatchDataRow() {
  MatchDataRow match_data_row;
  match_data_row.set_key("key");
  match_data_row.add_associated_data()->set_key("type");
  match_data_row.mutable_associated_data(0)->set_string_value("P");
  match_data_row.add_associated_data()->set_key("encrypted_user_id");
  match_data_row.mutable_associated_data(1)->set_bytes_value("user_id");
  return match_data_row;
}

// Returns a match data row corresponding to the first record within
// kSerializedData2.
MatchDataRow GetExpectedMatchDataRow2a() {
  MatchDataRow match_data_row;
  match_data_row.set_key("key2");
  match_data_row.add_associated_data()->set_key("type");
  match_data_row.mutable_associated_data(0)->set_string_value("E");
  match_data_row.add_associated_data()->set_key("encrypted_user_id");
  match_data_row.mutable_associated_data(1)->set_bytes_value("user_id");
  return match_data_row;
}

// Returns a match data row corresponding to the second record within
// kSerializedData2.
MatchDataRow GetExpectedMatchDataRow2b() {
  MatchDataRow match_data_row;
  match_data_row.set_key("key2");
  match_data_row.add_associated_data()->set_key("type");
  match_data_row.mutable_associated_data(0)->set_string_value("E");
  match_data_row.add_associated_data()->set_key("encrypted_user_id");
  match_data_row.mutable_associated_data(1)->set_bytes_value("user_id2");
  return match_data_row;
}

// Helper method to generate serialized data for an exporter data row
// with no encryption applied.
[[maybe_unused]] std::string GenerateSerializedData(
    const ExporterDataRow& exporter_data_row) {
  std::string serialized = exporter_data_row.SerializeAsString();
  return absl::Base64Escape(serialized);
}

}  // namespace

class MatchDataFileParserTest : public testing::Test {
 protected:
  MatchDataFileParserTest()
      : data_encryption_key_(std::make_shared<FakeCryptoKey>()) {}

  std::shared_ptr<CryptoKeyInterface> data_encryption_key_;
};

TEST_F(MatchDataFileParserTest, ParseMatchDataFileSingleRowWithoutDelimiter) {
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result = ParseMatchDataFile(
      kSerializedData, data_encryption_key_, match_data_groups);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(match_data_groups,
              ElementsAre(ElementsAre(EqualsProto(GetExpectedMatchDataRow()))));
}

TEST_F(MatchDataFileParserTest,
       ParseMatchDataFileSingleRowWithTrailingDelimiter) {
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result =
      ParseMatchDataFile(absl::StrCat(kSerializedData, "\n"),
                         data_encryption_key_, match_data_groups);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(match_data_groups,
              ElementsAre(ElementsAre(EqualsProto(GetExpectedMatchDataRow()))));
}

TEST_F(MatchDataFileParserTest, ParseMatchDataFileInvalidEncoding) {
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result = ParseMatchDataFile(
      "invalid-base64!", data_encryption_key_, match_data_groups);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(PARSER_INVALID_BASE64_DATA)));
  EXPECT_THAT(match_data_groups, IsEmpty());
}

TEST_F(MatchDataFileParserTest, ParseMatchDataFileInvalidProto) {
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result = ParseMatchDataFile(
      "InvalidProto", data_encryption_key_, match_data_groups);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(PARSER_INVALID_PROTO_DATA)));
  EXPECT_THAT(match_data_groups, IsEmpty());
}

TEST_F(MatchDataFileParserTest, ParseMatchDataFileDecryptionError) {
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result =
      ParseMatchDataFile(kSerializedData, mock_crypto_key, match_data_groups);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(PARSER_DATA_DECRYPTION_FAILED)));
  EXPECT_THAT(match_data_groups, IsEmpty());
}

TEST_F(MatchDataFileParserTest, ParseMatchDataFileMultipleRows) {
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result =
      ParseMatchDataFile(absl::StrCat(kSerializedData, "\n", kSerializedData2),
                         data_encryption_key_, match_data_groups);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(
      match_data_groups,
      UnorderedElementsAre(
          UnorderedElementsAre(EqualsProto(GetExpectedMatchDataRow())),
          UnorderedElementsAre(EqualsProto(GetExpectedMatchDataRow2a()),
                               EqualsProto(GetExpectedMatchDataRow2b()))));
}

TEST_F(MatchDataFileParserTest,
       ParseMatchDataFileMultipleRowsWithTrailingDelimiter) {
  std::vector<MatchDataGroup> match_data_groups;

  ExecutionResult result = ParseMatchDataFile(
      absl::StrCat(kSerializedData, "\n", kSerializedData2, "\n"),
      data_encryption_key_, match_data_groups);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(
      match_data_groups,
      UnorderedElementsAre(
          UnorderedElementsAre(EqualsProto(GetExpectedMatchDataRow())),
          UnorderedElementsAre(EqualsProto(GetExpectedMatchDataRow2a()),
                               EqualsProto(GetExpectedMatchDataRow2b()))));
}

}  // namespace google::confidential_match::lookup_server
