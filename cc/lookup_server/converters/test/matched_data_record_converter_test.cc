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

#include "cc/lookup_server/converters/src/matched_data_record_converter.h"

#include "cc/public/core/interface/execution_result.h"
#include "include/gtest/gtest.h"

#include "cc/lookup_server/converters/src/key_value_converter.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_api::MatchedDataRecord;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

TEST(MatchedDataRecordConverterTest, ConvertToMatchedDataRecordWithEmptyProto) {
  MatchDataRow match_data_row;
  MatchedDataRecord matched_data_record;

  ExecutionResult result =
      ConvertToMatchedDataRecord(match_data_row, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "");
  EXPECT_EQ(matched_data_record.associated_data_size(), 0);
}

TEST(MatchedDataRecordConverterTest, ConvertToMatchedDataRecordWithKeyOnly) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  MatchedDataRecord matched_data_record;

  ExecutionResult result =
      ConvertToMatchedDataRecord(match_data_row, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 0);
}

TEST(MatchedDataRecordConverterTest,
     ConvertToMatchedDataRecordWithKeyAndMetadata) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  *match_data_row.add_associated_data()->mutable_key() = "data_key";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "value";
  *match_data_row.add_associated_data()->mutable_key() = "data_key2";
  match_data_row.mutable_associated_data(1)->set_int_value(2);
  MatchedDataRecord matched_data_record;

  ExecutionResult result =
      ConvertToMatchedDataRecord(match_data_row, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 2);
  EXPECT_EQ(matched_data_record.associated_data(0).key(), "data_key");
  EXPECT_EQ(matched_data_record.associated_data(0).string_value(), "value");
  EXPECT_EQ(matched_data_record.associated_data(1).key(), "data_key2");
  EXPECT_EQ(matched_data_record.associated_data(1).int_value(), 2);
}

TEST(MatchedDataRecordConverterTest,
     ConvertToMatchedDataRecordWithAssociatedDataKeysEmptyData) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  MatchedDataRecord matched_data_record;
  std::string data_key = "data_key";
  std::vector<std::string*> associated_data_keys = {&data_key};

  ExecutionResult result = ConvertToMatchedDataRecord(
      match_data_row, associated_data_keys, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 0);
}

TEST(MatchedDataRecordConverterTest,
     ConvertToMatchedDataRecordWithEmptyAssociatedDataKeys) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  *match_data_row.add_associated_data()->mutable_key() = "data_key";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "value";
  MatchedDataRecord matched_data_record;
  std::vector<std::string*> associated_data_keys;

  ExecutionResult result = ConvertToMatchedDataRecord(
      match_data_row, associated_data_keys, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 0);
}

TEST(MatchedDataRecordConverterTest,
     ConvertToMatchedDataRecordWithAssociatedDataKeysPartialMatch) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  *match_data_row.add_associated_data()->mutable_key() = "data_key";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "value";
  *match_data_row.add_associated_data()->mutable_key() = "data_key2";
  match_data_row.mutable_associated_data(1)->set_int_value(2);
  MatchedDataRecord matched_data_record;
  std::string present_key = "data_key";
  std::string nonexistent_key = "nonexistent_key";
  std::vector<std::string*> associated_data_keys = {&present_key,
                                                    &nonexistent_key};

  ExecutionResult result = ConvertToMatchedDataRecord(
      match_data_row, associated_data_keys, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 1);
  EXPECT_EQ(matched_data_record.associated_data(0).key(), "data_key");
  EXPECT_EQ(matched_data_record.associated_data(0).string_value(), "value");
}

TEST(MatchedDataRecordConverterTest,
     ConvertToMatchedDataRecordWithAssociatedDataKeysFullMatch) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  *match_data_row.add_associated_data()->mutable_key() = "data_key";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "value";
  *match_data_row.add_associated_data()->mutable_key() = "data_key2";
  match_data_row.mutable_associated_data(1)->set_int_value(2);
  MatchedDataRecord matched_data_record;
  std::string present_key = "data_key";
  std::string present_key_2 = "data_key2";
  std::vector<std::string*> associated_data_keys = {&present_key,
                                                    &present_key_2};

  ExecutionResult result = ConvertToMatchedDataRecord(
      match_data_row, associated_data_keys, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 2);
  EXPECT_EQ(matched_data_record.associated_data(0).key(), "data_key");
  EXPECT_EQ(matched_data_record.associated_data(0).string_value(), "value");
  EXPECT_EQ(matched_data_record.associated_data(1).key(), "data_key2");
  EXPECT_EQ(matched_data_record.associated_data(1).int_value(), 2);
}

TEST(MatchedDataRecordConverterTest,
     ConvertToMatchedDataRecordWithDuplicateAssociatedDataKeys) {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "hash";
  *match_data_row.add_associated_data()->mutable_key() = "duplicated_key";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "value";
  *match_data_row.add_associated_data()->mutable_key() = "duplicated_key";
  match_data_row.mutable_associated_data(1)->set_int_value(2);
  MatchedDataRecord matched_data_record;
  std::string data_key = "duplicated_key";
  std::vector<std::string*> associated_data_keys = {&data_key};

  ExecutionResult result = ConvertToMatchedDataRecord(
      match_data_row, associated_data_keys, matched_data_record);

  EXPECT_EQ(result, SuccessExecutionResult());
  EXPECT_EQ(matched_data_record.lookup_key().key(), "hash");
  EXPECT_EQ(matched_data_record.associated_data_size(), 2);
  EXPECT_EQ(matched_data_record.associated_data(0).key(), "duplicated_key");
  EXPECT_EQ(matched_data_record.associated_data(0).string_value(), "value");
  EXPECT_EQ(matched_data_record.associated_data(1).key(), "duplicated_key");
  EXPECT_EQ(matched_data_record.associated_data(1).int_value(), 2);
}

}  // namespace google::confidential_match::lookup_server
