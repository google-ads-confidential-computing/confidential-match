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

#include "cc/core/data_table/src/data_table.h"

#include <string>
#include <vector>

#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "include/gtest/gtest.h"
#include "public/core/interface/execution_result.h"

#include "cc/core/data_table/src/error_codes.h"
#include "protos/core/data_value.pb.h"

namespace google::confidential_match {

using ::google::confidential_match::DataValue;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::ResultIs;

// Sample hash key with length of 32 characters when base-64 decoded.
constexpr absl::string_view kHash = "example-hash-string-of-length-32";
// Another sample hash key with length of 32 characters when base-64 decoded.
constexpr absl::string_view hKash2 = "another-hash-string-of-length-32";
// Sample hash key with invalid length.
constexpr absl::string_view kHashKeyInvalidLength = "short-hash-string";

class DataTableTest : public testing::Test {
 protected:
  DataTable data_table_;
};

TEST_F(DataTableTest, InsertSingleReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);

  ExecutionResult result = data_table_.Insert(kHash, data_value);

  EXPECT_SUCCESS(result);
}

TEST_F(DataTableTest, InsertMultipleReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  DataValue data_value2;
  data_value2.CopyFrom(data_value);
  data_value2.mutable_associated_data(1)->set_int_value(2);

  ExecutionResult data_value_result = data_table_.Insert(kHash, data_value);
  ExecutionResult data_value2_result = data_table_.Insert(kHash, data_value2);

  EXPECT_SUCCESS(data_value_result);
  EXPECT_SUCCESS(data_value2_result);
}

TEST_F(DataTableTest, InsertDuplicateReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  DataValue duplicate;
  duplicate.CopyFrom(data_value);

  ExecutionResult data_value_result = data_table_.Insert(kHash, data_value);
  ExecutionResult duplicate_result = data_table_.Insert(kHash, duplicate);

  EXPECT_SUCCESS(data_value_result);
  EXPECT_THAT(
      duplicate_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_ALREADY_EXISTS)));
}

TEST_F(DataTableTest, InsertHashKeyInvalidLengthReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);

  ExecutionResult result =
      data_table_.Insert(kHashKeyInvalidLength, data_value);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE)));
}

TEST_F(DataTableTest, FindExistingRecordReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].associated_data(0).key(), "type");
  EXPECT_EQ(out[0].associated_data(0).string_value(), "email");
  EXPECT_EQ(out[0].associated_data(1).key(), "user_id");
  EXPECT_EQ(out[0].associated_data(1).int_value(), 1);
}

TEST_F(DataTableTest, FindNonexistentRecordReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(hKash2, out);

  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(DataTableTest, FindNonexistentHashKeyInvalidLengthReturnsError) {
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHashKeyInvalidLength, out);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(DataTableTest, FindMultipleRecordsSameHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "type";
  *data_value2.mutable_associated_data(0)->mutable_string_value() = "phone";
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(1)->set_int_value(2);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value2).Successful());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_EQ(out.size(), 2);
  EXPECT_EQ(out[0].associated_data(0).key(), "type");
  EXPECT_EQ(out[0].associated_data(0).string_value(), "email");
  EXPECT_EQ(out[0].associated_data(1).key(), "user_id");
  EXPECT_EQ(out[0].associated_data(1).int_value(), 1);
  EXPECT_EQ(out[1].associated_data(0).key(), "type");
  EXPECT_EQ(out[1].associated_data(0).string_value(), "phone");
  EXPECT_EQ(out[1].associated_data(1).key(), "user_id");
  EXPECT_EQ(out[1].associated_data(1).int_value(), 2);
}

TEST_F(DataTableTest, FindMultipleRecordsDifferentHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "type";
  *data_value2.mutable_associated_data(0)->mutable_string_value() = "phone";
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(1)->set_int_value(2);
  EXPECT_TRUE(data_table_.Insert(hKash2, data_value2).Successful());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].associated_data(0).key(), "type");
  EXPECT_EQ(out[0].associated_data(0).string_value(), "email");
  EXPECT_EQ(out[0].associated_data(1).key(), "user_id");
  EXPECT_EQ(out[0].associated_data(1).int_value(), 1);
}

TEST_F(DataTableTest, EraseAllReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "type";
  *data_value2.mutable_associated_data(0)->mutable_string_value() = "phone";
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(1)->set_int_value(2);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value2).Successful());

  ExecutionResult erase_result = data_table_.Erase(kHash);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_EQ(data_table_.Find(kHash, out),
            FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST));
  EXPECT_EQ(out.size(), 0);
}

TEST_F(DataTableTest, EraseNonexistentReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());

  ExecutionResult erase_result = data_table_.Erase(hKash2);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(DataTableTest, EraseNonexistentHashKeyInvalidLengthReturnsError) {
  ExecutionResult erase_result = data_table_.Erase(kHashKeyInvalidLength);
  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(DataTableTest, EraseRecordReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "type";
  *data_value2.mutable_associated_data(0)->mutable_string_value() = "phone";
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(1)->set_int_value(2);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value2).Successful());

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_EQ(data_table_.Find(kHash, out), SuccessExecutionResult());
  EXPECT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].associated_data(1).int_value(), 2);
}

TEST_F(DataTableTest, EraseRecordNonexistentReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "type";
  *data_value.mutable_associated_data(0)->mutable_string_value() = "email";
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(1)->set_int_value(1);
  EXPECT_TRUE(data_table_.Insert(kHash, data_value).Successful());
  DataValue nonexistent_record;
  *nonexistent_record.add_associated_data()->mutable_key() = "type";
  *nonexistent_record.mutable_associated_data(0)->mutable_string_value() =
      "phone";
  *nonexistent_record.add_associated_data()->mutable_key() = "user_id";
  nonexistent_record.mutable_associated_data(1)->set_int_value(2);

  ExecutionResult erase_result = data_table_.Erase(kHash, nonexistent_record);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

}  // namespace google::confidential_match
