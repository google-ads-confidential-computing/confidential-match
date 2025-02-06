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

#include "cc/core/data_table/src/internal/marked_data_table_storage.h"

#include <string>
#include <vector>

#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "include/gtest/gtest.h"
#include "public/core/interface/execution_result.h"

#include "cc/core/data_table/src/error_codes.h"
#include "protos/core/data_value.pb.h"
#include "protos/core/data_value_internal.pb.h"

namespace google::confidential_match {

using ::google::confidential_match::DataValue;
using ::google::confidential_match::DataValueInternal;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::testing::AllOf;
using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

// Sample hash key with length of 32 characters when base-64 decoded.
constexpr absl::string_view kHash = "example-hash-string-of-length-32";
// Another sample hash key with length of 32 characters when base-64 decoded.
constexpr absl::string_view kHash2 = "another-hash-string-of-length-32";
// Sample hash key with invalid length.
constexpr absl::string_view kHashKeyInvalidLength = "short-hash-string";

class MarkedDataTableStorageTest : public testing::Test {
 protected:
  MarkedDataTableStorage data_table_;
};

TEST_F(MarkedDataTableStorageTest, InsertSingleReturnsSuccess) {
  DataValue data_value;
  data_value.add_associated_data()->set_key("user_id");
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);

  ExecutionResult result = data_table_.Insert(kHash, data_value_internal);

  EXPECT_SUCCESS(result);
}

TEST_F(MarkedDataTableStorageTest, InsertMultipleReturnsSuccess) {
  DataValue data_value;
  data_value.add_associated_data()->set_key("user_id");
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);

  DataValue data_value2;
  data_value2.add_associated_data()->set_key("user_id");
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value;
  data_value_internal2.set_version_id(2);

  ExecutionResult data_value_result =
      data_table_.Insert(kHash, data_value_internal);
  ExecutionResult data_value2_result =
      data_table_.Insert(kHash, data_value_internal2);

  EXPECT_SUCCESS(data_value_result);
  EXPECT_SUCCESS(data_value2_result);
}

TEST_F(MarkedDataTableStorageTest, InsertDuplicateReturnsSuccess) {
  DataValue data_value;
  data_value.add_associated_data()->set_key("user_id");
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  DataValueInternal duplicate;
  duplicate.CopyFrom(data_value_internal);

  ExecutionResult data_value_result =
      data_table_.Insert(kHash, data_value_internal);
  ExecutionResult duplicate_result = data_table_.Insert(kHash, duplicate);

  EXPECT_SUCCESS(data_value_result);
  EXPECT_SUCCESS(duplicate_result);
}

TEST_F(MarkedDataTableStorageTest,
       InsertDuplicateWithDifferentVersionReturnsSuccess) {
  DataValue data_value;
  data_value.add_associated_data()->set_key("user_id");
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  DataValueInternal duplicate;
  duplicate.set_version_id(2);
  duplicate.CopyFrom(data_value_internal);

  ExecutionResult data_value_result =
      data_table_.Insert(kHash, data_value_internal);
  ExecutionResult duplicate_result = data_table_.Insert(kHash, duplicate);

  EXPECT_SUCCESS(data_value_result);
  EXPECT_SUCCESS(duplicate_result);
}

TEST_F(MarkedDataTableStorageTest, InsertHashKeyInvalidLengthReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);

  ExecutionResult result =
      data_table_.Insert(kHashKeyInvalidLength, data_value_internal);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE)));
}

TEST_F(MarkedDataTableStorageTest, ReplaceNonexistentReturnsSuccess) {
  DataValue data_value;
  data_value.add_associated_data()->set_key("user_id");
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);

  DataValue data_value2;
  data_value2.add_associated_data()->set_key("user_id");
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value;
  data_value_internal2.set_version_id(2);

  std::vector<DataValueInternal> data_values = {data_value_internal,
                                                data_value_internal2};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValueInternal> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value_internal),
                                        EqualsProto(data_value_internal2)));
}

TEST_F(MarkedDataTableStorageTest, ReplaceExistingReturnsSuccess) {
  DataValue data_value;
  data_value.add_associated_data()->set_key("user_id");
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));

  DataValue data_value2;
  data_value2.add_associated_data()->set_key("user_id");
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value;
  data_value_internal2.set_version_id(2);
  std::vector<DataValueInternal> data_values = {data_value_internal2};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValueInternal> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value_internal2)));
}

TEST_F(MarkedDataTableStorageTest, ReplaceHashKeyInvalidLengthReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  std::vector<DataValueInternal> data_values = {data_value_internal};

  ExecutionResult result =
      data_table_.Replace(kHashKeyInvalidLength, data_values);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(DATA_TABLE_INVALID_KEY_SIZE)));
}

TEST_F(MarkedDataTableStorageTest, FindExistingRecordReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_F(MarkedDataTableStorageTest, FindNonexistentRecordReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash2, out);

  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest,
       FindNonexistentHashKeyInvalidLengthReturnsError) {
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHashKeyInvalidLength, out);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest,
       FindSingleRecordMultipleVersionsReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].associated_data(0).key(), "user_id");
  EXPECT_EQ(out[0].associated_data(0).int_value(), 1);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_F(MarkedDataTableStorageTest, FindMultipleRecordsSameHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value),
                                        EqualsProto(data_value2)));
}

TEST_F(MarkedDataTableStorageTest,
       FindMultipleRecordsDifferentVersionsReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value),
                                        EqualsProto(data_value2)));
}

TEST_F(MarkedDataTableStorageTest,
       FindMultipleRecordsDifferentHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value_internal2));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);
  std::vector<DataValue> out2;
  ExecutionResult read_result2 = data_table_.Find(kHash2, out2);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
  EXPECT_SUCCESS(read_result2);
  EXPECT_THAT(out2, ElementsAre(EqualsProto(data_value2)));
}

TEST_F(MarkedDataTableStorageTest, FindInternalMultipleRecordsReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));
  DataValue data_value3;
  *data_value3.add_associated_data()->mutable_key() = "user_id";
  data_value3.mutable_associated_data(0)->set_int_value(3);
  DataValueInternal data_value_internal3;
  *data_value_internal3.mutable_data_value() = data_value3;
  data_value_internal3.set_version_id(3);
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value_internal3));

  std::vector<DataValueInternal> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value_internal),
                                        EqualsProto(data_value_internal2)));
}

TEST_F(MarkedDataTableStorageTest, EraseUnmatchedVersionsReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));
  DataValue data_value3;
  *data_value3.add_associated_data()->mutable_key() = "user_id";
  data_value3.mutable_associated_data(0)->set_int_value(3);
  DataValueInternal data_value_internal3;
  *data_value_internal3.mutable_data_value() = data_value3;
  data_value_internal3.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal3));

  ExecutionResult erase_result = data_table_.EraseUnmatchedVersions(kHash, 1);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value),
                                        EqualsProto(data_value3)));
}

TEST_F(MarkedDataTableStorageTest,
       EraseUnmatchedVersionsMultipleKeysReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));
  DataValue data_value3;
  *data_value3.add_associated_data()->mutable_key() = "user_id";
  data_value3.mutable_associated_data(0)->set_int_value(3);
  DataValueInternal data_value_internal3;
  *data_value_internal3.mutable_data_value() = data_value3;
  data_value_internal3.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value_internal3));

  ExecutionResult erase_result = data_table_.EraseUnmatchedVersions(kHash, 2);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
  std::vector<DataValue> out2;
  EXPECT_SUCCESS(data_table_.Find(kHash2, out2));
  EXPECT_THAT(out2, ElementsAre(EqualsProto(data_value3)));
}

TEST_F(MarkedDataTableStorageTest,
       EraseUnmatchedVersionsAllEntriesRemovedReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));

  ExecutionResult erase_result = data_table_.EraseUnmatchedVersions(kHash, 0);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_THAT(
      data_table_.Find(kHash, out),
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_F(MarkedDataTableStorageTest,
       EraseUnmatchedVersionsNonexistentKeyReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));

  ExecutionResult erase_result = data_table_.EraseUnmatchedVersions(kHash2, 1);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest, EraseAllReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value_internal2));
  DataValue data_value3;
  *data_value3.add_associated_data()->mutable_key() = "user_id";
  data_value3.mutable_associated_data(0)->set_int_value(3);
  DataValueInternal data_value_internal3;
  *data_value_internal3.mutable_data_value() = data_value3;
  data_value_internal3.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal3));

  ExecutionResult erase_result = data_table_.Erase(kHash);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_EQ(data_table_.Find(kHash, out),
            FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST));
  EXPECT_THAT(out, IsEmpty());
  EXPECT_SUCCESS(data_table_.Find(kHash2, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
}

TEST_F(MarkedDataTableStorageTest, EraseNonexistentReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));

  ExecutionResult erase_result = data_table_.Erase(kHash2);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest,
       EraseNonexistentHashKeyInvalidLengthReturnsError) {
  ExecutionResult erase_result = data_table_.Erase(kHashKeyInvalidLength);
  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest, EraseRecordReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal2));

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value2);

  EXPECT_SUCCESS(erase_result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_F(MarkedDataTableStorageTest, EraseRecordUnmatchedContentsReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value2);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest, EraseRecordUnmatchedKeyReturnsError) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));

  ExecutionResult erase_result = data_table_.Erase(kHash2, data_value);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_F(MarkedDataTableStorageTest, BeginEndIterationSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = "user_id";
  data_value.mutable_associated_data(0)->set_int_value(1);
  DataValueInternal data_value_internal;
  *data_value_internal.mutable_data_value() = data_value;
  data_value_internal.set_version_id(1);
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value_internal));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = "user_id";
  data_value2.mutable_associated_data(0)->set_int_value(2);
  DataValueInternal data_value_internal2;
  *data_value_internal2.mutable_data_value() = data_value2;
  data_value_internal2.set_version_id(2);
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value_internal2));
  DataValue data_value3;
  *data_value3.add_associated_data()->mutable_key() = "user_id";
  data_value3.mutable_associated_data(0)->set_int_value(3);
  DataValueInternal data_value_internal3;
  *data_value_internal3.mutable_data_value() = data_value3;
  data_value_internal3.set_version_id(3);
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value_internal3));

  std::vector<std::string> keys;
  for (auto it = data_table_.Begin(); it != data_table_.End(); ++it) {
    keys.push_back(std::string(std::begin(it->first), std::end(it->first)));
  }

  EXPECT_EQ(keys.size(), 2);
  EXPECT_THAT(keys, AllOf(Contains(kHash), Contains(kHash2)));
}

}  // namespace google::confidential_match
