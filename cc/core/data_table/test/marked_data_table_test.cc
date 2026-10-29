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

#include "cc/core/data_table/src/marked_data_table.h"

#include <string>
#include <vector>

#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "public/core/interface/execution_result.h"

#include "cc/core/data_table/src/error_codes.h"
#include "protos/core/data_value.pb.h"

namespace google::confidential_match {
namespace {

using ::google::confidential_match::DataValue;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Return;
using ::testing::UnorderedElementsAre;

constexpr absl::string_view kDatasetId = "dataset-id";
constexpr absl::string_view kDatasetId2 = "dataset-id-2";
constexpr absl::string_view kHash = "example-hash-string-of-length-32";
constexpr absl::string_view kHash2 = "another-hash-string-of-length-32";
constexpr absl::string_view kUserIdKey = "user_id";
constexpr int kUserId = 1;
constexpr int kUserId2 = 2;

class MarkedDataTableTest : public testing::Test,
                            public testing::WithParamInterface<bool> {
 protected:
  MarkedDataTableTest()
      : data_table_(/*bucket_count=*/0, /*enable_packed_storage=*/GetParam()) {}

  MarkedDataTable data_table_;
};

INSTANTIATE_TEST_SUITE_P(MarkedDataTableTests, MarkedDataTableTest,
                         testing::Bool());

TEST_P(MarkedDataTableTest, InsertSingleReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));

  ExecutionResult result = data_table_.Insert(kHash, data_value);

  EXPECT_SUCCESS(result);
}

TEST_P(MarkedDataTableTest, InsertMultipleReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));

  ExecutionResult result = data_table_.Insert(kHash, data_value);
  ExecutionResult result2 = data_table_.Insert(kHash, data_value2);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(result2);
}

TEST_P(MarkedDataTableTest, InsertWithoutStartUpdateReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);

  ExecutionResult result = data_table_.Insert(kHash, data_value);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, InsertDuplicateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));

  ExecutionResult result = data_table_.Insert(kHash, data_value);
  ExecutionResult duplicate_result = data_table_.Insert(kHash, data_value);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(duplicate_result);
}

TEST_P(MarkedDataTableTest, FindNonexistentReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash2, out);

  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, FindNonexistentPendingUpdateReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash2, out);

  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, FindExistingRecordPendingUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest, FindExistingRecordNoPendingUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest, FindExistingRecordCanceledUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.CancelUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest, FindPendingNewVersionedUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest, FindPendingOldVersionedRecordReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value2));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest,
       FindPendingAndFinalizedRecordsSameHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value2));

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value),
                                        EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest,
       FindFinalizedDroppedRecordSameHashReturnsRemainingElement) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value2));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest,
       FindFinalizedDroppedRecordDifferentHashReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Insert(kHash2, data_value2));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, FindCanceledRetriedRecordSameHashReturnsElement) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.CancelUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest,
       FindCanceledDroppedRecordSameHashReturnsRemainingElement) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.CancelUpdate());
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value2));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest, ReplaceNonexistentReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  std::vector<DataValue> data_values = {data_value, data_value2};
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, UnorderedElementsAre(EqualsProto(data_value),
                                        EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest, ReplaceExistingSameUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  std::vector<DataValue> data_values = {data_value2};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest, ReplaceExistingNewPendingUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  std::vector<DataValue> data_values = {data_value2};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest, ReplaceWithoutStartUpdateReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  std::vector<DataValue> data_values = {data_value};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, ReplaceExistingSameRecordSameUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  std::vector<DataValue> data_values = {data_value};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest,
       ReplaceExistingSameRecordNewPendingUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  std::vector<DataValue> data_values = {data_value};

  ExecutionResult result = data_table_.Replace(kHash, data_values);

  EXPECT_SUCCESS(result);
  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value)));
}

TEST_P(MarkedDataTableTest, EraseRecordSameUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value);
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(erase_result);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, EraseRecordNewPendingUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value);
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(erase_result);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, EraseRecordSharingHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value2));

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value);
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(erase_result);
  EXPECT_SUCCESS(read_result);
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));
}

TEST_P(MarkedDataTableTest, EraseRecordWithoutStartUpdateReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);

  ExecutionResult result = data_table_.Erase(kHash, data_value);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, EraseRecordNonexistentReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  ExecutionResult erase_result = data_table_.Erase(kHash, data_value2);

  EXPECT_THAT(
      erase_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

TEST_P(MarkedDataTableTest, EraseKeySameUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  ExecutionResult erase_result = data_table_.Erase(kHash);
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(erase_result);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, EraseKeyNewPendingUpdateReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));

  ExecutionResult erase_result = data_table_.Erase(kHash);
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(erase_result);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, EraseKeySharingHashReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value2));

  ExecutionResult erase_result = data_table_.Erase(kHash);
  std::vector<DataValue> out;
  ExecutionResult read_result = data_table_.Find(kHash, out);

  EXPECT_SUCCESS(erase_result);
  EXPECT_THAT(
      read_result,
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
  EXPECT_THAT(out, IsEmpty());
}

TEST_P(MarkedDataTableTest, EraseKeyWithoutStartUpdateReturnsFailure) {
  EXPECT_THAT(data_table_.Erase(kHash),
              ResultIs(FailureExecutionResult(
                  VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest,
       StartUpdateWithInProgressUpdateSameDatasetIdReturnsFailure) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Insert(kHash, data_value));

  ExecutionResult second_update = data_table_.StartUpdate(kDatasetId);

  EXPECT_THAT(second_update,
              ResultIs(FailureExecutionResult(
                  VERSIONED_DATA_TABLE_UPDATE_ALREADY_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, StartUpdateWithNewDatasetIdReturnsFailure) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));

  ExecutionResult second_update = data_table_.StartUpdate(kDatasetId2);

  EXPECT_THAT(second_update,
              ResultIs(FailureExecutionResult(
                  VERSIONED_DATA_TABLE_UPDATE_ALREADY_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, FinalizeUpdateWithoutStartingReturnsFailure) {
  EXPECT_THAT(data_table_.FinalizeUpdate(),
              ResultIs(FailureExecutionResult(
                  VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, CancelUpdateWithoutStartingReturnsFailure) {
  EXPECT_THAT(data_table_.CancelUpdate(),
              ResultIs(FailureExecutionResult(
                  VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS)));
}

TEST_P(MarkedDataTableTest, IsUpdateInProgressNoUpdateReturnsFalse) {
  EXPECT_FALSE(data_table_.IsUpdateInProgress());
}

TEST_P(MarkedDataTableTest, IsUpdateInProgressWithUpdateReturnsTrue) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_TRUE(data_table_.IsUpdateInProgress());
}

TEST_P(MarkedDataTableTest, IsUpdateInProgressWithFinalizedUpdateReturnsFalse) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_FALSE(data_table_.IsUpdateInProgress());
}

TEST_P(MarkedDataTableTest, IsUpdateInProgressWithCanceledUpdateReturnsFalse) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.CancelUpdate());
  EXPECT_FALSE(data_table_.IsUpdateInProgress());
}

TEST_P(MarkedDataTableTest, GetDatasetIdWithoutUpdates) {
  EXPECT_EQ(data_table_.GetDatasetId(), "");
}

TEST_P(MarkedDataTableTest, GetDatasetIdWithPendingUpdate) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));

  const std::string dataset_id = data_table_.GetDatasetId();

  EXPECT_EQ(dataset_id, kDatasetId);
}

TEST_P(MarkedDataTableTest, GetDatasetIdWithMultipleUpdates) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  const std::string dataset_id = data_table_.GetDatasetId();

  EXPECT_EQ(dataset_id, kDatasetId2);
}

TEST_P(MarkedDataTableTest, GetPendingDatasetIdWithoutUpdates) {
  EXPECT_EQ(data_table_.GetPendingDatasetId(), "");
}

TEST_P(MarkedDataTableTest, GetPendingDatasetIdWithPendingUpdate) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));

  const std::string pending_dataset_id = data_table_.GetPendingDatasetId();

  EXPECT_EQ(pending_dataset_id, kDatasetId2);
}

TEST_P(MarkedDataTableTest, GetPendingDatasetIdWithFinalizedUpdate) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  const std::string pending_dataset_id = data_table_.GetPendingDatasetId();

  EXPECT_EQ(pending_dataset_id, "");
}

TEST_P(MarkedDataTableTest, GetPendingDatasetIdWithCanceledUpdate) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.CancelUpdate());

  const std::string pending_dataset_id = data_table_.GetPendingDatasetId();

  EXPECT_EQ(pending_dataset_id, kDatasetId);
}

TEST_P(MarkedDataTableTest, GetPendingDatasetIdWithCanceledWithNewUpdate) {
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.CancelUpdate());
  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));

  const std::string pending_dataset_id = data_table_.GetPendingDatasetId();

  EXPECT_EQ(pending_dataset_id, kDatasetId2);
}

TEST_P(MarkedDataTableTest,
       ReplaceAcrossUpdatesAndFinalizeDropsUnmatchedKeysReturnsSuccess) {
  DataValue data_value;
  *data_value.add_associated_data()->mutable_key() = kUserIdKey;
  data_value.mutable_associated_data(0)->set_int_value(kUserId);
  DataValue data_value2;
  *data_value2.add_associated_data()->mutable_key() = kUserIdKey;
  data_value2.mutable_associated_data(0)->set_int_value(kUserId2);

  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId));
  EXPECT_SUCCESS(data_table_.Replace(kHash, {data_value}));
  EXPECT_SUCCESS(data_table_.Replace(kHash2, {data_value}));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  EXPECT_SUCCESS(data_table_.StartUpdate(kDatasetId2));
  EXPECT_SUCCESS(data_table_.Replace(kHash, {data_value2}));
  EXPECT_SUCCESS(data_table_.FinalizeUpdate());

  std::vector<DataValue> out;
  EXPECT_SUCCESS(data_table_.Find(kHash, out));
  EXPECT_THAT(out, ElementsAre(EqualsProto(data_value2)));

  std::vector<DataValue> out2;
  EXPECT_THAT(
      data_table_.Find(kHash2, out2),
      ResultIs(FailureExecutionResult(DATA_TABLE_ENTRY_DOES_NOT_EXIST)));
}

}  // namespace
}  // namespace google::confidential_match
