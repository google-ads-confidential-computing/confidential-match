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

#include "cc/lookup_server/match_data_storage/src/in_memory_match_data_storage.h"

#include <memory>
#include <vector>

#include "cc/core/interface/type_def.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/match_data_storage/mock/fake_match_data_storage.h"
#include "cc/lookup_server/match_data_storage/mock/mock_match_data_storage.h"
#include "cc/lookup_server/match_data_storage/src/error_codes.h"
#include "cc/lookup_server/scheme_validator/src/error_codes.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_backend::
    DataExportInfo;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::protobuf::util::MessageDifferencer;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

// Sample base-64 hash key with length 32 after decoding
constexpr absl::string_view kHashKey =
    "ZXhhbXBsZS1oYXNoLXN0cmluZy1vZi1sZW5ndGgtMzI=";
constexpr absl::string_view kHashKey2 =
    "YW5vdGhlci1oYXNoLXN0cmluZy1vZi1sZW5ndGgtMzI=";
constexpr absl::string_view kHashKeyInvalidLength = "dGVzdA==";
constexpr absl::string_view kValidShardingSchemeType = "jch";

class InMemoryMatchDataStorageTest : public testing::Test {
 protected:
  InMemoryMatchDataStorageTest() {
    match_data_storage_ = std::make_unique<InMemoryMatchDataStorage>();
  }

  void SetUp() override { EXPECT_SUCCESS(match_data_storage_->Init()); }

  std::unique_ptr<MatchDataStorageInterface> match_data_storage_;
};

TEST_F(InMemoryMatchDataStorageTest, StartStop) {
  InMemoryMatchDataStorage match_data_storage;
  EXPECT_SUCCESS(match_data_storage.Init());
  EXPECT_SUCCESS(match_data_storage.Run());
  EXPECT_SUCCESS(match_data_storage.Stop());
}

TEST_F(InMemoryMatchDataStorageTest, InsertSingleReturnsSuccess) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));

  ExecutionResult result = match_data_storage_->Insert(row);

  EXPECT_SUCCESS(result);
}

TEST_F(InMemoryMatchDataStorageTest, InsertMultipleReturnsSuccess) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  MatchDataRow row2;
  row2.CopyFrom(row);
  row2.mutable_associated_data(1)->set_int_value(2);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));

  ExecutionResult result = match_data_storage_->Insert(row);
  ExecutionResult result2 = match_data_storage_->Insert(row2);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(result2);
}

TEST_F(InMemoryMatchDataStorageTest, InsertDuplicateReturnsSuccess) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));

  ExecutionResult result = match_data_storage_->Insert(row);
  ExecutionResult result2 = match_data_storage_->Insert(row);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(result2);
}

TEST_F(InMemoryMatchDataStorageTest, InsertHashKeyInvalidLengthReturnsError) {
  MatchDataRow row;
  *row.mutable_key() = kHashKeyInvalidLength;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));

  ExecutionResult result = match_data_storage_->Insert(row);

  EXPECT_THAT(
      result,
      ResultIs(FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR)));
}

TEST_F(InMemoryMatchDataStorageTest, ReplaceNonexistentRowReturnsSuccess) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  MatchDataRow row2;
  row2.CopyFrom(row);
  row2.mutable_associated_data(1)->set_int_value(2);
  std::vector<MatchDataRow> rows = {row, row2};
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));

  ExecutionResult result = match_data_storage_->Replace(kHashKey, rows);

  EXPECT_SUCCESS(result);
  ExecutionResultOr<std::vector<MatchDataRow>> get_result_or =
      match_data_storage_->Get(kHashKey);
  EXPECT_SUCCESS(get_result_or);
  EXPECT_THAT(*get_result_or,
              UnorderedElementsAre(EqualsProto(row), EqualsProto(row2)));
}

TEST_F(InMemoryMatchDataStorageTest, ReplaceExistingRowReturnsSuccess) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  EXPECT_SUCCESS(match_data_storage_->Insert(row));
  MatchDataRow row2;
  row2.CopyFrom(row);
  row2.mutable_associated_data(1)->set_int_value(2);
  std::vector<MatchDataRow> rows = {row2};

  ExecutionResult result = match_data_storage_->Replace(kHashKey, rows);

  EXPECT_SUCCESS(result);
  ExecutionResultOr<std::vector<MatchDataRow>> get_result_or =
      match_data_storage_->Get(kHashKey);
  EXPECT_SUCCESS(get_result_or);
  EXPECT_THAT(*get_result_or, ElementsAre(EqualsProto(row2)));
}

TEST_F(InMemoryMatchDataStorageTest, ReplaceWithoutStartUpdateReturnsError) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  std::vector<MatchDataRow> rows = {row};

  ExecutionResult result = match_data_storage_->Replace(kHashKey, rows);

  EXPECT_THAT(
      result,
      ResultIs(FailureExecutionResult(MATCH_DATA_STORAGE_REPLACE_ERROR)));
}

TEST_F(InMemoryMatchDataStorageTest, ReplaceWithInvalidKeyReturnsError) {
  MatchDataRow row;
  *row.mutable_key() = kHashKeyInvalidLength;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  std::vector<MatchDataRow> rows = {row};

  ExecutionResult result =
      match_data_storage_->Replace(kHashKeyInvalidLength, rows);

  EXPECT_THAT(
      result,
      ResultIs(FailureExecutionResult(MATCH_DATA_STORAGE_INVALID_KEY_ERROR)));
}

TEST_F(InMemoryMatchDataStorageTest, ReplaceWithMismatchedKeyReturnsError) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  std::vector<MatchDataRow> rows = {row};

  ExecutionResult result = match_data_storage_->Replace(kHashKey2, rows);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          MATCH_DATA_STORAGE_REPLACE_KEY_MISMATCH)));
}

TEST_F(InMemoryMatchDataStorageTest, GetNonexistentEntryReturnsNoMatches) {
  ExecutionResultOr<std::vector<MatchDataRow>> result_or =
      match_data_storage_->Get("nonexistent");
  EXPECT_SUCCESS(result_or);
  EXPECT_THAT(*result_or, IsEmpty());
}

TEST_F(InMemoryMatchDataStorageTest, GetNonBase64EncodedEntryReturnsNoMatches) {
  ExecutionResultOr<std::vector<MatchDataRow>> result_or =
      match_data_storage_->Get("Not base-64 encoded.");
  EXPECT_SUCCESS(result_or);
  EXPECT_THAT(*result_or, IsEmpty());
}

TEST_F(InMemoryMatchDataStorageTest, GetSingleEntryReturnsMatch) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  EXPECT_SUCCESS(match_data_storage_->Insert(row));
  EXPECT_SUCCESS(match_data_storage_->FinalizeUpdate());

  ExecutionResultOr<std::vector<MatchDataRow>> result_or =
      match_data_storage_->Get(kHashKey);

  EXPECT_SUCCESS(result_or);
  EXPECT_THAT(*result_or, ElementsAre(EqualsProto(row)));
}

TEST_F(InMemoryMatchDataStorageTest, GetMultipleEntriesReturnsMatches) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  EXPECT_SUCCESS(match_data_storage_->Insert(row));
  EXPECT_SUCCESS(match_data_storage_->FinalizeUpdate());
  MatchDataRow row2;
  row2.CopyFrom(row);
  row2.mutable_associated_data(1)->set_int_value(2);
  DataExportInfo data_export_info2;
  data_export_info2.set_data_export_id("dataset_id2");
  *data_export_info2.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info2.mutable_sharding_scheme()->set_num_shards(2);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info2));
  EXPECT_SUCCESS(match_data_storage_->Insert(row2));

  ExecutionResultOr<std::vector<MatchDataRow>> result_or =
      match_data_storage_->Get(kHashKey);

  EXPECT_SUCCESS(result_or);
  EXPECT_THAT(*result_or,
              UnorderedElementsAre(EqualsProto(row), EqualsProto(row2)));
}

TEST_F(InMemoryMatchDataStorageTest, GetCanceledUpdateReturnsMatch) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  EXPECT_SUCCESS(match_data_storage_->Insert(row));
  EXPECT_SUCCESS(match_data_storage_->FinalizeUpdate());
  MatchDataRow row2;
  *row2.mutable_key() = kHashKey;
  *row2.add_associated_data()->mutable_key() = "type";
  *row2.mutable_associated_data(0)->mutable_string_value() = "email";
  *row2.add_associated_data()->mutable_key() = "user_id";
  row2.mutable_associated_data(1)->set_int_value(2);
  DataExportInfo data_export_info2;
  data_export_info2.set_data_export_id("dataset_id2");
  *data_export_info2.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info2.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info2));
  EXPECT_SUCCESS(match_data_storage_->Insert(row2));
  EXPECT_SUCCESS(match_data_storage_->CancelUpdate());

  ExecutionResultOr<std::vector<MatchDataRow>> result_or =
      match_data_storage_->Get(kHashKey);

  EXPECT_SUCCESS(result_or);
  EXPECT_THAT(*result_or,
              UnorderedElementsAre(EqualsProto(row), EqualsProto(row2)));
}

TEST_F(InMemoryMatchDataStorageTest, RemoveNonexistentReturnsSuccess) {
  MatchDataRow row;
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));

  ExecutionResult result = match_data_storage_->Remove(row);

  EXPECT_SUCCESS(result);
}

TEST_F(InMemoryMatchDataStorageTest, RemoveExistingEntryReturnsSuccess) {
  MatchDataRow row;
  *row.mutable_key() = kHashKey;
  *row.add_associated_data()->mutable_key() = "type";
  *row.mutable_associated_data(0)->mutable_string_value() = "email";
  *row.add_associated_data()->mutable_key() = "user_id";
  row.mutable_associated_data(1)->set_int_value(1);
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  EXPECT_SUCCESS(match_data_storage_->StartUpdate(data_export_info));
  EXPECT_SUCCESS(match_data_storage_->Insert(row));
  MatchDataRow row_to_remove;
  row_to_remove.CopyFrom(row);
  row_to_remove.mutable_associated_data(1)->set_int_value(2);
  EXPECT_SUCCESS(match_data_storage_->Insert(row_to_remove));

  ExecutionResult result = match_data_storage_->Remove(row_to_remove);

  EXPECT_SUCCESS(result);
  ExecutionResultOr<std::vector<MatchDataRow>> remaining =
      match_data_storage_->Get(kHashKey);
  EXPECT_SUCCESS(remaining.result());
  EXPECT_THAT(*remaining, ElementsAre(EqualsProto(row)));
}

TEST_F(InMemoryMatchDataStorageTest, StartUpdateReturnsSuccess) {
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  *data_export_info.mutable_sharding_scheme()->mutable_type() =
      kValidShardingSchemeType;
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);

  ExecutionResult result = match_data_storage_->StartUpdate(data_export_info);

  EXPECT_SUCCESS(result);
}

TEST_F(InMemoryMatchDataStorageTest, StartUpdateWithInvalidSchemeReturnsError) {
  DataExportInfo data_export_info;
  data_export_info.set_data_export_id("dataset_id");
  data_export_info.mutable_sharding_scheme()->set_type("invalid_scheme");
  data_export_info.mutable_sharding_scheme()->set_num_shards(1);
  ExecutionResult result = match_data_storage_->StartUpdate(data_export_info);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_UNSUPPORTED_SCHEME_TYPE)));
}

TEST_F(InMemoryMatchDataStorageTest,
       FinalizeUpdateWithoutStartingUpdateReturnsError) {
  ExecutionResult result = match_data_storage_->FinalizeUpdate();
  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_PENDING_SCHEME_NOT_SET)));
}

}  // namespace google::confidential_match::lookup_server
