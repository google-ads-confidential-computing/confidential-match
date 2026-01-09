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

#include "cc/lookup_server/match_data_loader/src/match_data_loader.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/types/span.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/crypto_client/mock/fake_crypto_key.h"
#include "cc/lookup_server/crypto_client/mock/mock_crypto_client.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/match_data_loader/mock/fake_match_data_loader.h"
#include "cc/lookup_server/match_data_loader/mock/mock_match_data_loader.h"
#include "cc/lookup_server/match_data_loader/src/error_codes.h"
#include "cc/lookup_server/match_data_provider/mock/mock_data_provider.h"
#include "cc/lookup_server/match_data_provider/mock/mock_streamed_match_data_provider.h"
#include "cc/lookup_server/match_data_provider/src/error_codes.h"
#include "cc/lookup_server/match_data_storage/mock/mock_match_data_storage.h"
#include "cc/lookup_server/metric_client/mock/mock_metric_client.h"
#include "cc/lookup_server/orchestrator_client/mock/mock_orchestrator_client.h"
#include "cc/lookup_server/types/match_data_group.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"
#include "protos/lookup_server/backend/location.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    DataExportInfo;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::Location;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ConsumerStreamingContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::testing::A;
using ::testing::AtLeast;
using ::testing::AtMost;
using ::testing::ElementsAre;
using ::testing::Invoke;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::Return;
using ::testing::UnorderedElementsAre;

constexpr absl::string_view kBucketName = "test-bucket";
constexpr absl::string_view kBlobStoragePath = "test/path.txt";
constexpr absl::string_view kDataExportId = "test-data-export-id";
constexpr absl::string_view kClusterGroupId = "test-cluster-group-id";
constexpr absl::string_view kClusterId = "test-cluster-id";
constexpr absl::string_view kKmsResourceName = "test-kms-key";
constexpr absl::string_view kKmsRegion = "global";
constexpr absl::string_view kKmsWipProvider = "test-wip";
constexpr absl::string_view kEncryptedDek =
    "EsEBCiQApkGW7eCE1jZqG0tDAXM0t64Hlaa/"
    "8Tycq0YjxVtcbcdZjfYSmAEAY2rrOJP86Q2FW+Z8WSst9DU7yEO4Pij1gfeW+"
    "OutRy3FhJ5BKYbIJDeBzJofkgwC/"
    "a9EHHNMAmMqomS2zPwRtm2vSZn40JsCWMld3r47Ah4Ag6rpPQ9ctEY1cQaz2JCaJ17jfPJSbli"
    "adC7EMhD0Gz2uf+ZDS942ZoC4gY2J+"
    "lKPhIimNKOALm0rqM0hC1IZ5SFcMdWYAhpHCIOoirYGEj8KM3R5cGUuZ29vZ2xlYXBpcy5jb20"
    "vZ29vZ2xlLmNyeXB0by50aW5rLkFlc0djbVNpdktleRABGIOoirYGIAE=";
constexpr absl::string_view kExportMetadataFormat =
    R"({"encrypted_dek": "%s"})";
constexpr absl::string_view kDecryptedDek = "test-decrypted-dek";
constexpr uint64_t kDataLoadingIntervalMins = 1;

class MatchDataLoaderTest : public testing::Test {
 public:
  ExecutionResult CaptureMatchDataRow(absl::string_view key,
                                      absl::Span<const MatchDataRow> rows);

 protected:
  MatchDataLoaderTest()
      : mock_data_provider_(std::make_shared<MockDataProvider>()),
        mock_match_data_provider_(
            std::make_shared<MockStreamedMatchDataProvider>()),
        mock_match_data_storage_(std::make_shared<MockMatchDataStorage>()),
        mock_metric_client_(std::make_shared<MockMetricClient>()),
        mock_orchestrator_client_(std::make_shared<MockOrchestratorClient>()),
        mock_crypto_client_(std::make_shared<MockCryptoClient>()),
        match_data_loader_(std::make_unique<MatchDataLoader>(
            mock_data_provider_, mock_match_data_provider_,
            mock_match_data_storage_, mock_metric_client_,
            mock_orchestrator_client_, mock_crypto_client_, kClusterGroupId,
            kClusterId, kKmsResourceName, kKmsRegion, kKmsWipProvider,
            kDataLoadingIntervalMins)) {}

  void SetUp() override {
    EXPECT_SUCCESS(match_data_loader_->Init());
    match_data_rows_ = {};
  }

  std::shared_ptr<MockDataProvider> mock_data_provider_;
  std::shared_ptr<MockStreamedMatchDataProvider> mock_match_data_provider_;
  std::shared_ptr<MockMatchDataStorage> mock_match_data_storage_;
  std::shared_ptr<MockMetricClient> mock_metric_client_;
  std::shared_ptr<MockOrchestratorClient> mock_orchestrator_client_;
  std::shared_ptr<MockCryptoClient> mock_crypto_client_;
  std::unique_ptr<MatchDataLoaderInterface> match_data_loader_;
  std::vector<MatchDataRow> match_data_rows_;
};

// Builds a sample match data row for testing.
MatchDataRow GetSampleMatchDataRow() {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "key";
  *match_data_row.add_associated_data()->mutable_key() = "type";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "PHONE";
  *match_data_row.add_associated_data()->mutable_key() = "user_id";
  match_data_row.mutable_associated_data(1)->set_int_value(1);
  return match_data_row;
}

// Builds a second sample match data group for testing.
MatchDataRow GetSampleMatchDataRow2() {
  MatchDataRow match_data_row;
  *match_data_row.mutable_key() = "key";
  *match_data_row.add_associated_data()->mutable_key() = "type";
  *match_data_row.mutable_associated_data(0)->mutable_string_value() = "EMAIL";
  *match_data_row.add_associated_data()->mutable_key() = "user_id";
  match_data_row.mutable_associated_data(1)->set_int_value(2);
  return match_data_row;
}

// Builds a sample match data group for testing.
MatchDataGroup GetSampleMatchDataGroup() {
  return MatchDataGroup{GetSampleMatchDataRow()};
}

// Builds a second sample match data group for testing.
MatchDataGroup GetSampleMatchDataGroup2() {
  return MatchDataGroup{GetSampleMatchDataRow2()};
}

// Helper mock to simulate fetching the export metadata.
ExecutionResult MockGetExportMetadata(
    AsyncContext<Location, std::string> context) {
  std::string export_metadata =
      absl::StrFormat(kExportMetadataFormat, kEncryptedDek);
  context.result = SuccessExecutionResult();
  context.response = std::make_shared<std::string>(export_metadata);
  context.Finish();
  return SuccessExecutionResult();
}

// Helper to capture the MatchDataRow arguments produced during a test.
ExecutionResult MatchDataLoaderTest::CaptureMatchDataRow(
    absl::string_view key, absl::Span<const MatchDataRow> rows) {
  for (const auto& row : rows) {
    match_data_rows_.push_back(row);
  }
  return SuccessExecutionResult();
}

// Helper mock to simulate a fetch with no match data returned.
ExecutionResult MockGetMatchDataWithoutData(
    ConsumerStreamingContext<Location, MatchDataBatch> context,
    std::shared_ptr<CryptoKeyInterface> crypto_key) {
  context.MarkDone();
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate a fetch with a single match data rows returned.
ExecutionResult MockGetMatchDataWithSingleRow(
    ConsumerStreamingContext<Location, MatchDataBatch> context,
    std::shared_ptr<CryptoKeyInterface> crypto_key) {
  MatchDataBatch batch = {GetSampleMatchDataGroup()};
  EXPECT_SUCCESS(context.TryPushResponse(batch));
  context.ProcessNextMessage();

  context.MarkDone();
  context.result = SuccessExecutionResult();
  context.Finish();

  return SuccessExecutionResult();
}

// Helper mock to simulate an immediate fetch error.
ExecutionResult MockGetMatchDataWithFetchFailure(
    ConsumerStreamingContext<Location, MatchDataBatch> context,
    std::shared_ptr<CryptoKeyInterface> crypto_key) {
  context.MarkDone();
  context.result = FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
  context.Finish();

  return SuccessExecutionResult();
}

// Helper mock to simulate a successful read followed by a fetch error.
ExecutionResult MockGetMatchDataWithSuccessThenFetchFailure(
    ConsumerStreamingContext<Location, MatchDataBatch> context,
    std::shared_ptr<CryptoKeyInterface> crypto_key) {
  MatchDataBatch batch = {GetSampleMatchDataGroup()};
  EXPECT_SUCCESS(context.TryPushResponse(batch));
  context.ProcessNextMessage();

  context.MarkDone();
  context.result = FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
  context.Finish();

  return SuccessExecutionResult();
}

// Helper mock to simulate a fetch with multiple match data rows returned.
ExecutionResult MockGetMatchDataWithMultipleRows(
    ConsumerStreamingContext<Location, MatchDataBatch> context,
    std::shared_ptr<CryptoKeyInterface> crypto_key) {
  MatchDataBatch batch = {GetSampleMatchDataGroup()};
  EXPECT_SUCCESS(context.TryPushResponse(batch));
  context.ProcessNextMessage();

  MatchDataBatch batch2 = {GetSampleMatchDataGroup2()};
  EXPECT_SUCCESS(context.TryPushResponse(batch2));
  context.ProcessNextMessage();

  context.MarkDone();
  context.result = SuccessExecutionResult();
  context.Finish();

  return SuccessExecutionResult();
}

//  Helper mock to simulate a successful GetCryptoKey response from the crypto
//  client.
void MockGetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> decrypt_context) {
  EXPECT_NE(decrypt_context.request, nullptr);
  EXPECT_EQ(decrypt_context.request->wrapped_key_info().kek_kms_resource_id(),
            kKmsResourceName);
  EXPECT_THAT(decrypt_context.request->wrapped_key_info().encrypted_dek(),
              Not(IsEmpty()));
  decrypt_context.response = std::make_shared<FakeCryptoKey>();
  decrypt_context.result = SuccessExecutionResult();
  decrypt_context.Finish();
}

// Helper mock to simulate a failed GetCryptoKey response from the crypto
// client.
void MockFailedGetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> decrypt_context) {
  decrypt_context.result = FailureExecutionResult(CRYPTO_CLIENT_GET_AEAD_ERROR);
  decrypt_context.Finish();
}

TEST_F(MatchDataLoaderTest, StartStop) {
  EXPECT_CALL(*mock_data_provider_, Get)
      .Times(AtMost(1))
      .WillOnce(MockGetExportMetadata);
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .Times(AtMost(1))
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey)
      .Times(AtMost(1))
      .WillOnce(MockGetCryptoKey);
  EXPECT_CALL(*mock_orchestrator_client_,
              GetDataExportInfo(A<const GetDataExportInfoRequest&>()))
      .Times(AtMost(1))
      .WillOnce(Return(GetDataExportInfoResponse{
          .data_export_info = std::make_shared<DataExportInfo>()}));
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .Times(AtMost(1))
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  MatchDataLoader match_data_loader(
      mock_data_provider_, mock_match_data_provider_, mock_match_data_storage_,
      mock_metric_client_, mock_orchestrator_client_, mock_crypto_client_,
      kClusterGroupId, kClusterId, kKmsResourceName, kKmsRegion,
      kKmsWipProvider, kDataLoadingIntervalMins);

  EXPECT_SUCCESS(match_data_loader.Init());
  EXPECT_SUCCESS(match_data_loader.Run());
  EXPECT_SUCCESS(match_data_loader.Stop());
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));
}

TEST_F(MatchDataLoaderTest, LoadWithErrorStartingJobReturnsError) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(
          Return(FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR)));
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);

  ExecutionResult result =
      match_data_loader_->Load(data_export_info, kEncryptedDek);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(match_data_rows_, IsEmpty());
}

TEST_F(MatchDataLoaderTest, LoadWithInvalidEncryptedDekReturnsError) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).Times(0);

  EXPECT_THAT(match_data_loader_->Load(data_export_info, "invalid"),
              ResultIs(FailureExecutionResult(
                  MATCH_DATA_LOADER_INVALID_ENCRYPTED_DEK)));
  EXPECT_THAT(match_data_rows_, IsEmpty());
}

TEST_F(MatchDataLoaderTest, LoadGetCryptoKeyErrorYieldsEmptyList) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey)
      .WillOnce(MockFailedGetCryptoKey);

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));
  EXPECT_THAT(match_data_rows_, IsEmpty());
}

TEST_F(MatchDataLoaderTest, LoadEmptyIsSuccessful) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .Times(1)
      .WillOnce(MockGetMatchDataWithoutData);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, FinalizeUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .Times(AtLeast(1))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));
}

TEST_F(MatchDataLoaderTest, LoadSingleEntryIsSuccessful) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithSingleRow);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillRepeatedly(Invoke(this, &MatchDataLoaderTest::CaptureMatchDataRow));
  EXPECT_CALL(*mock_match_data_storage_, FinalizeUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .Times(AtLeast(1))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  EXPECT_THAT(match_data_rows_,
              ElementsAre(EqualsProto(GetSampleMatchDataRow())));
}

TEST_F(MatchDataLoaderTest, LoadMultipleEntriesIsSuccessful) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithMultipleRows);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillRepeatedly(Invoke(this, &MatchDataLoaderTest::CaptureMatchDataRow));
  EXPECT_CALL(*mock_match_data_storage_, FinalizeUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .Times(AtLeast(1))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  EXPECT_THAT(match_data_rows_,
              UnorderedElementsAre(EqualsProto(GetSampleMatchDataRow()),
                                   EqualsProto(GetSampleMatchDataRow2())));
}

TEST_F(MatchDataLoaderTest, LoadWithImmediateFetchFailureCancelsUpdate) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithFetchFailure);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillRepeatedly(Invoke(this, &MatchDataLoaderTest::CaptureMatchDataRow));
  EXPECT_CALL(*mock_match_data_storage_, CancelUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .Times(AtLeast(1))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));

  EXPECT_THAT(match_data_rows_, IsEmpty());
}

TEST_F(MatchDataLoaderTest, LoadWithSuccessThenFetchFailureCancelsUpdate) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithSuccessThenFetchFailure);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillRepeatedly(Invoke(this, &MatchDataLoaderTest::CaptureMatchDataRow));
  EXPECT_CALL(*mock_match_data_storage_, CancelUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .Times(AtLeast(1))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));

  EXPECT_THAT(match_data_rows_,
              ElementsAre(EqualsProto(GetSampleMatchDataRow())));
}

TEST_F(MatchDataLoaderTest, LoadWithRetriedFailureIsSuccessful) {
  DataExportInfo data_export_info;
  *data_export_info.mutable_data_export_id() = kDataExportId;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *data_export_info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kBlobStoragePath;
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithFetchFailure)
      .WillOnce(MockGetMatchDataWithSingleRow);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .Times(2)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillRepeatedly(Invoke(this, &MatchDataLoaderTest::CaptureMatchDataRow));
  EXPECT_CALL(*mock_match_data_storage_, CancelUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, FinalizeUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey)
      .Times(2)
      .WillRepeatedly(MockGetCryptoKey);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .Times(AtLeast(1))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));
  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  EXPECT_THAT(match_data_rows_,
              ElementsAre(EqualsProto(GetSampleMatchDataRow())));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
