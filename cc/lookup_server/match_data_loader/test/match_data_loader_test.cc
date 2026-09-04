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
#include "cc/lookup_server/match_data_storage/src/error_codes.h"
#include "cc/lookup_server/metric_client/mock/fake_metric_client.h"
#include "cc/lookup_server/metric_client/mock/mock_metric_client.h"
#include "cc/lookup_server/orchestrator_client/mock/mock_orchestrator_client.h"
#include "cc/lookup_server/orchestrator_client/src/error_codes.h"
#include "cc/lookup_server/types/match_data_group.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
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
constexpr absl::string_view kShardingSchemeType = "jch";
constexpr int kShardingSchemeNumShards = 11111;
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
        mock_otel_metric_client_(std::make_shared<FakeMetricClient>()),
        mock_orchestrator_client_(std::make_shared<MockOrchestratorClient>()),
        mock_crypto_client_(std::make_shared<MockCryptoClient>()),
        match_data_loader_(std::make_unique<MatchDataLoader>(
            mock_data_provider_, mock_match_data_provider_,
            mock_match_data_storage_, mock_metric_client_,
            mock_otel_metric_client_, mock_orchestrator_client_,
            mock_crypto_client_, kClusterGroupId, kClusterId, kKmsResourceName,
            kKmsRegion, kKmsWipProvider, kDataLoadingIntervalMins)) {}

  void SetUp() override {
    EXPECT_SUCCESS(match_data_loader_->Init());
    match_data_rows_ = {};
    *data_export_info_.mutable_data_export_id() = kDataExportId;
    *data_export_info_.mutable_shard_location()
         ->mutable_blob_storage_location()
         ->mutable_bucket_name() = kBucketName;
    *data_export_info_.mutable_shard_location()
         ->mutable_blob_storage_location()
         ->mutable_path() = kBlobStoragePath;
    data_export_info_.mutable_sharding_scheme()->set_type(kShardingSchemeType);
    data_export_info_.mutable_sharding_scheme()->set_num_shards(
        kShardingSchemeNumShards);
  }

  std::shared_ptr<MockDataProvider> mock_data_provider_;
  std::shared_ptr<MockStreamedMatchDataProvider> mock_match_data_provider_;
  std::shared_ptr<MockMatchDataStorage> mock_match_data_storage_;
  std::shared_ptr<MockMetricClient> mock_metric_client_;
  std::shared_ptr<FakeMetricClient> mock_otel_metric_client_;
  std::shared_ptr<MockOrchestratorClient> mock_orchestrator_client_;
  std::shared_ptr<MockCryptoClient> mock_crypto_client_;
  std::unique_ptr<MatchDataLoaderInterface> match_data_loader_;
  std::vector<MatchDataRow> match_data_rows_;
  DataExportInfo data_export_info_;
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

// Helper mock to simulate a scheduling failure during metadata fetch.
ExecutionResult MockGetExportMetadataScheduleFailure(
    AsyncContext<Location, std::string> context) {
  return FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
}

// Helper mock to simulate an async metadata fetch failure.
ExecutionResult MockGetExportMetadataAsyncFailure(
    AsyncContext<Location, std::string> context) {
  context.result = FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate a parsing failure during metadata read.
ExecutionResult MockGetInvalidExportMetadata(
    AsyncContext<Location, std::string> context) {
  context.result = SuccessExecutionResult();
  context.response = std::make_shared<std::string>("invalid json metadata");
  context.Finish();
  return SuccessExecutionResult();
}

void VerifyMetricLabels(const FakeMetricClient::RecordedMetric& metric) {
  EXPECT_EQ(metric.labels.at("cluster_id"), kClusterId);
  EXPECT_EQ(metric.labels.at("cluster_group_id"), kClusterGroupId);
  EXPECT_EQ(metric.labels.at("data_export_id"), kDataExportId);
  EXPECT_EQ(metric.labels.at("sharding_scheme_type"), kShardingSchemeType);
  EXPECT_EQ(metric.labels.at("sharding_scheme_num_shards"),
            std::to_string(kShardingSchemeNumShards));
}

void VerifyMetricClusterLabels(const FakeMetricClient::RecordedMetric& metric) {
  EXPECT_EQ(metric.labels.at("cluster_id"), kClusterId);
  EXPECT_EQ(metric.labels.at("cluster_group_id"), kClusterGroupId);
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
          .data_export_info =
              std::make_shared<DataExportInfo>(data_export_info_)}));
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .Times(AtMost(1))
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  MatchDataLoader match_data_loader(
      mock_data_provider_, mock_match_data_provider_, mock_match_data_storage_,
      mock_metric_client_, mock_otel_metric_client_, mock_orchestrator_client_,
      mock_crypto_client_, kClusterGroupId, kClusterId, kKmsResourceName,
      kKmsRegion, kKmsWipProvider, kDataLoadingIntervalMins);

  EXPECT_SUCCESS(match_data_loader.Init());
  EXPECT_SUCCESS(match_data_loader.Run());
  EXPECT_SUCCESS(match_data_loader.Stop());
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  bool found_get_data_export_info = false;
  bool found_get_export_metadata = false;
  for (const auto& metric : mock_otel_metric_client_->GetRecordedMetrics()) {
    if (metric.name == "data_loader_data_export_info_duration") {
      found_get_data_export_info = true;
      EXPECT_EQ(metric.type, MetricType::METRIC_TYPE_GAUGE);
      EXPECT_EQ(metric.unit, MetricUnit::METRIC_UNIT_MILLISECONDS);
      VerifyMetricClusterLabels(metric);
      EXPECT_FALSE(metric.value.empty());
      EXPECT_EQ(metric.labels.at("IsSuccessful"), "true");
    } else if (metric.name == "data_loader_get_export_metadata_duration") {
      found_get_export_metadata = true;
      EXPECT_EQ(metric.type, MetricType::METRIC_TYPE_GAUGE);
      EXPECT_EQ(metric.unit, MetricUnit::METRIC_UNIT_MILLISECONDS);
      EXPECT_FALSE(metric.value.empty());
      EXPECT_EQ(metric.labels.at("IsSuccessful"), "true");
      VerifyMetricLabels(metric);
    }
  }
  EXPECT_TRUE(found_get_data_export_info);
  EXPECT_TRUE(found_get_export_metadata);
}

TEST_F(MatchDataLoaderTest, GetDataExportInfoFailure) {
  EXPECT_CALL(*mock_orchestrator_client_,
              GetDataExportInfo(A<const GetDataExportInfoRequest&>()))
      .Times(AtMost(1))
      .WillOnce(
          Return(FailureExecutionResult(ORCHESTRATOR_CLIENT_PARSE_ERROR)));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  MatchDataLoader match_data_loader(
      mock_data_provider_, mock_match_data_provider_, mock_match_data_storage_,
      mock_metric_client_, mock_otel_metric_client_, mock_orchestrator_client_,
      mock_crypto_client_, kClusterGroupId, kClusterId, kKmsResourceName,
      kKmsRegion, kKmsWipProvider, kDataLoadingIntervalMins);

  EXPECT_SUCCESS(match_data_loader.Init());
  EXPECT_SUCCESS(match_data_loader.Run());
  // Wait for load to run
  absl::SleepFor(absl::Seconds(1));
  EXPECT_SUCCESS(match_data_loader.Stop());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 2);
  const auto& err_metric = mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(err_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(err_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(err_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(err_metric.value, "1");
  EXPECT_EQ(err_metric.labels.at("BackendErrorReason"),
            "Failed to parse response from the Orchestrator.");
  EXPECT_EQ(err_metric.labels.at("cluster_id"), kClusterId);
  EXPECT_EQ(err_metric.labels.at("cluster_group_id"), kClusterGroupId);
}

TEST_F(MatchDataLoaderTest, GetExportMetadataFetchScheduleError) {
  EXPECT_CALL(*mock_data_provider_, Get)
      .Times(AtMost(1))
      .WillOnce(MockGetExportMetadataScheduleFailure);
  EXPECT_CALL(*mock_orchestrator_client_,
              GetDataExportInfo(A<const GetDataExportInfoRequest&>()))
      .Times(AtMost(1))
      .WillOnce(Return(GetDataExportInfoResponse{
          .data_export_info =
              std::make_shared<DataExportInfo>(data_export_info_)}));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  MatchDataLoader match_data_loader(
      mock_data_provider_, mock_match_data_provider_, mock_match_data_storage_,
      mock_metric_client_, mock_otel_metric_client_, mock_orchestrator_client_,
      mock_crypto_client_, kClusterGroupId, kClusterId, kKmsResourceName,
      kKmsRegion, kKmsWipProvider, kDataLoadingIntervalMins);

  EXPECT_SUCCESS(match_data_loader.Init());
  EXPECT_SUCCESS(match_data_loader.Run());
  // Wait for load to run
  absl::SleepFor(absl::Seconds(1));
  EXPECT_SUCCESS(match_data_loader.Stop());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 3);

  const auto& metric_dur = mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(metric_dur.name, "data_loader_data_export_info_duration");
  EXPECT_EQ(metric_dur.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(metric_dur.unit, MetricUnit::METRIC_UNIT_MILLISECONDS);
  EXPECT_EQ(metric_dur.labels.at("IsSuccessful"), "true");
  VerifyMetricClusterLabels(metric_dur);

  const auto& metric_err = mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(metric_err.name, "data_loader_load_error_count");
  EXPECT_EQ(metric_err.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(metric_err.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(metric_err.value, "1");
  EXPECT_EQ(metric_err.labels.at("BackendErrorReason"),
            "Failed to fetch requested data.");
  VerifyMetricLabels(metric_err);

  const auto& metric_dur_2 = mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(metric_dur_2.name, "data_loader_get_export_metadata_duration");
  EXPECT_EQ(metric_dur_2.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(metric_dur_2.unit, MetricUnit::METRIC_UNIT_MILLISECONDS);
  EXPECT_EQ(metric_dur_2.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(metric_dur_2);
}

TEST_F(MatchDataLoaderTest, GetExportMetadataFetchAsyncError) {
  EXPECT_CALL(*mock_data_provider_, Get)
      .Times(AtMost(1))
      .WillOnce(MockGetExportMetadataAsyncFailure);
  EXPECT_CALL(*mock_orchestrator_client_,
              GetDataExportInfo(A<const GetDataExportInfoRequest&>()))
      .Times(AtMost(1))
      .WillOnce(Return(GetDataExportInfoResponse{
          .data_export_info =
              std::make_shared<DataExportInfo>(data_export_info_)}));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  MatchDataLoader match_data_loader(
      mock_data_provider_, mock_match_data_provider_, mock_match_data_storage_,
      mock_metric_client_, mock_otel_metric_client_, mock_orchestrator_client_,
      mock_crypto_client_, kClusterGroupId, kClusterId, kKmsResourceName,
      kKmsRegion, kKmsWipProvider, kDataLoadingIntervalMins);

  EXPECT_SUCCESS(match_data_loader.Init());
  EXPECT_SUCCESS(match_data_loader.Run());
  // Wait for load to run
  absl::SleepFor(absl::Seconds(1));
  EXPECT_SUCCESS(match_data_loader.Stop());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 3);

  const auto& export_info_duration =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(export_info_duration.name, "data_loader_data_export_info_duration");
  EXPECT_EQ(export_info_duration.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(export_info_duration.unit, MetricUnit::METRIC_UNIT_MILLISECONDS);
  EXPECT_EQ(export_info_duration.labels.at("IsSuccessful"), "true");
  VerifyMetricClusterLabels(export_info_duration);

  const auto& metadata_duration =
      mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(metadata_duration.name, "data_loader_get_export_metadata_duration");
  EXPECT_EQ(metadata_duration.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(metadata_duration.unit, MetricUnit::METRIC_UNIT_MILLISECONDS);
  EXPECT_EQ(metadata_duration.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(metadata_duration);

  const auto& metadata_fetch_error =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(metadata_fetch_error.name, "data_loader_load_error_count");
  EXPECT_EQ(metadata_fetch_error.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(metadata_fetch_error.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(metadata_fetch_error.value, "1");
  EXPECT_EQ(metadata_fetch_error.labels.at("BackendErrorReason"),
            "Failed to fetch requested data.");
  VerifyMetricLabels(metadata_fetch_error);
}

TEST_F(MatchDataLoaderTest, GetExportMetadataParseError) {
  EXPECT_CALL(*mock_data_provider_, Get)
      .Times(AtMost(1))
      .WillOnce(MockGetInvalidExportMetadata);
  EXPECT_CALL(*mock_orchestrator_client_,
              GetDataExportInfo(A<const GetDataExportInfoRequest&>()))
      .Times(AtMost(1))
      .WillOnce(Return(GetDataExportInfoResponse{
          .data_export_info =
              std::make_shared<DataExportInfo>(data_export_info_)}));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillRepeatedly(Return(SuccessExecutionResult()));
  MatchDataLoader match_data_loader(
      mock_data_provider_, mock_match_data_provider_, mock_match_data_storage_,
      mock_metric_client_, mock_otel_metric_client_, mock_orchestrator_client_,
      mock_crypto_client_, kClusterGroupId, kClusterId, kKmsResourceName,
      kKmsRegion, kKmsWipProvider, kDataLoadingIntervalMins);

  EXPECT_SUCCESS(match_data_loader.Init());
  EXPECT_SUCCESS(match_data_loader.Run());
  // Wait for load to run
  absl::SleepFor(absl::Seconds(1));
  EXPECT_SUCCESS(match_data_loader.Stop());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 3);
  const auto& err_metric = mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(err_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(err_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(err_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(err_metric.value, "1");
  EXPECT_EQ(err_metric.labels.at("BackendErrorReason"),
            "Unable to parse raw export metadata.");
  VerifyMetricLabels(err_metric);
}

TEST_F(MatchDataLoaderTest, LoadWithErrorStartingJobReturnsError) {
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(
          Return(FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR)));
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);

  ExecutionResult result =
      match_data_loader_->Load(data_export_info_, kEncryptedDek);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(match_data_rows_, IsEmpty());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 1);
  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Failed to fetch requested data.");
  VerifyMetricLabels(error_metric);
  // No duration metric is recorded.
}

TEST_F(MatchDataLoaderTest, LoadWithInvalidEncryptedDekReturnsError) {
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).Times(0);

  EXPECT_THAT(match_data_loader_->Load(data_export_info_, "invalid"),
              ResultIs(FailureExecutionResult(
                  MATCH_DATA_LOADER_INVALID_ENCRYPTED_DEK)));
  EXPECT_THAT(match_data_rows_, IsEmpty());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 1);
  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Unable to parse encrypted DEK into keyset.");
  VerifyMetricLabels(error_metric);
}

TEST_F(MatchDataLoaderTest, LoadGetCryptoKeyErrorYieldsEmptyList) {
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey)
      .WillOnce(MockFailedGetCryptoKey);

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  EXPECT_THAT(match_data_rows_, IsEmpty());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 1);
  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Crypto client failed to get Aead from Keyset.");
  VerifyMetricLabels(error_metric);
}

TEST_F(MatchDataLoaderTest, LoadEmptyIsSuccessful) {
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

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  // Validate OpenTelemetry metrics.
  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 5);
  const auto& duration_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(duration_metric.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(duration_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(duration_metric.value.empty());
  EXPECT_EQ(duration_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(duration_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(duration_metric);

  const auto& full_cycle_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(full_cycle_metric.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(full_cycle_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(full_cycle_metric.value.empty());
  EXPECT_EQ(full_cycle_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(full_cycle_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(full_cycle_metric);

  const auto& record_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(record_count_metric.name, "data_loader_record_count");
  EXPECT_EQ(record_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(record_count_metric.value, "0");
  EXPECT_EQ(record_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(record_count_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(record_count_metric);

  const auto& key_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(key_count_metric.name, "data_loader_key_count");
  EXPECT_EQ(key_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(key_count_metric.value, "0");
  EXPECT_EQ(key_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(key_count_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(key_count_metric);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(age_metric.name,
            "data_loader_duration_since_last_refresh_in_seconds");
  EXPECT_EQ(age_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(age_metric.value.empty());
  EXPECT_EQ(age_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  VerifyMetricClusterLabels(age_metric);

  // Validate legacy metrics.
  for (const auto& metric : mock_metric_client_->GetRecordedMetrics()) {
    EXPECT_TRUE(metric.labels.find("IsSuccessful") == metric.labels.end());
  }
}

TEST_F(MatchDataLoaderTest, LoadSingleEntryIsSuccessful) {
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

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  EXPECT_THAT(match_data_rows_,
              ElementsAre(EqualsProto(GetSampleMatchDataRow())));

  // Validate OpenTelemetry metrics.
  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 5);
  const auto& duration_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(duration_metric.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(duration_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(duration_metric.value.empty());
  EXPECT_EQ(duration_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(duration_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(duration_metric);

  const auto& full_cycle_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(full_cycle_metric.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(full_cycle_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(full_cycle_metric.value.empty());
  EXPECT_EQ(full_cycle_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(full_cycle_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(full_cycle_metric);

  const auto& record_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(record_count_metric.name, "data_loader_record_count");
  EXPECT_EQ(record_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(record_count_metric.value, "1");
  EXPECT_EQ(record_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(record_count_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(record_count_metric);

  const auto& key_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(key_count_metric.name, "data_loader_key_count");
  EXPECT_EQ(key_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(key_count_metric.value, "1");
  EXPECT_EQ(key_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(key_count_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(key_count_metric);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(age_metric.name,
            "data_loader_duration_since_last_refresh_in_seconds");
  EXPECT_EQ(age_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(age_metric.value.empty());
  EXPECT_EQ(age_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  VerifyMetricClusterLabels(age_metric);

  // Validate legacy metrics.
  for (const auto& metric : mock_metric_client_->GetRecordedMetrics()) {
    EXPECT_TRUE(metric.labels.find("IsSuccessful") == metric.labels.end());
  }
}

TEST_F(MatchDataLoaderTest, LoadMultipleEntriesIsSuccessful) {
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

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  EXPECT_THAT(match_data_rows_,
              UnorderedElementsAre(EqualsProto(GetSampleMatchDataRow()),
                                   EqualsProto(GetSampleMatchDataRow2())));

  // Validate OpenTelemetry metrics.
  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 5);
  const auto& duration_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(duration_metric.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(duration_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(duration_metric.value.empty());
  EXPECT_EQ(duration_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(duration_metric.labels.size(), 6);
  EXPECT_EQ(duration_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(duration_metric);

  const auto& full_cycle_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(full_cycle_metric.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(full_cycle_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(full_cycle_metric.value.empty());
  EXPECT_EQ(full_cycle_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(full_cycle_metric.labels.size(), 6);
  EXPECT_EQ(full_cycle_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(full_cycle_metric);

  const auto& record_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(record_count_metric.name, "data_loader_record_count");
  EXPECT_EQ(record_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(record_count_metric.value, "2");
  EXPECT_EQ(record_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(record_count_metric.labels.size(), 6);
  EXPECT_EQ(record_count_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(record_count_metric);

  const auto& key_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(key_count_metric.name, "data_loader_key_count");
  EXPECT_EQ(key_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(key_count_metric.value, "2");
  EXPECT_EQ(key_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(key_count_metric.labels.size(), 6);
  EXPECT_EQ(key_count_metric.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(key_count_metric);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(age_metric.name,
            "data_loader_duration_since_last_refresh_in_seconds");
  EXPECT_EQ(age_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(age_metric.value.empty());
  EXPECT_EQ(age_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  VerifyMetricClusterLabels(age_metric);

  // Validate legacy metrics.
  for (const auto& metric : mock_metric_client_->GetRecordedMetrics()) {
    EXPECT_TRUE(metric.labels.find("IsSuccessful") == metric.labels.end());
  }
}

TEST_F(MatchDataLoaderTest, LoadWithImmediateFetchFailureCancelsUpdate) {
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

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));

  EXPECT_THAT(match_data_rows_, IsEmpty());

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 5);
  const auto& full_cycle_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(full_cycle_metric.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(full_cycle_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(full_cycle_metric.value.empty());
  EXPECT_EQ(full_cycle_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(full_cycle_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(full_cycle_metric);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(age_metric.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(age_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(age_metric.value.empty());
  EXPECT_EQ(age_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(age_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(age_metric);

  const auto& record_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(record_count_metric.name, "data_loader_record_count");
  EXPECT_EQ(record_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(record_count_metric.value, "0");
  EXPECT_EQ(record_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(record_count_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(record_count_metric);

  const auto& key_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(key_count_metric.name, "data_loader_key_count");
  EXPECT_EQ(key_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(key_count_metric.value, "0");
  EXPECT_EQ(key_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(key_count_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(key_count_metric);

  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Failed to fetch requested data.");
  VerifyMetricLabels(error_metric);

  // Validate legacy metrics.
  for (const auto& metric : mock_metric_client_->GetRecordedMetrics()) {
    EXPECT_TRUE(metric.labels.find("IsSuccessful") == metric.labels.end());
  }
}

TEST_F(MatchDataLoaderTest, LoadWithSuccessThenFetchFailureCancelsUpdate) {
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

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));

  EXPECT_THAT(match_data_rows_,
              ElementsAre(EqualsProto(GetSampleMatchDataRow())));

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 5);
  const auto& full_cycle_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(full_cycle_metric.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(full_cycle_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(full_cycle_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_FALSE(full_cycle_metric.value.empty());
  EXPECT_EQ(full_cycle_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(full_cycle_metric);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(age_metric.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(age_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_FALSE(age_metric.value.empty());
  EXPECT_EQ(age_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(age_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(age_metric);

  const auto& record_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(record_count_metric.name, "data_loader_record_count");
  EXPECT_EQ(record_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(record_count_metric.value, "1");
  EXPECT_EQ(record_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(record_count_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(record_count_metric);

  const auto& key_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(key_count_metric.name, "data_loader_key_count");
  EXPECT_EQ(key_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(key_count_metric.value, "1");
  EXPECT_EQ(key_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(key_count_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(key_count_metric);

  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Failed to fetch requested data.");
  VerifyMetricLabels(error_metric);

  // Validate legacy metrics.
  for (const auto& metric : mock_metric_client_->GetRecordedMetrics()) {
    EXPECT_TRUE(metric.labels.find("IsSuccessful") == metric.labels.end());
  }
}

TEST_F(MatchDataLoaderTest, LoadWithRetriedFailureIsSuccessful) {
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

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  // Wait for finalize threads to complete
  absl::SleepFor(absl::Seconds(1));

  EXPECT_THAT(match_data_rows_,
              ElementsAre(EqualsProto(GetSampleMatchDataRow())));

  // Validate OpenTelemetry metrics that first load failed but second succeeded.
  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 10);

  const auto& first_full_cycle =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(first_full_cycle.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(first_full_cycle.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(first_full_cycle.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(first_full_cycle.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(first_full_cycle);

  const auto& first_update_duration =
      mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(first_update_duration.name,
            "data_loader_update_duration_in_seconds");
  EXPECT_EQ(first_update_duration.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(first_update_duration.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(first_update_duration.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(first_update_duration);

  const auto& first_record_count =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(first_record_count.name, "data_loader_record_count");
  EXPECT_EQ(first_record_count.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(first_record_count.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(first_record_count.value, "0");
  EXPECT_EQ(first_record_count.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(first_record_count);

  const auto& first_key_count =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(first_key_count.name, "data_loader_key_count");
  EXPECT_EQ(first_key_count.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(first_key_count.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(first_key_count.value, "0");
  EXPECT_EQ(first_key_count.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(first_key_count);

  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Failed to fetch requested data.");
  VerifyMetricLabels(error_metric);

  const auto& update_duration =
      mock_otel_metric_client_->GetRecordedMetrics()[5];
  EXPECT_EQ(update_duration.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(update_duration.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(update_duration.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(update_duration.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(update_duration);

  const auto& second_full_cycle =
      mock_otel_metric_client_->GetRecordedMetrics()[6];
  EXPECT_EQ(second_full_cycle.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(second_full_cycle.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(second_full_cycle.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(second_full_cycle.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(second_full_cycle);

  const auto& second_record_count =
      mock_otel_metric_client_->GetRecordedMetrics()[7];
  EXPECT_EQ(second_record_count.name, "data_loader_record_count");
  EXPECT_EQ(second_record_count.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(second_record_count.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(second_record_count.value, "1");
  EXPECT_EQ(second_record_count.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(second_record_count);

  const auto& second_key_count =
      mock_otel_metric_client_->GetRecordedMetrics()[8];
  EXPECT_EQ(second_key_count.name, "data_loader_key_count");
  EXPECT_EQ(second_key_count.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(second_key_count.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(second_key_count.value, "1");
  EXPECT_EQ(second_key_count.labels.at("IsSuccessful"), "true");
  VerifyMetricLabels(second_key_count);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[9];
  EXPECT_EQ(age_metric.name,
            "data_loader_duration_since_last_refresh_in_seconds");
  EXPECT_EQ(age_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(age_metric.unit, MetricUnit::METRIC_UNIT_SECONDS);
  EXPECT_EQ(age_metric.labels.size(), 2);
  VerifyMetricClusterLabels(age_metric);

  // Validate legacy metrics.
  for (const auto& metric : mock_metric_client_->GetRecordedMetrics()) {
    EXPECT_TRUE(metric.labels.find("IsSuccessful") == metric.labels.end());
  }
}

TEST_F(MatchDataLoaderTest, LoadStartUpdateFailure) {
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(
          Return(FailureExecutionResult(MATCH_DATA_STORAGE_INSERT_ERROR)));
  EXPECT_CALL(*mock_match_data_storage_, Replace).Times(0);
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 1);
  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Failed to insert the match data.");
  VerifyMetricLabels(error_metric);
}

TEST_F(MatchDataLoaderTest, LoadReplaceFailure) {
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithSingleRow);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillOnce(Return(
          FailureExecutionResult(MATCH_DATA_STORAGE_REPLACE_KEY_MISMATCH)));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));

  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 1);
  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Match data rows must match the provided key during replacement.");
  VerifyMetricLabels(error_metric);
}

TEST_F(MatchDataLoaderTest, LoadFinalizeUpdateFailure) {
  EXPECT_CALL(*mock_match_data_provider_, GetMatchData)
      .WillOnce(MockGetMatchDataWithSingleRow);
  EXPECT_CALL(*mock_match_data_storage_, StartUpdate)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_match_data_storage_, Replace)
      .WillRepeatedly(Invoke(this, &MatchDataLoaderTest::CaptureMatchDataRow));
  EXPECT_CALL(*mock_match_data_storage_, FinalizeUpdate)
      .WillOnce(
          Return(FailureExecutionResult(MATCH_DATA_STORAGE_REPLACE_ERROR)));
  EXPECT_CALL(*mock_crypto_client_, GetCryptoKey).WillOnce(MockGetCryptoKey);

  EXPECT_SUCCESS(match_data_loader_->Load(data_export_info_, kEncryptedDek));
  // Wait for finalize thread to run
  absl::SleepFor(absl::Seconds(1));

  // Verify OpenTelemetry metrics. Table update is successful, but full cycle
  // update is not successful.
  ASSERT_EQ(mock_otel_metric_client_->GetRecordedMetrics().size(), 6);

  const auto& update_duration =
      mock_otel_metric_client_->GetRecordedMetrics()[0];
  EXPECT_EQ(update_duration.name, "data_loader_update_duration_in_seconds");
  EXPECT_EQ(update_duration.labels.at("IsSuccessful"), "true");
  EXPECT_FALSE(update_duration.value.empty());
  VerifyMetricLabels(update_duration);

  const auto& error_metric = mock_otel_metric_client_->GetRecordedMetrics()[1];
  EXPECT_EQ(error_metric.name, "data_loader_load_error_count");
  EXPECT_EQ(error_metric.type, MetricType::METRIC_TYPE_COUNTER);
  EXPECT_EQ(error_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(error_metric.value, "1");
  EXPECT_EQ(error_metric.labels.at("BackendErrorReason"),
            "Failed to replace the match data.");
  VerifyMetricLabels(error_metric);

  const auto& dur_full_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[2];
  EXPECT_EQ(dur_full_metric.name,
            "data_loader_update_full_cycle_duration_in_seconds");
  EXPECT_EQ(dur_full_metric.labels.at("IsSuccessful"), "false");
  EXPECT_FALSE(dur_full_metric.value.empty());
  VerifyMetricLabels(dur_full_metric);

  const auto& record_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[3];
  EXPECT_EQ(record_count_metric.name, "data_loader_record_count");
  EXPECT_EQ(record_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(record_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(record_count_metric.value, "1");
  EXPECT_EQ(record_count_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(record_count_metric);

  const auto& key_count_metric =
      mock_otel_metric_client_->GetRecordedMetrics()[4];
  EXPECT_EQ(key_count_metric.name, "data_loader_key_count");
  EXPECT_EQ(key_count_metric.type, MetricType::METRIC_TYPE_GAUGE);
  EXPECT_EQ(key_count_metric.unit, MetricUnit::METRIC_UNIT_COUNT);
  EXPECT_EQ(key_count_metric.value, "1");
  EXPECT_EQ(key_count_metric.labels.at("IsSuccessful"), "false");
  VerifyMetricLabels(key_count_metric);

  const auto& age_metric = mock_otel_metric_client_->GetRecordedMetrics()[5];
  EXPECT_EQ(age_metric.name,
            "data_loader_duration_since_last_refresh_in_seconds");
  EXPECT_FALSE(age_metric.value.empty());
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
