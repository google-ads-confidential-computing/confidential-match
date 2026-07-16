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

#include "cc/match_service/lookup_service_client/lookup_service_client.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/status/status_matchers.h"
#include "absl/strings/str_join.h"
#include "cc/core/async/async_context.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/util/jump_consistent_hasher.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/lookup_service_shard_client/mock_lookup_service_shard_client.h"
#include "cc/match_service/orchestrator_client/mock_orchestrator_client.h"
#include "cc/public/cpio/mock/metric_client/mock_metric_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::cmrt::sdk::metric_service::v1::MetricUnit;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse_Shard;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::cpio::MetricClientInterface;
using ::google::scp::cpio::MockMetricClient;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

constexpr char kClusterGroupId[] = "testClusterGroupId";
constexpr char kUrlBase[] = "lookup-%d.lookupservice.goog/v1/lookup";
constexpr char kGaiaAssociatedData[] = "encrypted_gaia_id";
constexpr int kMaxRecordsPerRequest = 10;
constexpr char kMetricNamespace[] = "test_namespace";

class LookupServiceClientTest : public testing::Test {
 protected:
  LookupServiceClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_orchestrator_client_(std::make_shared<MockOrchestratorClient>()),
        mock_lookup_shard_client_(
            std::make_shared<MockLookupServiceShardClient>()),
        mock_metric_client_(std::make_shared<MockMetricClient>()),
        lookup_client_(std::make_unique<LookupServiceClient>(
            mock_orchestrator_client_.get(), mock_lookup_shard_client_.get(),
            kMaxRecordsPerRequest, kClusterGroupId, mock_metric_client_.get(),
            kMetricNamespace)) {}

  std::shared_ptr<LoggerInterface> logger_;
  std::shared_ptr<MockOrchestratorClient> mock_orchestrator_client_;
  std::shared_ptr<MockLookupServiceShardClient> mock_lookup_shard_client_;
  std::shared_ptr<MockMetricClient> mock_metric_client_;
  std::unique_ptr<LookupServiceClientInterface> lookup_client_;

  void SetUp() override {
    EXPECT_TRUE(lookup_client_->Init().ok());
    EXPECT_TRUE(lookup_client_->Run().ok());
  }

  void TearDown() override { EXPECT_TRUE(lookup_client_->Stop().ok()); }

  GetCurrentShardingSchemeResponse CreateShardingScheme(int num_shards) {
    GetCurrentShardingSchemeResponse response;
    for (int i = 0; i < num_shards; ++i) {
      auto* shard = response.add_shards();
      shard->set_shard_number(i);
      shard->set_server_address_uri(absl::StrFormat(kUrlBase, i));
    }
    response.set_type("jch");
    return response;
  }

  backend::LookupServiceRequest CreateLookupRequest(int num_records) {
    backend::LookupServiceRequest request;
    request.set_key_format(backend::LookupServiceRequest::KEY_FORMAT_HASHED);
    auto* hash_info = request.mutable_hash_info();
    hash_info->set_hash_type(
        backend::LookupServiceRequest_HashInfo_HashType_HASH_TYPE_SHA_256);

    for (int i = 0; i < num_records; ++i) {
      auto* record = request.add_data_records();
      record->mutable_lookup_key()->set_key(std::to_string(i));
    }
    request.add_associated_data_keys(kGaiaAssociatedData);
    return request;
  }
};

void MockGetShardingScheme(const GetCurrentShardingSchemeResponse& response,
                           AsyncContext<GetCurrentShardingSchemeRequest,
                                        GetCurrentShardingSchemeResponse>
                               context) noexcept {
  context.response =
      std::make_shared<GetCurrentShardingSchemeResponse>(response);
  context.Finish();
}

void MockGetShardingSchemeFailure(AsyncContext<GetCurrentShardingSchemeRequest,
                                               GetCurrentShardingSchemeResponse>
                                      context) noexcept {
  context.status = absl::InternalError("err");
  context.Finish();
}

void MockShardLookupSuccess(
    std::unordered_set<std::string>& captured_keys,
    AsyncContext<LookupRequest, LookupResponse> context) noexcept {
  for (const auto& data : context.request->associated_data_keys()) {
    captured_keys.insert(data);
  }
  auto response = std::make_shared<LookupResponse>();
  for (const auto& rec : context.request->data_records()) {
    auto* res = response->add_lookup_results();
    *res->mutable_client_data_record() = rec;
    res->set_status(LookupResult::STATUS_SUCCESS);
  }
  context.response = response;
  context.Finish();
}

void MockShardLookupFailure(
    AsyncContext<LookupRequest, LookupResponse> context) noexcept {
  context.status = Status(backend::Error_Reason_INTERNAL_ERROR, "err");
  context.Finish();
}

TEST_F(LookupServiceClientTest, LookupRecordsReturnsLookupResponse) {
  int num_records = 100;
  auto request_proto = CreateLookupRequest(num_records);
  request_proto.add_associated_data_keys("encrypted_gaia_id");
  auto scheme = CreateShardingScheme(3);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingScheme, scheme, _1));
  std::unordered_set<std::string> associated_data;
  // 100 data records, 3 shards, and 10 records max per shard request
  // Each shard gets 3 full requests, and 1 partially filled request; 4x3 = 12
  // total
  EXPECT_CALL(*mock_lookup_shard_client_, Lookup)
      .Times(12)
      .WillRepeatedly(
          std::bind(MockShardLookupSuccess, std::ref(associated_data), _1));
  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request_proto),
          [&](auto& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_EQ(context.response->lookup_results_size(), num_records);
          },
          logger_);

  lookup_client_->Lookup(context);

  EXPECT_EQ(associated_data.size(), 1);
  EXPECT_EQ(absl::StrJoin(associated_data, ","), kGaiaAssociatedData);
}

TEST_F(LookupServiceClientTest, LookupPreservesOrderAcrossShards) {
  int num_records = 10;
  auto request_proto = CreateLookupRequest(num_records);
  auto scheme = CreateShardingScheme(2);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingScheme, scheme, _1));
  std::unordered_set<std::string> unneeded;
  EXPECT_CALL(*mock_lookup_shard_client_, Lookup)
      .WillRepeatedly(
          std::bind(MockShardLookupSuccess, std::ref(unneeded), _1));
  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request_proto),
          [&](auto& context) {
            EXPECT_THAT(context.status, IsOk());
            ASSERT_EQ(context.response->lookup_results_size(), num_records);
            // Ensure index i in the response corresponds to index i in the
            // request.
            for (int i = 0; i < num_records; ++i) {
              EXPECT_EQ(context.response->lookup_results(i)
                            .client_data_record()
                            .lookup_key()
                            .key(),
                        std::to_string(i))
                  << "Mismatch at index " << i;
            }
          },
          logger_);

  lookup_client_->Lookup(context);
}

TEST_F(LookupServiceClientTest, ShardClientThrowsReturnsFailure) {
  auto request_proto = CreateLookupRequest(1);
  auto scheme = CreateShardingScheme(1);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingScheme, scheme, _1));
  EXPECT_CALL(*mock_lookup_shard_client_, Lookup)
      .WillOnce(std::bind(MockShardLookupFailure, _1));
  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request_proto),
          [](auto& context) {
            EXPECT_THAT(context.status, StatusIs(absl::StatusCode::kInternal));
            EXPECT_EQ(context.response, nullptr);
          },
          logger_);

  lookup_client_->Lookup(context);
}

TEST_F(LookupServiceClientTest, OrchestratorClientThrowsReturnsFailure) {
  auto request_proto = CreateLookupRequest(1);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingSchemeFailure, _1));
  // Shard client should NOT be called
  EXPECT_CALL(*mock_lookup_shard_client_, Lookup).Times(0);
  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request_proto),
          [](auto& context) {
            EXPECT_THAT(context.status, StatusIs(absl::StatusCode::kInternal));
            EXPECT_EQ(context.response, nullptr);
          },
          logger_);

  lookup_client_->Lookup(context);
}

TEST_F(LookupServiceClientTest, LookupUsesDecryptedKeyForShardingIfPresent) {
  // Setup a scheme with enough shards to make distribution distinct
  int num_shards = 50;
  auto scheme = CreateShardingScheme(num_shards);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingScheme, scheme, _1));
  backend::LookupServiceRequest request;
  request.set_key_format(backend::LookupServiceRequest::KEY_FORMAT_HASHED);
  auto* hash_info = request.mutable_hash_info();
  hash_info->set_hash_type(
      backend::LookupServiceRequest_HashInfo_HashType_HASH_TYPE_SHA_256);
  auto* record = request.add_data_records();
  // Define distinct keys
  std::string primary_key = "primary_key_value";
  std::string decrypted_key = "decrypted_key_value";
  record->mutable_lookup_key()->set_key(primary_key);
  record->mutable_lookup_key()->set_decrypted_key(decrypted_key);
  // Calculate the expected shard index based on the decrypted_key
  int expected_shard_idx = JumpConsistentHash(decrypted_key, num_shards);
  std::string expected_endpoint = absl::StrFormat(kUrlBase, expected_shard_idx);
  std::unordered_set<std::string> unneeded;
  // Verify that the Lookup call is made to the endpoint corresponding to the
  // decrypted_key
  EXPECT_CALL(*mock_lookup_shard_client_, Lookup(_, expected_endpoint))
      .WillOnce(std::bind(MockShardLookupSuccess, std::ref(unneeded), _1));
  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request),
          [&](auto& context) {
            EXPECT_THAT(context.status, IsOk());
            // Verify the result is present
            EXPECT_EQ(context.response->lookup_results_size(), 1);
          },
          logger_);

  lookup_client_->Lookup(context);
}

TEST_F(LookupServiceClientTest, LookupRecordsMetrics) {
  int num_records = 2;
  auto request_proto = CreateLookupRequest(num_records);
  request_proto.set_application(backend::Application::APPLICATION_ECL);
  request_proto.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED);

  auto scheme = CreateShardingScheme(2);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingScheme, scheme, _1));

  std::unordered_set<std::string> associated_data;
  EXPECT_CALL(*mock_lookup_shard_client_, Lookup)
      .WillRepeatedly(
          std::bind(MockShardLookupSuccess, std::ref(associated_data), _1));

  std::vector<std::string> seen_lookup_shards;
  EXPECT_CALL(*mock_metric_client_, PutMetrics)
      .Times(4)
      .WillRepeatedly([&seen_lookup_shards](auto& context) {
        context.result = scp::core::SuccessExecutionResult();
        const auto& metrics = context.request->metrics();
        ASSERT_EQ(metrics.size(), 1);
        const auto& metric = metrics.at(0);
        if (metric.name() == "LookupServerRequestCount") {
          EXPECT_EQ(metric.value(), "1");
          EXPECT_EQ(metric.unit(), MetricUnit::METRIC_UNIT_COUNT);
        } else if (metric.name() == "LookupServerRequestLatency") {
          // just check it's not empty
          EXPECT_FALSE(metric.value().empty());
          EXPECT_EQ(metric.unit(), MetricUnit::METRIC_UNIT_MILLISECONDS);
        } else {
          FAIL() << "Unexpected metric name: " << metric.name();
        }
        const auto& labels = metric.labels();
        // We just want to test that there is a value for LookupServerShard, we
        // validate them via seen_lookup_shards later.
        ASSERT_THAT(labels, UnorderedElementsAre(
                                Pair("LookupServerShard", _),
                                Pair("Application", "APPLICATION_ECL"),
                                Pair("KeyFormat", "KEY_FORMAT_HASHED")));
        seen_lookup_shards.push_back(labels.at("LookupServerShard"));
        context.Finish();
      });

  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request_proto),
          [&](auto& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(seen_lookup_shards,
                        UnorderedElementsAre("0", "0", "1", "1"));
          },
          logger_);

  lookup_client_->Lookup(context);
}

TEST_F(LookupServiceClientTest, LookupRecordsFailureMetrics) {
  int num_records = 1;
  auto request_proto = CreateLookupRequest(num_records);
  request_proto.set_application(backend::Application::APPLICATION_ECL);
  request_proto.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED);

  auto scheme = CreateShardingScheme(1);
  EXPECT_CALL(*mock_orchestrator_client_, GetCurrentShardingScheme)
      .WillOnce(std::bind(MockGetShardingScheme, scheme, _1));

  EXPECT_CALL(*mock_lookup_shard_client_, Lookup)
      .WillOnce(std::bind(MockShardLookupFailure, _1));

  EXPECT_CALL(*mock_metric_client_, PutMetrics)
      .Times(3)  // 1 for count, 1 for latency, 1 for error count
      .WillRepeatedly([](auto& context) {
        const auto& metrics = context.request->metrics();
        ASSERT_EQ(metrics.size(), 1);
        const auto& metric = metrics.at(0);
        if (metric.name() == "LookupServerRequestCount") {
          EXPECT_EQ(metric.value(), "1");
          EXPECT_EQ(metric.unit(), MetricUnit::METRIC_UNIT_COUNT);
        } else if (metric.name() == "LookupServerRequestLatency") {
          EXPECT_FALSE(metric.value().empty());
          EXPECT_EQ(metric.unit(), MetricUnit::METRIC_UNIT_MILLISECONDS);
        } else if (metric.name() == "LookupServerRequestErrorCount") {
          EXPECT_EQ(metric.value(), "1");
          EXPECT_EQ(metric.unit(), MetricUnit::METRIC_UNIT_COUNT);
          const auto& labels = metric.labels();
          EXPECT_EQ(labels.at("BackendErrorReason"), "INTERNAL_ERROR");
        } else {
          FAIL() << "Unexpected metric name: " << metric.name();
        }
        context.Finish();
      });

  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      context(
          std::make_shared<backend::LookupServiceRequest>(request_proto),
          [&](auto& context) {
            EXPECT_THAT(context.status, StatusIs(absl::StatusCode::kInternal));
          },
          logger_);

  lookup_client_->Lookup(context);
}

}  // namespace
}  // namespace google::confidential_match::match_service
