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

#include "cc/lookup_server/coordinator_client/src/cpio_cached_coordinator_client.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/utils/key_fetching/interface/key_fetcher_with_cache_interface.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::v1::KeyCoordinatorConfiguration;
using ::google::confidential_match::lookup_server::proto_api::EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::Timestamp;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::IsSuccessfulAndHolds;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::SubstituteAndParseTextToProto;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::Key;
using ::google::scp::cpio::KeyFetcherWithCacheInterface;
using ::testing::_;
using ::testing::Eq;
using ::testing::Return;
using ::testing::Test;

constexpr absl::string_view kKeyId = "test-key-id-1";
constexpr absl::string_view kPublicKey = "test-public-key";
constexpr absl::string_view kPrivateKey = "test-private-key";

constexpr absl::string_view kTestEndpoint1 = "https://coord1.example.com";
constexpr absl::string_view kTestAccountIdentity1 =
    "sa1@example.iam.gserviceaccount.com";
constexpr absl::string_view kTestWipProvider1 =
    "projects/123/locations/global/workloadIdentityPools/pool1/providers/prov1";
constexpr absl::string_view kTestAudienceUrl1 =
    "https://coord1.example.com/function";

constexpr absl::string_view kTestEndpoint2 = "https://coord2.example.com";
constexpr absl::string_view kTestAccountIdentity2 =
    "sa2@example.iam.gserviceaccount.com";
constexpr absl::string_view kTestWipProvider2 =
    "projects/456/locations/global/workloadIdentityPools/pool2/providers/prov2";
constexpr absl::string_view kTestAudienceUrl2 =
    "https://coord2.example.com/function";

class MockKeyFetcherWithCache : public KeyFetcherWithCacheInterface {
 public:
  MOCK_METHOD(ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(ExecutionResult, Stop, (), (noexcept, override));
  MOCK_METHOD(ExecutionResultOr<Key>, GetKey, (const std::string&),
              (noexcept, override));
  MOCK_METHOD(ExecutionResultOr<std::vector<Key>>, GetValidKeys, (Timestamp),
              (noexcept, override));
};

KeyCoordinatorConfiguration CreateDefaultKeyCoordinatorConfiguration() {
  return SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
      R"pb(
        private_key_endpoints {
          endpoint: "$0"
          account_identity: "$1"
          gcp_wip_provider: "$2"
          gcp_cloud_function_url: "$3"
        }
      )pb",
      kTestEndpoint1, kTestAccountIdentity1, kTestWipProvider1,
      kTestAudienceUrl1);
}

GetHybridKeyRequest CreateDefaultGetHybridKeyRequest(
    absl::string_view key_id = kKeyId) {
  return SubstituteAndParseTextToProto<GetHybridKeyRequest>(
      R"pb(
        key_id: "$0"
        coordinators {
          key_service_endpoint: "$1"
          account_identity: "$2"
          kms_wip_provider: "$3"
          key_service_audience_url: "$4"
        }
      )pb",
      key_id, kTestEndpoint1, kTestAccountIdentity1, kTestWipProvider1,
      kTestAudienceUrl1);
}

class CpioCachedCoordinatorClientTest : public Test {
 protected:
  CpioCachedCoordinatorClientTest()
      : mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_key_fetcher_(std::make_shared<MockKeyFetcherWithCache>()),
        client_(std::make_unique<CpioCachedCoordinatorClient>(
            mock_async_executor_,
            absl::flat_hash_map<std::string,
                                std::shared_ptr<KeyFetcherWithCacheInterface>>{
                {StringifyEndpoints(CreateDefaultKeyCoordinatorConfiguration()),
                 mock_key_fetcher_}})) {
    mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
      work();
      return SuccessExecutionResult();
    };
  }

  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::shared_ptr<MockKeyFetcherWithCache> mock_key_fetcher_;
  std::unique_ptr<CpioCachedCoordinatorClient> client_;
};

TEST_F(CpioCachedCoordinatorClientTest, InitSuccess) {
  EXPECT_SUCCESS(client_->Init());
}

TEST_F(CpioCachedCoordinatorClientTest, RunSuccess) {
  EXPECT_SUCCESS(client_->Run());
}

TEST_F(CpioCachedCoordinatorClientTest, StopSuccess) {
  EXPECT_SUCCESS(client_->Stop());
}

TEST_F(CpioCachedCoordinatorClientTest, NullParametersReturnError) {
  CpioCachedCoordinatorClient null_client(
      nullptr,
      absl::flat_hash_map<std::string,
                          std::shared_ptr<KeyFetcherWithCacheInterface>>{});
  EXPECT_THAT(null_client.Init(),
              ResultIs(FailureExecutionResult(
                  COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));

  GetHybridKeyRequest request = CreateDefaultGetHybridKeyRequest();
  EXPECT_THAT(null_client.GetHybridKey(request),
              ResultIs(FailureExecutionResult(
                  COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));

  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request =
      std::make_shared<GetHybridKeyRequest>(CreateDefaultGetHybridKeyRequest());
  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result,
                    ResultIs(FailureExecutionResult(
                        COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));
        is_complete = true;
      };
  null_client.GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioCachedCoordinatorClientTest, GetHybridKeySyncSuccess) {
  Key fetched_key;
  fetched_key.key_id = kKeyId;
  fetched_key.public_key = kPublicKey;
  fetched_key.private_key = kPrivateKey;

  EXPECT_CALL(*mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce(Return(fetched_key));

  GetHybridKeyRequest request = CreateDefaultGetHybridKeyRequest();

  auto response_or = client_->GetHybridKey(request);
  ASSERT_SUCCESS(response_or);
  GetHybridKeyResponse expected_response =
      SubstituteAndParseTextToProto<GetHybridKeyResponse>(
          R"pb(
            hybrid_key {
              key_id: "$0"
              public_key: "$1"
              private_key: "$2"
            }
          )pb",
          kKeyId, kPublicKey, kPrivateKey);
  EXPECT_THAT(*response_or, EqualsProto(expected_response));
}

TEST_F(CpioCachedCoordinatorClientTest, GetHybridKeyAsyncSuccess) {
  Key fetched_key;
  fetched_key.key_id = kKeyId;
  fetched_key.public_key = kPublicKey;
  fetched_key.private_key = kPrivateKey;

  EXPECT_CALL(*mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce(Return(fetched_key));

  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request =
      std::make_shared<GetHybridKeyRequest>(CreateDefaultGetHybridKeyRequest());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        ASSERT_NE(ctx.response, nullptr);
        GetHybridKeyResponse expected_response =
            SubstituteAndParseTextToProto<GetHybridKeyResponse>(
                R"pb(
                  hybrid_key {
                    key_id: "$0"
                    public_key: "$1"
                    private_key: "$2"
                  }
                )pb",
                kKeyId, kPublicKey, kPrivateKey);
        EXPECT_THAT(*ctx.response, EqualsProto(expected_response));
        is_complete = true;
      };

  client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioCachedCoordinatorClientTest, AsyncScheduleFailureReturnsError) {
  mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
    return FailureExecutionResult(123);
  };

  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request =
      std::make_shared<GetHybridKeyRequest>(CreateDefaultGetHybridKeyRequest());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result, ResultIs(FailureExecutionResult(123)));
        is_complete = true;
      };

  client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioCachedCoordinatorClientTest, GetHybridKeyMissingKeyIdReturnsError) {
  GetHybridKeyRequest request = CreateDefaultGetHybridKeyRequest("");

  auto response_or = client_->GetHybridKey(request);
  EXPECT_THAT(response_or.result(),
              ResultIs(FailureExecutionResult(
                  COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));
}

TEST_F(CpioCachedCoordinatorClientTest, GetHybridKeyFetcherFailure) {
  constexpr uint64_t kFetcherErrorCode = 12345;
  EXPECT_CALL(*mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce(Return(FailureExecutionResult(kFetcherErrorCode)));

  GetHybridKeyRequest request = CreateDefaultGetHybridKeyRequest();

  auto response_or = client_->GetHybridKey(request);
  EXPECT_THAT(response_or.result(),
              ResultIs(FailureExecutionResult(kFetcherErrorCode)));
}

TEST_F(CpioCachedCoordinatorClientTest,
       GetHybridKeyAsyncMissingKeyIdReturnsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>(
      CreateDefaultGetHybridKeyRequest(""));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result,
                    ResultIs(FailureExecutionResult(
                        COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));
        is_complete = true;
      };

  client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioCachedCoordinatorClientTest, GetHybridKeyAsyncFetcherFailure) {
  constexpr uint64_t kFetcherErrorCode = 12345;
  EXPECT_CALL(*mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce(Return(FailureExecutionResult(kFetcherErrorCode)));

  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request =
      std::make_shared<GetHybridKeyRequest>(CreateDefaultGetHybridKeyRequest());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result,
                    ResultIs(FailureExecutionResult(kFetcherErrorCode)));
        is_complete = true;
      };

  client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioCachedCoordinatorClientTest,
       ResolveKeyFetcherNotFoundWithUnknownCoordinator) {
  auto mock_fetcher_a = std::make_shared<MockKeyFetcherWithCache>();
  auto mock_fetcher_b = std::make_shared<MockKeyFetcherWithCache>();

  auto key_coordinator_config_a =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints { endpoint: "$0" }
          )pb",
          kTestEndpoint1);

  auto key_coordinator_config_b =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints { endpoint: "$0" }
          )pb",
          kTestEndpoint2);

  absl::flat_hash_map<std::string,
                      std::shared_ptr<KeyFetcherWithCacheInterface>>
      map;
  map[StringifyEndpoints(key_coordinator_config_a)] = mock_fetcher_a;
  map[StringifyEndpoints(key_coordinator_config_b)] = mock_fetcher_b;

  CpioCachedCoordinatorClient multi_client(mock_async_executor_, map);

  auto request = SubstituteAndParseTextToProto<GetHybridKeyRequest>(
      R"pb(
        key_id: "$0"
        coordinators {
          key_service_endpoint: "https://unknown-coordinator.example.com"
        }
      )pb",
      kKeyId);

  auto response_or = multi_client.GetHybridKey(request);
  EXPECT_THAT(response_or.result(),
              ResultIs(FailureExecutionResult(
                  COORDINATOR_CLIENT_UNKNOWN_COORDINATOR_ERROR)));
}

TEST_F(CpioCachedCoordinatorClientTest,
       ResolveKeyFetcherNotFoundWithEmptyCoordinatorsAndMultipleSets) {
  auto mock_fetcher_a = std::make_shared<MockKeyFetcherWithCache>();
  auto mock_fetcher_b = std::make_shared<MockKeyFetcherWithCache>();

  auto key_coordinator_config_a =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints { endpoint: "$0" }
          )pb",
          kTestEndpoint1);

  auto key_coordinator_config_b =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints { endpoint: "$0" }
          )pb",
          kTestEndpoint2);

  absl::flat_hash_map<std::string,
                      std::shared_ptr<KeyFetcherWithCacheInterface>>
      map;
  map[StringifyEndpoints(key_coordinator_config_a)] = mock_fetcher_a;
  map[StringifyEndpoints(key_coordinator_config_b)] = mock_fetcher_b;

  CpioCachedCoordinatorClient multi_client(mock_async_executor_, map);

  auto request = SubstituteAndParseTextToProto<GetHybridKeyRequest>(
      R"pb(
        key_id: "$0"
      )pb",
      kKeyId);

  auto response_or = multi_client.GetHybridKey(request);
  EXPECT_THAT(response_or.result(),
              ResultIs(FailureExecutionResult(
                  COORDINATOR_CLIENT_UNKNOWN_COORDINATOR_ERROR)));
}

TEST_F(CpioCachedCoordinatorClientTest,
       GetHybridKeyMultipleCoordinatorSetsRouting) {
  auto mock_fetcher_a = std::make_shared<MockKeyFetcherWithCache>();
  auto mock_fetcher_b = std::make_shared<MockKeyFetcherWithCache>();

  auto key_coordinator_config_a =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints {
              endpoint: "https://coord-a.endpoint"
              account_identity: "account-a"
              gcp_wip_provider: "wip-a"
              gcp_cloud_function_url: "aud-a"
            }
          )pb");

  auto key_coordinator_config_b =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints {
              endpoint: "https://coord-b.endpoint"
              account_identity: "account-b"
              gcp_wip_provider: "wip-b"
              gcp_cloud_function_url: "aud-b"
            }
          )pb");

  absl::flat_hash_map<std::string,
                      std::shared_ptr<KeyFetcherWithCacheInterface>>
      map;
  map[StringifyEndpoints(key_coordinator_config_a)] = mock_fetcher_a;
  map[StringifyEndpoints(key_coordinator_config_b)] = mock_fetcher_b;

  CpioCachedCoordinatorClient multi_client(mock_async_executor_, map);

  Key key_b;
  key_b.key_id = kKeyId;
  key_b.public_key = "pub-b";
  key_b.private_key = "priv-b";

  EXPECT_CALL(*mock_fetcher_b, GetKey(std::string(kKeyId)))
      .WillOnce(Return(key_b));
  EXPECT_CALL(*mock_fetcher_a, GetKey(_)).Times(0);

  auto request = SubstituteAndParseTextToProto<GetHybridKeyRequest>(
      R"pb(
        key_id: "$0"
        coordinators {
          key_service_endpoint: "https://coord-b.endpoint"
          account_identity: "account-b"
          kms_wip_provider: "wip-b"
          key_service_audience_url: "aud-b"
        }
      )pb",
      kKeyId);

  auto response_or = multi_client.GetHybridKey(request);
  ASSERT_SUCCESS(response_or);
  GetHybridKeyResponse expected_response =
      SubstituteAndParseTextToProto<GetHybridKeyResponse>(
          R"pb(
            hybrid_key {
              key_id: "$0"
              public_key: "pub-b"
              private_key: "priv-b"
            }
          )pb",
          kKeyId);
  EXPECT_THAT(*response_or, EqualsProto(expected_response));
}

TEST(StringifyTest, StringifyEndpointsAndCoordinatorsEquivalent) {
  auto key_coordinator_config =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints {
              endpoint: "$0"
              account_identity: "$1"
              gcp_wip_provider: "$2"
              gcp_cloud_function_url: "$3"
            }
            private_key_endpoints {
              endpoint: "$4"
              account_identity: "$5"
              gcp_wip_provider: "$6"
              gcp_cloud_function_url: "$7"
            }
          )pb",
          kTestEndpoint1, kTestAccountIdentity1, kTestWipProvider1,
          kTestAudienceUrl1, kTestEndpoint2, kTestAccountIdentity2,
          kTestWipProvider2, kTestAudienceUrl2);

  auto request = SubstituteAndParseTextToProto<GetHybridKeyRequest>(
      R"pb(
        key_id: "$0"
        coordinators {
          key_service_endpoint: "$1"
          account_identity: "completely-different-account-identity"
          kms_wip_provider: "$2"
          key_service_audience_url: "$3"
        }
        coordinators {
          key_service_endpoint: "$4"
          kms_wip_provider: "$5"
          key_service_audience_url: "$6"
        }
      )pb",
      kKeyId, kTestEndpoint1, kTestWipProvider1, kTestAudienceUrl1,
      kTestEndpoint2, kTestWipProvider2, kTestAudienceUrl2);

  std::string endpoints_str = StringifyEndpoints(key_coordinator_config);
  std::string coordinators_str = StringifyCoordinators(request.coordinators());

  EXPECT_FALSE(endpoints_str.empty());
  EXPECT_EQ(endpoints_str, coordinators_str);
}

TEST(StringifyTest, StringifyEncryptionKeyInfoCoordinatorsEquivalent) {
  auto key_coordinator_config =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints {
              endpoint: "$0"
              account_identity: "$1"
              gcp_wip_provider: "$2"
              gcp_cloud_function_url: "$3"
            }
            private_key_endpoints {
              endpoint: "$4"
              account_identity: "$5"
              gcp_wip_provider: "$6"
              gcp_cloud_function_url: "$7"
            }
          )pb",
          kTestEndpoint1, kTestAccountIdentity1, kTestWipProvider1,
          kTestAudienceUrl1, kTestEndpoint2, kTestAccountIdentity2,
          kTestWipProvider2, kTestAudienceUrl2);

  auto coordinator_key_info =
      SubstituteAndParseTextToProto<EncryptionKeyInfo::CoordinatorKeyInfo>(
          R"pb(
            coordinator_info {
              key_service_endpoint: "$0"
              kms_identity: "completely-different-kms-identity"
              kms_wip_provider: "$1"
              key_service_audience_url: "$2"
            }
            coordinator_info {
              key_service_endpoint: "$3"
              kms_wip_provider: "$4"
              key_service_audience_url: "$5"
            }
          )pb",
          kTestEndpoint1, kTestWipProvider1, kTestAudienceUrl1,
          kTestEndpoint2, kTestWipProvider2, kTestAudienceUrl2);

  std::string endpoints_str = StringifyEndpoints(key_coordinator_config);
  std::string coordinators_str =
      StringifyCoordinators(coordinator_key_info.coordinator_info());

  EXPECT_FALSE(endpoints_str.empty());
  EXPECT_EQ(endpoints_str, coordinators_str);
}

TEST(StringifyTest, StringifyEndpointsAndCoordinatorsSortsByEndpoint) {
  auto key_coordinator_config_reverse =
      SubstituteAndParseTextToProto<KeyCoordinatorConfiguration>(
          R"pb(
            private_key_endpoints {
              endpoint: "$4"
              account_identity: "$5"
              gcp_wip_provider: "$6"
              gcp_cloud_function_url: "$7"
            }
            private_key_endpoints {
              endpoint: "$0"
              account_identity: "$1"
              gcp_wip_provider: "$2"
              gcp_cloud_function_url: "$3"
            }
          )pb",
          kTestEndpoint1, kTestAccountIdentity1, kTestWipProvider1,
          kTestAudienceUrl1, kTestEndpoint2, kTestAccountIdentity2,
          kTestWipProvider2, kTestAudienceUrl2);

  auto request_reverse = SubstituteAndParseTextToProto<GetHybridKeyRequest>(
      R"pb(
        key_id: "$0"
        coordinators {
          key_service_endpoint: "$4"
          kms_wip_provider: "$5"
          key_service_audience_url: "$6"
        }
        coordinators {
          key_service_endpoint: "$1"
          account_identity: "completely-different-account-identity"
          kms_wip_provider: "$2"
          key_service_audience_url: "$3"
        }
      )pb",
      kKeyId, kTestEndpoint1, kTestWipProvider1, kTestAudienceUrl1,
      kTestEndpoint2, kTestWipProvider2, kTestAudienceUrl2);

  auto coordinator_key_info_reverse =
      SubstituteAndParseTextToProto<EncryptionKeyInfo::CoordinatorKeyInfo>(
          R"pb(
            coordinator_info {
              key_service_endpoint: "$3"
              kms_wip_provider: "$4"
              key_service_audience_url: "$5"
            }
            coordinator_info {
              key_service_endpoint: "$0"
              kms_identity: "completely-different-kms-identity"
              kms_wip_provider: "$1"
              key_service_audience_url: "$2"
            }
          )pb",
          kTestEndpoint1, kTestWipProvider1, kTestAudienceUrl1, kTestEndpoint2,
          kTestWipProvider2, kTestAudienceUrl2);

  std::string expected =
      absl::StrCat(kTestEndpoint1, kTestWipProvider1, kTestAudienceUrl1,
                   kTestEndpoint2, kTestWipProvider2, kTestAudienceUrl2);

  EXPECT_EQ(StringifyEndpoints(key_coordinator_config_reverse), expected);
  EXPECT_EQ(StringifyCoordinators(request_reverse.coordinators()), expected);
  EXPECT_EQ(
      StringifyCoordinators(coordinator_key_info_reverse.coordinator_info()),
      expected);
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
