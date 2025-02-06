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

#include "cc/lookup_server/coordinator_client/src/cached_coordinator_client.h"

#include <ctime>
#include <memory>

#include "absl/strings/escaping.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/async_executor/src/async_executor.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/coordinator_client/mock/fake_coordinator_client.h"
#include "cc/lookup_server/coordinator_client/mock/mock_coordinator_client.h"
#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "cc/lookup_server/interface/coordinator_client_interface.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutor;
using ::google::scp::core::AsyncExecutorInterface;
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
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;
using ::testing::Eq;
using ::testing::Return;

constexpr absl::string_view kKeyId = "test-key-id-1";
constexpr absl::string_view kKeyId2 = "test-key-id-2";
constexpr absl::string_view kKeyServiceEndpoint =
    "https://test-private-key-service-1.google.com/v1alpha";
constexpr absl::string_view kKeyServiceEndpoint2 =
    "https://test-private-key-service-2.google.com/v1alpha/";
constexpr absl::string_view kAccountIdentity =
    "test-verified-user-1@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kAccountIdentity2 =
    "test-verified-user-2@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kWipProvider =
    "projects/1/locations/global/workloadIdentityPools/test-wip/providers/"
    "test-provider";
constexpr absl::string_view kWipProvider2 =
    "projects/2/locations/global/workloadIdentityPools/test-wip/providers/"
    "test-provider";
constexpr absl::string_view kKeyServiceAudienceUrl =
    "https://test-key-service-audience-url-1.a.run.app";
constexpr absl::string_view kKeyServiceAudienceUrl2 =
    "https://test-key-service-audience-url-2.a.run.app";
constexpr absl::string_view kPublicKey = "test-public-key";
constexpr absl::string_view kPublicKey2 = "test-public-key-2";
constexpr absl::string_view kPrivateKey = "test-private-key";
constexpr absl::string_view kPrivateKey2 = "test-private-key-2";

class CachedCoordinatorClientTest : public testing::Test {
 protected:
  CachedCoordinatorClientTest()
      : mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_base_coordinator_client_(
            std::make_shared<MockCoordinatorClient>()),
        cached_coordinator_client_(std::make_unique<CachedCoordinatorClient>(
            mock_async_executor_, mock_base_coordinator_client_)) {
    mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
      return SuccessExecutionResult();
    };
    mock_async_executor_->schedule_for_mock =
        [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
          return SuccessExecutionResult();
        };
  }

  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::shared_ptr<MockCoordinatorClient> mock_base_coordinator_client_;
  std::unique_ptr<CoordinatorClientInterface> cached_coordinator_client_;
};

// Helper to simulate a successful response from the base coordinator client.
void MockGetHybridKeyWithResponse(
    const GetHybridKeyResponse& response,
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        key_context) noexcept {
  key_context.result = SuccessExecutionResult();
  key_context.response = std::make_shared<GetHybridKeyResponse>(response);
  key_context.Finish();
}

// Helper to simulate a failed response from the base coordinator client.
void MockGetHybridKeyWithFailure(
    const ExecutionResult& failure_result,
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        key_context) noexcept {
  key_context.result = failure_result;
  key_context.Finish();
}

TEST_F(CachedCoordinatorClientTest, GetHybridKeyYieldsSuccess) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyResponse mocked_response;
  mocked_response.mutable_hybrid_key()->set_key_id(kKeyId);
  mocked_response.mutable_hybrid_key()->set_public_key(kPublicKey);
  mocked_response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, mocked_response, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(ctx.response->hybrid_key().key_id(), kKeyId);
        EXPECT_EQ(ctx.response->hybrid_key().public_key(), kPublicKey);
        EXPECT_EQ(ctx.response->hybrid_key().private_key(), kPrivateKey);
        is_complete = true;
      };

  cached_coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CachedCoordinatorClientTest, GetHybridKeySyncReturnsSuccess) {
  GetHybridKeyRequest request;
  request.set_key_id(kKeyId);
  GetHybridKeyResponse mocked_response;
  mocked_response.mutable_hybrid_key()->set_key_id(kKeyId);
  mocked_response.mutable_hybrid_key()->set_public_key(kPublicKey);
  mocked_response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, mocked_response, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or =
      cached_coordinator_client_->GetHybridKey(request);

  EXPECT_SUCCESS(response_or);
  EXPECT_EQ(response_or->hybrid_key().key_id(), kKeyId);
  EXPECT_EQ(response_or->hybrid_key().public_key(), kPublicKey);
  EXPECT_EQ(response_or->hybrid_key().private_key(), kPrivateKey);
}

TEST_F(CachedCoordinatorClientTest, GetHybridKeyWithAsyncFailureYieldsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(
          MockGetHybridKeyWithFailure,
          FailureExecutionResult(COORDINATOR_CLIENT_KEY_FETCH_ERROR), _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result, ResultIs(FailureExecutionResult(
                                    COORDINATOR_CLIENT_KEY_FETCH_ERROR)));
        is_complete = true;
      };

  cached_coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CachedCoordinatorClientTest, GetHybridKeySyncWithFailureReturnsError) {
  GetHybridKeyRequest request;
  request.set_key_id(kKeyId);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(
          MockGetHybridKeyWithFailure,
          FailureExecutionResult(COORDINATOR_CLIENT_KEY_FETCH_ERROR), _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or =
      cached_coordinator_client_->GetHybridKey(request);

  EXPECT_THAT(
      response_or,
      ResultIs(FailureExecutionResult(COORDINATOR_CLIENT_KEY_FETCH_ERROR)));
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyRepeatedlyWithSameRequestUsesCache) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator =
      context.request->add_coordinators();
  coordinator->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator->set_account_identity(kAccountIdentity);
  coordinator->set_kms_wip_provider(kWipProvider);
  coordinator->set_key_service_audience_url(kKeyServiceAudienceUrl);
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context2(context);
  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_key_id(kKeyId);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(ctx.response->hybrid_key().key_id(), kKeyId);
        EXPECT_EQ(ctx.response->hybrid_key().public_key(), kPublicKey);
        EXPECT_EQ(ctx.response->hybrid_key().private_key(), kPrivateKey);
        is_complete = true;
      };
  std::atomic<bool> is_complete2 = false;
  context2.callback =
      [&is_complete2](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(ctx.response->hybrid_key().key_id(), kKeyId);
        EXPECT_EQ(ctx.response->hybrid_key().public_key(), kPublicKey);
        EXPECT_EQ(ctx.response->hybrid_key().private_key(), kPrivateKey);
        is_complete2 = true;
      };

  cached_coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
  cached_coordinator_client_->GetHybridKey(context2);
  WaitUntil([&]() { return is_complete2.load(); });
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyRepeatedlyWithDifferentRequestsReturnsCorrectEntry) {
  GetHybridKeyRequest request1;
  request1.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request1.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator1->set_account_identity(kAccountIdentity);
  coordinator1->set_kms_wip_provider(kWipProvider);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl);

  GetHybridKeyRequest request2;
  request2.set_key_id(kKeyId2);
  GetHybridKeyRequest::Coordinator* coordinator2 = request2.add_coordinators();
  coordinator2->set_key_service_endpoint(kKeyServiceEndpoint2);
  coordinator2->set_account_identity(kAccountIdentity2);
  coordinator2->set_kms_wip_provider(kWipProvider2);
  coordinator2->set_key_service_audience_url(kKeyServiceAudienceUrl2);

  GetHybridKeyResponse response1;
  response1.mutable_hybrid_key()->set_key_id(kKeyId);
  response1.mutable_hybrid_key()->set_public_key(kPublicKey);
  response1.mutable_hybrid_key()->set_private_key(kPrivateKey);
  GetHybridKeyResponse response2;
  response2.mutable_hybrid_key()->set_key_id(kKeyId2);
  response2.mutable_hybrid_key()->set_public_key(kPublicKey2);
  response2.mutable_hybrid_key()->set_private_key(kPrivateKey2);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response1, _1))
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response2, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or1_1 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2_1 =
      cached_coordinator_client_->GetHybridKey(request2);
  ExecutionResultOr<GetHybridKeyResponse> response_or1_2 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2_2 =
      cached_coordinator_client_->GetHybridKey(request2);

  EXPECT_THAT(response_or1_1, IsSuccessfulAndHolds(EqualsProto(response1)));
  EXPECT_THAT(response_or1_2, IsSuccessfulAndHolds(EqualsProto(response1)));
  EXPECT_THAT(response_or2_1, IsSuccessfulAndHolds(EqualsProto(response2)));
  EXPECT_THAT(response_or2_2, IsSuccessfulAndHolds(EqualsProto(response2)));
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyWithDifferentKeyIdDoesNotUseCache) {
  GetHybridKeyRequest request1;
  request1.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request1.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator1->set_account_identity(kAccountIdentity);
  coordinator1->set_kms_wip_provider(kWipProvider);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl);
  GetHybridKeyRequest request2(request1);
  request2.set_key_id(kKeyId2);

  GetHybridKeyResponse response1;
  response1.mutable_hybrid_key()->set_key_id(kKeyId);
  response1.mutable_hybrid_key()->set_public_key(kPublicKey);
  response1.mutable_hybrid_key()->set_private_key(kPrivateKey);
  GetHybridKeyResponse response2;
  response2.mutable_hybrid_key()->set_key_id(kKeyId2);
  response2.mutable_hybrid_key()->set_public_key(kPublicKey2);
  response2.mutable_hybrid_key()->set_private_key(kPrivateKey2);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response1, _1))
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response2, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or1 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2 =
      cached_coordinator_client_->GetHybridKey(request2);

  EXPECT_THAT(response_or1, IsSuccessfulAndHolds(EqualsProto(response1)));
  EXPECT_THAT(response_or2, IsSuccessfulAndHolds(EqualsProto(response2)));
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyWithDifferentKeyServiceEndpointDoesNotUseCache) {
  GetHybridKeyRequest request1;
  request1.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request1.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator1->set_account_identity(kAccountIdentity);
  coordinator1->set_kms_wip_provider(kWipProvider);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl);
  GetHybridKeyRequest request2(request1);
  request2.mutable_coordinators(0)->set_key_service_endpoint(
      kKeyServiceEndpoint2);

  GetHybridKeyResponse response1;
  response1.mutable_hybrid_key()->set_key_id(kKeyId);
  response1.mutable_hybrid_key()->set_public_key(kPublicKey);
  response1.mutable_hybrid_key()->set_private_key(kPrivateKey);
  GetHybridKeyResponse response2;
  response2.mutable_hybrid_key()->set_key_id(kKeyId);
  response2.mutable_hybrid_key()->set_public_key(kPublicKey2);
  response2.mutable_hybrid_key()->set_private_key(kPrivateKey2);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response1, _1))
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response2, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or1 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2 =
      cached_coordinator_client_->GetHybridKey(request2);

  EXPECT_THAT(response_or1, IsSuccessfulAndHolds(EqualsProto(response1)));
  EXPECT_THAT(response_or2, IsSuccessfulAndHolds(EqualsProto(response2)));
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyWithDifferentAccountIdentityDoesNotUseCache) {
  GetHybridKeyRequest request1;
  request1.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request1.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator1->set_account_identity(kAccountIdentity);
  coordinator1->set_kms_wip_provider(kWipProvider);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl);
  GetHybridKeyRequest request2(request1);
  request2.mutable_coordinators(0)->set_account_identity(kAccountIdentity2);

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_key_id(kKeyId);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .Times(2)
      .WillRepeatedly(std::bind(MockGetHybridKeyWithResponse, response, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or1 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2 =
      cached_coordinator_client_->GetHybridKey(request2);

  EXPECT_THAT(response_or1, IsSuccessfulAndHolds(EqualsProto(response)));
  EXPECT_THAT(response_or2, IsSuccessfulAndHolds(EqualsProto(response)));
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyWithDifferentWipProviderDoesNotUseCache) {
  GetHybridKeyRequest request1;
  request1.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request1.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator1->set_account_identity(kAccountIdentity);
  coordinator1->set_kms_wip_provider(kWipProvider);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl);
  GetHybridKeyRequest request2(request1);
  request2.mutable_coordinators(0)->set_kms_wip_provider(kWipProvider2);

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_key_id(kKeyId);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .Times(2)
      .WillRepeatedly(std::bind(MockGetHybridKeyWithResponse, response, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or1 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2 =
      cached_coordinator_client_->GetHybridKey(request2);

  EXPECT_THAT(response_or1, IsSuccessfulAndHolds(EqualsProto(response)));
  EXPECT_THAT(response_or2, IsSuccessfulAndHolds(EqualsProto(response)));
}

TEST_F(CachedCoordinatorClientTest,
       GetHybridKeyWithDifferentKeyServiceAudienceDoesNotUseCache) {
  GetHybridKeyRequest request1;
  request1.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request1.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint);
  coordinator1->set_account_identity(kAccountIdentity);
  coordinator1->set_kms_wip_provider(kWipProvider);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl);
  GetHybridKeyRequest request2(request1);
  request2.mutable_coordinators(0)->set_key_service_audience_url(
      kKeyServiceAudienceUrl2);

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_key_id(kKeyId);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_base_coordinator_client_, GetHybridKeyAsync)
      .Times(2)
      .WillRepeatedly(std::bind(MockGetHybridKeyWithResponse, response, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or1 =
      cached_coordinator_client_->GetHybridKey(request1);
  ExecutionResultOr<GetHybridKeyResponse> response_or2 =
      cached_coordinator_client_->GetHybridKey(request2);

  EXPECT_THAT(response_or1, IsSuccessfulAndHolds(EqualsProto(response)));
  EXPECT_THAT(response_or2, IsSuccessfulAndHolds(EqualsProto(response)));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
