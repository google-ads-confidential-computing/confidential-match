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

#include "cc/lookup_server/coordinator_client/src/coordinator_client.h"

#include <memory>

#include "absl/strings/escaping.h"
#include "cc/core/async_executor/src/async_executor.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/mock/private_key_client/mock_private_key_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/coordinator_client/mock/fake_coordinator_client.h"
#include "cc/lookup_server/coordinator_client/mock/mock_coordinator_client.h"
#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "cc/lookup_server/interface/coordinator_client_interface.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::private_key_service::v1::ListPrivateKeysRequest;
using ::google::cmrt::sdk::private_key_service::v1::ListPrivateKeysResponse;
using ::google::cmrt::sdk::private_key_service::v1::PrivateKey;
using ::google::cmrt::sdk::private_key_service::v1::PrivateKeyEndpoint;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutor;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::IsSuccessfulAndHolds;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::MockPrivateKeyClient;
using ::std::placeholders::_1;
using ::testing::Eq;
using ::testing::Return;

constexpr absl::string_view kKeyId = "test-key-id";
constexpr absl::string_view kKeyServiceEndpoint1 =
    "https://test-private-key-service1.google.com/v1alpha";
constexpr absl::string_view kKeyServiceEndpoint2 =
    "https://test-private-key-service2.google.com/v1alpha";
constexpr absl::string_view kAccountIdentity1 =
    "test-verified-user1@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kAccountIdentity2 =
    "test-verified-user2@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kWipProvider1 =
    "projects/1/locations/global/workloadIdentityPools/test-wip/providers/"
    "test-provider";
constexpr absl::string_view kWipProvider2 =
    "projects/2/locations/global/workloadIdentityPools/test-wip/providers/"
    "test-provider";
constexpr absl::string_view kKeyServiceAudienceUrl1 =
    "https://test-key-service-audience-url1.a.run.app";
constexpr absl::string_view kKeyServiceAudienceUrl2 =
    "https://test-key-service-audience-url2.a.run.app";
constexpr absl::string_view kPublicKey = "test-public-key";
constexpr absl::string_view kPrivateKey = "test-private-key";

class CoordinatorClientTest : public testing::Test {
 protected:
  CoordinatorClientTest()
      : mock_private_key_client_(std::make_shared<MockPrivateKeyClient>()),
        coordinator_client_(
            std::make_unique<CoordinatorClient>(mock_private_key_client_)) {}

  std::shared_ptr<MockPrivateKeyClient> mock_private_key_client_;
  std::unique_ptr<CoordinatorClientInterface> coordinator_client_;
};

// Helper to simulate a successful response from the CPIO private key client.
void MockListPrivateKeysWithResponse(
    const ListPrivateKeysResponse& response,
    AsyncContext<ListPrivateKeysRequest, ListPrivateKeysResponse>&
        list_context) noexcept {
  list_context.result = SuccessExecutionResult();
  list_context.response = std::make_shared<ListPrivateKeysResponse>(response);
  list_context.Finish();
}

// Helper to simulate a successful response from the CPIO private key client
// after validating the request.
void MockListPrivateKeysWithExpectedRequestAndResponse(
    const ListPrivateKeysRequest& expected_request,
    const ListPrivateKeysResponse& response,
    AsyncContext<ListPrivateKeysRequest, ListPrivateKeysResponse>&
        list_context) noexcept {
  EXPECT_THAT(*list_context.request, EqualsProto(expected_request));

  list_context.result = SuccessExecutionResult();
  list_context.response = std::make_shared<ListPrivateKeysResponse>(response);
  list_context.Finish();
}

// Helper to simulate a failed response from the CPIO private key client.
void MockListPrivateKeysWithFailedResult(
    const ExecutionResult& failed_result,
    AsyncContext<ListPrivateKeysRequest, ListPrivateKeysResponse>&
        list_context) noexcept {
  list_context.result = failed_result;
  list_context.Finish();
}

TEST_F(CoordinatorClientTest, StartStop) {
  EXPECT_SUCCESS(coordinator_client_->Init());
  EXPECT_SUCCESS(coordinator_client_->Run());
  EXPECT_SUCCESS(coordinator_client_->Stop());
}

TEST_F(CoordinatorClientTest, GetHybridKeyYieldsSuccess) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 =
      context.request->add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);

  ListPrivateKeysResponse list_response;
  PrivateKey* private_key = list_response.add_private_keys();
  private_key->set_key_id(kKeyId);
  private_key->set_public_key(kPublicKey);
  private_key->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_private_key_client_, ListPrivateKeys)
      .WillOnce(std::bind(MockListPrivateKeysWithResponse, list_response, _1));

  GetHybridKeyResponse expected_response;
  expected_response.mutable_hybrid_key()->set_key_id(kKeyId);
  expected_response.mutable_hybrid_key()->set_public_key(kPublicKey);
  expected_response.mutable_hybrid_key()->set_private_key(kPrivateKey);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete, &expected_response](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_THAT(*ctx.response, EqualsProto(expected_response));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CoordinatorClientTest, GetHybridKeySyncReturnsSuccess) {
  GetHybridKeyRequest request;
  request.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 = request.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);

  ListPrivateKeysResponse list_response;
  PrivateKey* private_key = list_response.add_private_keys();
  private_key->set_key_id(kKeyId);
  private_key->set_public_key(kPublicKey);
  private_key->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_private_key_client_, ListPrivateKeys)
      .WillOnce(std::bind(MockListPrivateKeysWithResponse, list_response, _1));

  ExecutionResultOr<GetHybridKeyResponse> response_or =
      coordinator_client_->GetHybridKey(request);

  EXPECT_SUCCESS(response_or);
  GetHybridKeyResponse expected_response;
  expected_response.mutable_hybrid_key()->set_key_id(kKeyId);
  expected_response.mutable_hybrid_key()->set_public_key(kPublicKey);
  expected_response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  EXPECT_THAT(*response_or, EqualsProto(expected_response));
}

TEST_F(CoordinatorClientTest, GetHybridKeyMultipleCoordinatorsYieldsSuccess) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 =
      context.request->add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);
  GetHybridKeyRequest::Coordinator* coordinator2 =
      context.request->add_coordinators();
  coordinator2->set_key_service_endpoint(kKeyServiceEndpoint2);
  coordinator2->set_account_identity(kAccountIdentity2);
  coordinator2->set_kms_wip_provider(kWipProvider2);
  coordinator2->set_key_service_audience_url(kKeyServiceAudienceUrl2);

  ListPrivateKeysRequest expected_request;
  expected_request.add_key_ids(kKeyId);
  PrivateKeyEndpoint* expected_endpoint1 = expected_request.add_key_endpoints();
  expected_endpoint1->set_endpoint(kKeyServiceEndpoint1);
  expected_endpoint1->set_account_identity(kAccountIdentity1);
  expected_endpoint1->set_gcp_wip_provider(kWipProvider1);
  expected_endpoint1->set_gcp_cloud_function_url(kKeyServiceAudienceUrl1);
  PrivateKeyEndpoint* expected_endpoint2 = expected_request.add_key_endpoints();
  expected_endpoint2->set_endpoint(kKeyServiceEndpoint2);
  expected_endpoint2->set_account_identity(kAccountIdentity2);
  expected_endpoint2->set_gcp_wip_provider(kWipProvider2);
  expected_endpoint2->set_gcp_cloud_function_url(kKeyServiceAudienceUrl2);
  ListPrivateKeysResponse mock_list_response;
  PrivateKey* private_key = mock_list_response.add_private_keys();
  private_key->set_key_id(kKeyId);
  private_key->set_public_key(kPublicKey);
  private_key->set_private_key(kPrivateKey);
  EXPECT_CALL(*mock_private_key_client_, ListPrivateKeys)
      .WillOnce(std::bind(MockListPrivateKeysWithExpectedRequestAndResponse,
                          expected_request, mock_list_response, _1));

  GetHybridKeyResponse expected_response;
  expected_response.mutable_hybrid_key()->set_key_id(kKeyId);
  expected_response.mutable_hybrid_key()->set_public_key(kPublicKey);
  expected_response.mutable_hybrid_key()->set_private_key(kPrivateKey);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete, &expected_response](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_THAT(*ctx.response, EqualsProto(expected_response));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CoordinatorClientTest, GetHybridKeyNoKeyProvidedYieldsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  GetHybridKeyRequest::Coordinator* coordinator1 =
      context.request->add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result, ResultIs(FailureExecutionResult(
                                    COORDINATOR_CLIENT_KEY_NOT_FOUND_ERROR)));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CoordinatorClientTest, GetHybridKeyNoCoordinatorsProvidedYieldsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result,
                    ResultIs(FailureExecutionResult(
                        COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CoordinatorClientTest, GetHybridKeyNoKeyServiceEndpointYieldsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 =
      context.request->add_coordinators();
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result,
                    ResultIs(FailureExecutionResult(
                        COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR)));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CoordinatorClientTest, GetHybridKeyNoKeyFoundYieldsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 =
      context.request->add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);

  ListPrivateKeysResponse list_response;
  EXPECT_CALL(*mock_private_key_client_, ListPrivateKeys)
      .WillOnce(std::bind(MockListPrivateKeysWithResponse, list_response, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result, ResultIs(FailureExecutionResult(
                                    COORDINATOR_CLIENT_KEY_NOT_FOUND_ERROR)));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CoordinatorClientTest, GetHybridKeyWithClientErrorYieldsError) {
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> context;
  context.request = std::make_shared<GetHybridKeyRequest>();
  context.request->set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 =
      context.request->add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);

  ListPrivateKeysResponse list_response;
  EXPECT_CALL(*mock_private_key_client_, ListPrivateKeys)
      .WillOnce(std::bind(MockListPrivateKeysWithFailedResult,
                          FailureExecutionResult(1), _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](
          AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>& ctx) {
        EXPECT_THAT(ctx.result, ResultIs(FailureExecutionResult(
                                    COORDINATOR_CLIENT_KEY_FETCH_ERROR)));
        is_complete = true;
      };

  coordinator_client_->GetHybridKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
