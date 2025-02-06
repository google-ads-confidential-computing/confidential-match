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

#include "cc/lookup_server/kms_client/src/cached_kms_client.h"

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

#include "cc/lookup_server/interface/kms_client_interface.h"
#include "cc/lookup_server/kms_client/mock/fake_kms_client.h"
#include "cc/lookup_server/kms_client/mock/mock_kms_client.h"
#include "cc/lookup_server/kms_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

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
using ::google::scp::core::test::IsSuccessfulAndHolds;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;
using ::testing::Eq;
using ::testing::Return;

constexpr size_t kTestAsyncExecutorThreadCount = 4;
constexpr size_t kTestAsyncExecutorQueueCap = 1000;

constexpr absl::string_view kKmsResourceName =
    "projects/test-project/locations/test-location/keyRings/test-keyring/"
    "cryptoKeys/test-key";
constexpr absl::string_view kKmsResourceName2 =
    "projects/test-project/locations/test-location/keyRings/test-keyring/"
    "cryptoKeys/test-key2";
constexpr absl::string_view kWipProvider =
    "projects/test-project/locations/test-location/"
    "workloadIdentityPools/test-wip/providers/test-provider";
constexpr absl::string_view kWipProvider2 =
    "projects/test-project/locations/test-location/"
    "workloadIdentityPools/test-wip/providers/test-provider2";
constexpr absl::string_view kPlaintext = "decrypted";
constexpr absl::string_view kPlaintext2 = "decrypted2";
constexpr absl::string_view kCiphertext = "encrypted";
constexpr absl::string_view kCiphertext2 = "encrypted2";
constexpr absl::string_view kVerifiedIdentity =
    "verified@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kVerifiedIdentity2 =
    "verified2@test-project.iam.gserviceaccount.com";

class CachedKmsClientTest : public testing::Test {
 protected:
  CachedKmsClientTest()
      : mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_base_kms_client_(std::make_shared<MockKmsClient>()),
        cached_kms_client_(std::make_unique<CachedKmsClient>(
            mock_async_executor_, mock_base_kms_client_)) {
    mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
      return SuccessExecutionResult();
    };
    mock_async_executor_->schedule_for_mock =
        [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
          return SuccessExecutionResult();
        };
  }

  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::shared_ptr<MockKmsClient> mock_base_kms_client_;
  std::unique_ptr<KmsClientInterface> cached_kms_client_;
};

// Helper to simulate a successful response from the base KMS client.
ExecutionResult MockDecryptSuccessful(
    absl::string_view response,
    AsyncContext<DecryptRequest, std::string> decrypt_context) noexcept {
  decrypt_context.result = SuccessExecutionResult();
  decrypt_context.response = std::make_shared<std::string>(response);
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

// Helper to simulate an asynchronous failure response from the base KMS client.
ExecutionResult MockDecryptAsyncFailure(
    ExecutionResult result,
    AsyncContext<DecryptRequest, std::string> decrypt_context) noexcept {
  decrypt_context.result = result;
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

TEST_F(CachedKmsClientTest, DecryptReturnsSuccess) {
  AsyncContext<DecryptRequest, std::string> context;
  context.request = std::make_shared<DecryptRequest>();
  context.request->set_key_resource_name(kKmsResourceName);
  context.request->set_gcp_wip_provider(kWipProvider);
  context.request->set_account_identity(kVerifiedIdentity);
  context.request->set_ciphertext(kCiphertext);
  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<DecryptRequest, std::string>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(*ctx.response, kPlaintext);
        is_complete = true;
      };

  ExecutionResult result = cached_kms_client_->Decrypt(context);

  EXPECT_SUCCESS(result);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CachedKmsClientTest, DecryptSyncReturnsSuccess) {
  DecryptRequest request;
  request.set_key_resource_name(kKmsResourceName);
  request.set_gcp_wip_provider(kWipProvider);
  request.set_account_identity(kVerifiedIdentity);
  request.set_ciphertext(kCiphertext);
  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1));

  ExecutionResultOr<std::string> result = cached_kms_client_->Decrypt(request);

  EXPECT_THAT(result, IsSuccessfulAndHolds(Eq(kPlaintext)));
}

TEST_F(CachedKmsClientTest, DecryptWithSyncFailureReturnsError) {
  AsyncContext<DecryptRequest, std::string> context;
  context.request = std::make_shared<DecryptRequest>();
  context.request->set_key_resource_name(kKmsResourceName);
  context.request->set_gcp_wip_provider(kWipProvider);
  context.request->set_account_identity(kVerifiedIdentity);
  context.request->set_ciphertext(kCiphertext);
  context.callback = [](AsyncContext<DecryptRequest, std::string>& ctx) {
    ADD_FAILURE() << "The callback was unexpectedly invoked on start failure.";
  };
  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(Return(FailureExecutionResult(KMS_CLIENT_DECRYPTION_ERROR)));

  ExecutionResult result = cached_kms_client_->Decrypt(context);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(KMS_CLIENT_DECRYPTION_ERROR)));
}

TEST_F(CachedKmsClientTest, DecryptWithAsyncFailureReturnsError) {
  AsyncContext<DecryptRequest, std::string> context;
  context.request = std::make_shared<DecryptRequest>();
  context.request->set_key_resource_name(kKmsResourceName);
  context.request->set_gcp_wip_provider(kWipProvider);
  context.request->set_account_identity(kVerifiedIdentity);
  context.request->set_ciphertext(kCiphertext);
  std::atomic<bool> is_complete = false;
  context.callback = [&is_complete](
                         AsyncContext<DecryptRequest, std::string>& ctx) {
    EXPECT_THAT(ctx.result,
                ResultIs(FailureExecutionResult(KMS_CLIENT_DECRYPTION_ERROR)));
    is_complete = true;
  };
  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptAsyncFailure,
                          FailureExecutionResult(KMS_CLIENT_DECRYPTION_ERROR),
                          _1));

  ExecutionResult result = cached_kms_client_->Decrypt(context);

  EXPECT_SUCCESS(result);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CachedKmsClientTest, DecryptRepeatedlyWithSameRequestUsesCache) {
  AsyncContext<DecryptRequest, std::string> context;
  context.request = std::make_shared<DecryptRequest>();
  context.request->set_key_resource_name(kKmsResourceName);
  context.request->set_gcp_wip_provider(kWipProvider);
  context.request->set_account_identity(kVerifiedIdentity);
  context.request->set_ciphertext(kCiphertext);
  AsyncContext<DecryptRequest, std::string> context2(context);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<DecryptRequest, std::string>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(*ctx.response, kPlaintext);
        is_complete = true;
      };
  std::atomic<bool> is_complete2 = false;
  context2.callback =
      [&is_complete2](AsyncContext<DecryptRequest, std::string>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(*ctx.response, kPlaintext);
        is_complete2 = true;
      };
  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1));

  ExecutionResult result = cached_kms_client_->Decrypt(context);
  ExecutionResult result2 = cached_kms_client_->Decrypt(context2);

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(result2);
  WaitUntil([&]() { return is_complete.load(); });
  WaitUntil([&]() { return is_complete2.load(); });
}

TEST_F(CachedKmsClientTest,
       DecryptRepeatedlyWithDifferentRequestsReturnsCorrectCachedEntry) {
  DecryptRequest request1;
  request1.set_key_resource_name(kKmsResourceName);
  request1.set_gcp_wip_provider(kWipProvider);
  request1.set_account_identity(kVerifiedIdentity);
  request1.set_ciphertext(kCiphertext);

  DecryptRequest request2;
  request2.set_key_resource_name(kKmsResourceName2);
  request2.set_gcp_wip_provider(kWipProvider2);
  request2.set_account_identity(kVerifiedIdentity2);
  request2.set_ciphertext(kCiphertext2);

  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1))
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext2, _1));

  ExecutionResultOr<std::string> result1_1 =
      cached_kms_client_->Decrypt(request1);
  ExecutionResultOr<std::string> result2_1 =
      cached_kms_client_->Decrypt(request2);
  ExecutionResultOr<std::string> result1_2 =
      cached_kms_client_->Decrypt(request1);
  ExecutionResultOr<std::string> result2_2 =
      cached_kms_client_->Decrypt(request2);

  EXPECT_THAT(result1_1, IsSuccessfulAndHolds(Eq(kPlaintext)));
  EXPECT_THAT(result1_2, IsSuccessfulAndHolds(Eq(kPlaintext)));
  EXPECT_THAT(result2_1, IsSuccessfulAndHolds(Eq(kPlaintext2)));
  EXPECT_THAT(result2_2, IsSuccessfulAndHolds(Eq(kPlaintext2)));
}

TEST_F(CachedKmsClientTest, DecryptWithDifferentCiphertextDoesNotUseCache) {
  DecryptRequest request1;
  request1.set_key_resource_name(kKmsResourceName);
  request1.set_gcp_wip_provider(kWipProvider);
  request1.set_account_identity(kVerifiedIdentity);
  request1.set_ciphertext(kCiphertext);

  DecryptRequest request2(request1);
  request2.set_ciphertext(kCiphertext2);

  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1))
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext2, _1));

  ExecutionResultOr<std::string> result1 =
      cached_kms_client_->Decrypt(request1);
  ExecutionResultOr<std::string> result2 =
      cached_kms_client_->Decrypt(request2);

  EXPECT_THAT(result1, IsSuccessfulAndHolds(Eq(kPlaintext)));
  EXPECT_THAT(result2, IsSuccessfulAndHolds(Eq(kPlaintext2)));
}

TEST_F(CachedKmsClientTest,
       DecryptWithDifferentKmsResourceNameDoesNotUseCache) {
  DecryptRequest request1;
  request1.set_key_resource_name(kKmsResourceName);
  request1.set_gcp_wip_provider(kWipProvider);
  request1.set_account_identity(kVerifiedIdentity);
  request1.set_ciphertext(kCiphertext);

  DecryptRequest request2(request1);
  request2.set_key_resource_name(kKmsResourceName2);

  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1))
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext2, _1));

  ExecutionResultOr<std::string> result1 =
      cached_kms_client_->Decrypt(request1);
  ExecutionResultOr<std::string> result2 =
      cached_kms_client_->Decrypt(request2);

  EXPECT_THAT(result1, IsSuccessfulAndHolds(Eq(kPlaintext)));
  EXPECT_THAT(result2, IsSuccessfulAndHolds(Eq(kPlaintext2)));
}

TEST_F(CachedKmsClientTest, DecryptWithDifferentWipProviderDoesNotUseCache) {
  DecryptRequest request1;
  request1.set_key_resource_name(kKmsResourceName);
  request1.set_gcp_wip_provider(kWipProvider);
  request1.set_account_identity(kVerifiedIdentity);
  request1.set_ciphertext(kCiphertext);

  DecryptRequest request2(request1);
  request2.set_gcp_wip_provider(kWipProvider2);

  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1))
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext2, _1));

  ExecutionResultOr<std::string> result1 =
      cached_kms_client_->Decrypt(request1);
  ExecutionResultOr<std::string> result2 =
      cached_kms_client_->Decrypt(request2);

  EXPECT_THAT(result1, IsSuccessfulAndHolds(Eq(kPlaintext)));
  EXPECT_THAT(result2, IsSuccessfulAndHolds(Eq(kPlaintext2)));
}

TEST_F(CachedKmsClientTest,
       DecryptWithDifferentAccountIdentityDoesNotUseCache) {
  DecryptRequest request1;
  request1.set_key_resource_name(kKmsResourceName);
  request1.set_gcp_wip_provider(kWipProvider);
  request1.set_account_identity(kVerifiedIdentity);
  request1.set_ciphertext(kCiphertext);

  DecryptRequest request2(request1);
  request2.set_account_identity(kVerifiedIdentity2);

  EXPECT_CALL(*mock_base_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1))
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext2, _1));

  ExecutionResultOr<std::string> result1 =
      cached_kms_client_->Decrypt(request1);
  ExecutionResultOr<std::string> result2 =
      cached_kms_client_->Decrypt(request2);

  EXPECT_THAT(result1, IsSuccessfulAndHolds(Eq(kPlaintext)));
  EXPECT_THAT(result2, IsSuccessfulAndHolds(Eq(kPlaintext2)));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
