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

#include "cc/lookup_server/kms_client/src/kms_client.h"

#include <memory>
#include <string>

#include "absl/strings/escaping.h"
#include "cc/core/async_executor/src/async_executor.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/mock/kms_client/mock_kms_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/interface/kms_client_interface.h"
#include "cc/lookup_server/kms_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutor;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::IsSuccessfulAndHolds;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;
using ::testing::Eq;
using ::testing::Return;

using MockCpioKmsClient = ::google::scp::cpio::MockKmsClient;

constexpr absl::string_view kKmsResourceName =
    "projects/test-project/locations/test-location/keyRings/test-keyring/"
    "cryptoKeys/test-key";
constexpr absl::string_view kWipProvider =
    "projects/test-project/locations/test-location/"
    "workloadIdentityPools/test-wip/providers/test-provider";
constexpr absl::string_view kCiphertext = "encrypted";
constexpr absl::string_view kPlaintext = "decrypted";
constexpr absl::string_view kVerifiedIdentity =
    "verified@test-project.iam.gserviceaccount.com";

class KmsClientTest : public testing::Test {
 protected:
  KmsClientTest()
      : mock_cpio_kms_client_(std::make_shared<MockCpioKmsClient>()),
        kms_client_(std::make_unique<KmsClient>(mock_cpio_kms_client_)) {}

  std::shared_ptr<MockCpioKmsClient> mock_cpio_kms_client_;
  std::unique_ptr<KmsClientInterface> kms_client_;
};

// Helper to simulate a successful response from the CPIO KMS client.
ExecutionResult MockDecryptSuccessful(
    absl::string_view response,
    AsyncContext<google::cmrt::sdk::kms_service::v1::DecryptRequest,
                 google::cmrt::sdk::kms_service::v1::DecryptResponse>&
        decrypt_context) noexcept {
  decrypt_context.result = SuccessExecutionResult();
  decrypt_context.response =
      std::make_shared<google::cmrt::sdk::kms_service::v1::DecryptResponse>();
  decrypt_context.response->set_plaintext(response);
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

// Helper to simulate an asynchronous failure response from the CPIO KMS client.
ExecutionResult MockDecryptAsyncFailure(
    ExecutionResult result,
    AsyncContext<google::cmrt::sdk::kms_service::v1::DecryptRequest,
                 google::cmrt::sdk::kms_service::v1::DecryptResponse>&
        decrypt_context) noexcept {
  decrypt_context.result = result;
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

TEST_F(KmsClientTest, StartStop) {
  EXPECT_CALL(*mock_cpio_kms_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_kms_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_kms_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(kms_client_->Init());
  EXPECT_SUCCESS(kms_client_->Run());
  EXPECT_SUCCESS(kms_client_->Stop());
}

TEST_F(KmsClientTest, DecryptReturnsSuccess) {
  AsyncContext<DecryptRequest, std::string> context;
  context.request = std::make_shared<DecryptRequest>();
  context.request->set_key_resource_name(kKmsResourceName);
  context.request->set_gcp_wip_provider(kWipProvider);
  context.request->set_account_identity(kVerifiedIdentity);
  context.request->set_ciphertext(kCiphertext);
  EXPECT_CALL(*mock_cpio_kms_client_, Decrypt)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<DecryptRequest, std::string>& ctx) {
        EXPECT_SUCCESS(ctx.result);
        EXPECT_EQ(*ctx.response, kPlaintext);
        is_complete = true;
      };

  kms_client_->Decrypt(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(KmsClientTest, DecryptSyncReturnsSuccess) {
  DecryptRequest request;
  request.set_key_resource_name(kKmsResourceName);
  request.set_gcp_wip_provider(kWipProvider);
  request.set_account_identity(kVerifiedIdentity);
  request.set_ciphertext(kCiphertext);
  EXPECT_CALL(*mock_cpio_kms_client_, Decrypt)
      .WillOnce(std::bind(MockDecryptSuccessful, kPlaintext, _1));

  ExecutionResultOr<std::string> result = kms_client_->Decrypt(request);

  EXPECT_THAT(result, IsSuccessfulAndHolds(Eq(kPlaintext)));
}

TEST_F(KmsClientTest, DecryptWithAsyncFailureReturnsError) {
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
  EXPECT_CALL(*mock_cpio_kms_client_, Decrypt)
      .WillOnce(
          std::bind(MockDecryptAsyncFailure, FailureExecutionResult(1), _1));

  kms_client_->Decrypt(context);
  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
