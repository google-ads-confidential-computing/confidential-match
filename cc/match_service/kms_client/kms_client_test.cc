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

#include "cc/match_service/kms_client/kms_client.h"

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/mock/kms_client/mock_kms_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::cmrt::sdk::kms_service::v1::DecryptRequest;
using ::google::cmrt::sdk::kms_service::v1::DecryptResponse;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::Return;

using MockCpioKmsClient = ::google::scp::cpio::MockKmsClient;

constexpr absl::string_view kCiphertext = "encrypted_data";
constexpr absl::string_view kPlaintext = "decrypted_data";

class KmsClientTest : public testing::Test {
 protected:
  KmsClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_cpio_kms_client_(std::make_shared<MockCpioKmsClient>()),
        kms_client_(std::make_unique<KmsClient>(mock_cpio_kms_client_)) {}

  std::shared_ptr<LoggerInterface> logger_;
  std::shared_ptr<MockCpioKmsClient> mock_cpio_kms_client_;
  std::unique_ptr<KmsClientInterface> kms_client_;
};

ExecutionResult MockDecryptAsyncFailure(
    ExecutionResult result,
    google::scp::core::AsyncContext<
        google::cmrt::sdk::kms_service::v1::DecryptRequest,
        google::cmrt::sdk::kms_service::v1::DecryptResponse>&
        decrypt_context) noexcept {
  decrypt_context.result = result;
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

TEST_F(KmsClientTest, InitRunStopSuccess) {
  EXPECT_CALL(*mock_cpio_kms_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_kms_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_kms_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));

  EXPECT_TRUE(kms_client_->Init().ok());
  EXPECT_TRUE(kms_client_->Run().ok());
  EXPECT_TRUE(kms_client_->Stop().ok());
}

TEST_F(KmsClientTest, InitRunStopFailure) {
  EXPECT_CALL(*mock_cpio_kms_client_, Init)
      .WillOnce(Return(FailureExecutionResult(123)));

  auto status = kms_client_->Init();
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(GetBackendErrorReason(status), Error::INTERNAL_ERROR);
}

TEST_F(KmsClientTest, DecryptSuccess) {
  auto request = std::make_shared<DecryptRequest>();
  request->set_ciphertext(std::string(kCiphertext));
  // Mock the CPIO client behavior
  EXPECT_CALL(*mock_cpio_kms_client_, Decrypt).WillOnce([](auto context) {
    context.response = std::make_shared<DecryptResponse>();
    context.response->set_plaintext(std::string(kPlaintext));
    context.result = SuccessExecutionResult();
    context.Finish();
    return SuccessExecutionResult();
  });
  std::atomic<bool> done = false;
  AsyncContext<DecryptRequest, std::string> context(
      request,
      [&](google::confidential_match::AsyncContext<DecryptRequest, std::string>&
              ctx) {
        EXPECT_TRUE(ctx.status.ok());
        EXPECT_EQ(*ctx.response, kPlaintext);
        done = true;
      },
      logger_);

  kms_client_->Decrypt(context);
  WaitUntil([&]() { return done.load(); });
}

TEST_F(KmsClientTest, DecryptAsyncFailure) {
  auto request = std::make_shared<DecryptRequest>();
  request->set_ciphertext(std::string(kCiphertext));
  // Mock CPIO reporting a failure via callback
  EXPECT_CALL(*mock_cpio_kms_client_, Decrypt).WillOnce([](auto context) {
    context.result = FailureExecutionResult(123);
    context.Finish();
    return SuccessExecutionResult();
  });

  std::atomic<bool> done = false;
  AsyncContext<DecryptRequest, std::string> context(
      request,
      [&](google::confidential_match::AsyncContext<DecryptRequest, std::string>&
              ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status),
                  Error::WRAPPED_KEY_FETCHING_ERROR);
        done = true;
      },
      logger_);

  kms_client_->Decrypt(context);
  WaitUntil([&]() { return done.load(); });
}

TEST_F(KmsClientTest, DecryptDispatchFailure) {
  auto request = std::make_shared<DecryptRequest>();
  request->set_ciphertext(std::string(kCiphertext));
  // Mock CPIO failing
  EXPECT_CALL(*mock_cpio_kms_client_, Decrypt)
      .WillOnce(
          std::bind(MockDecryptAsyncFailure, FailureExecutionResult(1), _1));
  std::atomic<bool> done = false;
  AsyncContext<DecryptRequest, std::string> context(
      request,
      [&](google::confidential_match::AsyncContext<DecryptRequest, std::string>&
              ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status),
                  Error::WRAPPED_KEY_FETCHING_ERROR);
        done = true;
      },
      logger_);

  kms_client_->Decrypt(context);
  WaitUntil([&]() { return done.load(); });
}

}  // namespace
}  // namespace google::confidential_match::match_service
