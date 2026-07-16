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

#include "cc/match_service/kms_client/cached_kms_client.h"

#include <atomic>
#include <memory>
#include <queue>
#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "cc/match_service/kms_client/mock_kms_client.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::google::cmrt::sdk::kms_service::v1::DecryptRequest;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::Timestamp;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::WaitUntil;
using ::testing::_;
using ::testing::Return;

constexpr absl::string_view kCiphertext = "encrypted";
constexpr absl::string_view kCiphertext2 = "encrypted2";
constexpr absl::string_view kPlaintext = "decrypted";
constexpr absl::string_view kPlaintext2 = "decrypted2";

class CachedKmsClientTest : public testing::Test {
 protected:
  CachedKmsClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_base_kms_client_(std::make_unique<MockKmsClient>()),
        cached_kms_client_(std::make_unique<CachedKmsClient>(
            mock_async_executor_, mock_base_kms_client_.get())) {
    mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
      return SuccessExecutionResult();
    };
    mock_async_executor_->schedule_for_mock =
        [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
          return SuccessExecutionResult();
        };
  }

  void SetUp() override {
    EXPECT_THAT(cached_kms_client_->Init(), IsOk());
    EXPECT_THAT(cached_kms_client_->Run(), IsOk());
  }

  std::shared_ptr<LoggerInterface> logger_;
  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::unique_ptr<MockKmsClient> mock_base_kms_client_;
  std::unique_ptr<KmsClientInterface> cached_kms_client_;
};

TEST_F(CachedKmsClientTest, DecryptUsesUnderlyingClientOnCacheMiss) {
  auto request = std::make_shared<DecryptRequest>();
  request->set_ciphertext(std::string(kCiphertext));
  EXPECT_CALL(*mock_base_kms_client_, Decrypt).WillOnce([](auto ctx) {
    ctx.response = std::make_shared<std::string>(kPlaintext);
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });
  std::atomic<bool> done = false;
  AsyncContext<DecryptRequest, std::string> context(
      request,
      [&](auto& ctx) {
        EXPECT_TRUE(ctx.status.ok());
        EXPECT_EQ(*ctx.response, kPlaintext);
        done = true;
      },
      logger_);

  cached_kms_client_->Decrypt(context);
  WaitUntil([&]() { return done.load(); });
}

TEST_F(CachedKmsClientTest, DecryptUsesCacheOnSecondCall) {
  DecryptRequest request;
  request.set_ciphertext(std::string(kCiphertext));
  // The underlying client should only be called ONCE
  EXPECT_CALL(*mock_base_kms_client_, Decrypt).WillOnce([](auto ctx) {
    ctx.response = std::make_shared<std::string>(kPlaintext);
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });
  std::atomic<bool> done0 = false;
  google::confidential_match::AsyncContext<DecryptRequest, std::string>
      context0(
          std::make_shared<DecryptRequest>(request),
          [&](auto& ctx) {
            EXPECT_TRUE(ctx.status.ok());
            EXPECT_EQ(*ctx.response, kPlaintext);
            done0 = true;
          },
          logger_);
  std::atomic<bool> done1 = false;
  google::confidential_match::AsyncContext<DecryptRequest, std::string>
      context1(
          std::make_shared<DecryptRequest>(request),
          [&](auto& ctx) {
            EXPECT_TRUE(ctx.status.ok());
            EXPECT_EQ(*ctx.response, kPlaintext);
            done1 = true;
          },
          logger_);

  cached_kms_client_->Decrypt(context0);
  cached_kms_client_->Decrypt(context1);

  WaitUntil([&]() { return done0.load(); });
  WaitUntil([&]() { return done1.load(); });
}

TEST_F(CachedKmsClientTest, DecryptWithDifferentRequestDoesNotUseCache) {
  DecryptRequest request0;
  request0.set_ciphertext(std::string(kCiphertext));
  DecryptRequest request1;
  request1.set_ciphertext(std::string(kCiphertext2));
  // Expect two calls, returning different plaintexts
  EXPECT_CALL(*mock_base_kms_client_, Decrypt)
      .WillOnce([](auto ctx) {
        ctx.response = std::make_shared<std::string>(kPlaintext);
        ctx.status = absl::OkStatus();
        ctx.Finish();
      })
      .WillOnce([](auto ctx) {
        ctx.response = std::make_shared<std::string>(kPlaintext2);
        ctx.status = absl::OkStatus();
        ctx.Finish();
      });
  std::atomic<bool> done0 = false;
  google::confidential_match::AsyncContext<DecryptRequest, std::string>
      context0(
          std::make_shared<DecryptRequest>(request0),
          [&](auto& ctx) { done0 = true; }, logger_);
  std::atomic<bool> done1 = false;
  google::confidential_match::AsyncContext<DecryptRequest, std::string>
      context1(
          std::make_shared<DecryptRequest>(request1),
          [&](auto& ctx) {
            EXPECT_TRUE(ctx.status.ok());
            EXPECT_EQ(*ctx.response, kPlaintext2);
            done1 = true;
          },
          logger_);

  cached_kms_client_->Decrypt(context0);
  cached_kms_client_->Decrypt(context1);

  WaitUntil([&]() { return done0.load(); });
  WaitUntil([&]() { return done1.load(); });
}

TEST_F(CachedKmsClientTest, DecryptPropagatesFailure) {
  DecryptRequest request;
  request.set_ciphertext(std::string(kCiphertext));
  EXPECT_CALL(*mock_base_kms_client_, Decrypt).WillOnce([](auto ctx) {
    ctx.status = Status(Error::KMS_CLIENT_ERROR, "KMS Failure");
    ctx.Finish();
  });
  std::atomic<bool> done = false;
  google::confidential_match::AsyncContext<DecryptRequest, std::string> context(
      std::make_shared<DecryptRequest>(request),
      [&](auto& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::KMS_CLIENT_ERROR);
        done = true;
      },
      logger_);

  cached_kms_client_->Decrypt(context);

  WaitUntil([&]() { return done.load(); });
}

}  // namespace
}  // namespace google::confidential_match::match_service
