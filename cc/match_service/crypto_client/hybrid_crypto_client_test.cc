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

#include "cc/match_service/crypto_client/hybrid_crypto_client.h"

#include <atomic>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/public/cpio/proto/private_key_service/v1/private_key_service.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/core/async/async_context.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/error/error.h"
#include "cc/public/cpio/utils/key_fetching/interface/key_fetcher_with_cache_interface.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::Key;
using ::google::scp::cpio::KeyFetcherWithCacheInterface;

class MockKeyFetcherWithCache : public KeyFetcherWithCacheInterface {
 public:
  MOCK_METHOD(ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(ExecutionResult, Stop, (), (noexcept, override));
  MOCK_METHOD(ExecutionResultOr<Key>, GetKey, (const std::string&),
              (noexcept, override));
  MOCK_METHOD(ExecutionResultOr<std::vector<Key>>, GetValidKeys,
              (google::scp::core::Timestamp), (noexcept, override));
};

constexpr absl::string_view kTestPrivateHpkeP256 =
    "CHsSswEKqgEKNXR5cGUuZ29vZ2xlYXBpcy5jb20vZ29vZ2xlLmNyeXB0by50aW5rLkhwa2VQcm"
    "l2YXRlS2V5Em8SSxIGCAIQARgBGkEE/owZzgkFGR68KYqSRXklMfJvDOziRgY56Lw5y39waoJq"
    "d5tM+Wm4oOU5x/Yvs9MK1qqPgOMPHRKKr9aKLOcuoBog885/2uV+GjENh/HrvebzKL4Kmc28rf"
    "TWWJzyneS4/9IYAhABGHsgAw==";

constexpr absl::string_view kKeyId = "key_id";

class HybridCryptoClientTest : public testing::Test {
 protected:
  HybridCryptoClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        crypto_client_(
            std::make_unique<HybridCryptoClient>(&mock_key_fetcher_)) {}

  std::shared_ptr<LoggerInterface> logger_;
  MockKeyFetcherWithCache mock_key_fetcher_;
  std::unique_ptr<CryptoClientInterface> crypto_client_;
};

EncryptionKeyInfo GetEncryptionKeyInfo() {
  EncryptionKeyInfo encryption_key_info;
  encryption_key_info.mutable_coordinator_key_info()->set_key_id(kKeyId);
  return encryption_key_info;
}

TEST_F(HybridCryptoClientTest, StartStop) {
  EXPECT_TRUE(crypto_client_->Init().ok());
  EXPECT_TRUE(crypto_client_->Run().ok());
  EXPECT_TRUE(crypto_client_->Stop().ok());
}

TEST_F(HybridCryptoClientTest, GetCryptoKeySuccess) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetEncryptionKeyInfo());
  EXPECT_CALL(mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce([](const std::string& key_id) {
        Key key;
        key.key_id = key_id;
        key.private_key = std::string(kTestPrivateHpkeP256);
        return ExecutionResultOr<Key>(key);
      });

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_TRUE(ctx.status.ok());
        EXPECT_NE(ctx.response, nullptr);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HybridCryptoClientTest, GetCryptoKeyBadBase64) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetEncryptionKeyInfo());
  EXPECT_CALL(mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce([](const std::string& key_id) {
        Key key;
        key.key_id = key_id;
        key.private_key = "bad base 64";
        return ExecutionResultOr<Key>(key);
      });
  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status),
                  Error::DECODING_ERROR);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HybridCryptoClientTest, GetCryptoKeyFetcherFailure) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetEncryptionKeyInfo());
  EXPECT_CALL(mock_key_fetcher_, GetKey(std::string(kKeyId)))
      .WillOnce([](const std::string& key_id) {
        return ExecutionResultOr<Key>(FailureExecutionResult(SC_UNKNOWN));
      });
  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::KEY_FETCHING_ERROR);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::match_service
