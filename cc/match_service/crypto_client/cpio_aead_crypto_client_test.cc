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

#include "cc/match_service/crypto_client/cpio_aead_crypto_client.h"

#include <atomic>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/async/async_context.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/error/error.h"
#include "cc/public/cpio/interface/error_codes.h"
#include "cc/public/cpio/utils/key_fetching/proto/cloud_wrapped_key.pb.h"
#include "cc/public/cpio/utils/key_fetching/src/error_codes.h"
#include "cc/public/cpio/utils/key_fetching/src/wrapped_key_handler_with_cache_mock.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::google::cmrt::sdk::v1::CloudWrappedKey;
using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::SubstituteAndParseTextToProto;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::WrappedKeyHandlerWithCacheMock;
using ::testing::Return;

constexpr absl::string_view kTestDek =
    "CP24wPEEEmcKWwozdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2"
    "NtU2l2S2V5EiIaIObVHDAKMgQg0ww0qfsLoKzu0ui90WbHAqiJf8qayOqsGAEQARj9uMDxBCA"
    "B";

constexpr absl::string_view kGcpKmsResourceName =
    "projects/test-project/locations/test-location/keyRings/test-keyring/"
    "cryptoKeys/test-key";
constexpr absl::string_view kWipProvider =
    "projects/test-project/locations/test-location/"
    "workloadIdentityPools/test-wip/providers/test-provider";
constexpr absl::string_view kAwsKmsResourceName =
    "arn:aws:kms:us-east-2:123456789012:key/test-key";
constexpr absl::string_view kRoleArn =
    "arn:aws:iam::123456789012:role/cfm-test-role";

class CpioAeadCryptoClientTest : public testing::Test {
 protected:
  CpioAeadCryptoClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        crypto_client_(std::make_unique<CpioAeadCryptoClient>(
            &mock_async_executor_, &mock_aws_key_handler_,
            &mock_gcp_key_handler_)) {
    EXPECT_TRUE(crypto_client_->Init().ok());
    EXPECT_TRUE(crypto_client_->Run().ok());
  }

  std::shared_ptr<LoggerInterface> logger_;
  MockAsyncExecutor mock_async_executor_;
  WrappedKeyHandlerWithCacheMock mock_aws_key_handler_;
  WrappedKeyHandlerWithCacheMock mock_gcp_key_handler_;
  std::unique_ptr<CryptoClientInterface> crypto_client_;
};

EncryptionKeyInfo GetAwsEncryptionKeyInfo(
    absl::string_view kek_id = kAwsKmsResourceName) {
  return SubstituteAndParseTextToProto<EncryptionKeyInfo>(
      R"pb(
        wrapped_key_info {
          encrypted_dek: "$0"
          kek_kms_resource_id: "$1"
          aws_wrapped_key_info { role_arn: "$2" }
        }
      )pb",
      kTestDek, kek_id, kRoleArn);
}

EncryptionKeyInfo GetGcpEncryptionKeyInfo(
    absl::string_view kek_id = kGcpKmsResourceName) {
  return SubstituteAndParseTextToProto<EncryptionKeyInfo>(
      R"pb(
        wrapped_key_info {
          encrypted_dek: "$0"
          kek_kms_resource_id: "$1"
          gcp_wrapped_key_info { wip_provider: "$2" }
        }
      )pb",
      kTestDek, kek_id, kWipProvider);
}

TEST_F(CpioAeadCryptoClientTest, StartStop) {
  EXPECT_TRUE(crypto_client_->Init().ok());
  EXPECT_TRUE(crypto_client_->Run().ok());
  EXPECT_TRUE(crypto_client_->Stop().ok());
}

TEST_F(CpioAeadCryptoClientTest, GetGcpCryptoKeySuccess) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));

  CloudWrappedKey expected_key = SubstituteAndParseTextToProto<CloudWrappedKey>(
      R"pb(
        gcp_wrapped_key { encrypted_dek: "$0" kek_uri: "$1" wip_provider: "$2" }
      )pb",
      kTestDek, kGcpKmsResourceName, kWipProvider);

  EXPECT_CALL(mock_gcp_key_handler_, GetKey(EqualsProto(expected_key)))
      .WillOnce(Return(ExecutionResultOr<std::string>(decoded_dek)));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_THAT(ctx.status, IsOk());
        ASSERT_NE(ctx.response, nullptr);
        auto ciphertext_or = ctx.response->Encrypt("test_plaintext");
        ASSERT_THAT(ciphertext_or, IsOk());
        EXPECT_THAT(ctx.response->Decrypt(*ciphertext_or),
                    IsOkAndHolds("test_plaintext"));
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetAwsCryptoKeySuccess) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetAwsEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));

  CloudWrappedKey expected_key = SubstituteAndParseTextToProto<CloudWrappedKey>(
      R"pb(
        aws_wrapped_key { encrypted_dek: "$0" kek_uri: "$1" role_arn: "$2" }
      )pb",
      kTestDek, kAwsKmsResourceName, kRoleArn);

  EXPECT_CALL(mock_aws_key_handler_, GetKey(EqualsProto(expected_key)))
      .WillOnce(Return(ExecutionResultOr<std::string>(decoded_dek)));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_THAT(ctx.status, IsOk());
        ASSERT_NE(ctx.response, nullptr);
        auto ciphertext_or = ctx.response->Encrypt("test_plaintext");
        ASSERT_THAT(ciphertext_or, IsOk());
        EXPECT_THAT(ctx.response->Decrypt(*ciphertext_or),
                    IsOkAndHolds("test_plaintext"));
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyPermissionDenied) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>(FailureExecutionResult(
          scp::core::errors::SC_CPIO_INVALID_CREDENTIALS))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status),
                  Error::CUSTOMER_KEY_PERMISSION_DENIED);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyQuotaExceeded) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>(FailureExecutionResult(
          scp::core::errors::SC_CPIO_REQUEST_LIMIT_REACHED))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status),
                  Error::CUSTOMER_QUOTA_EXCEEDED);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyInvalidArgument) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>(FailureExecutionResult(
          scp::core::errors::SC_CPIO_INVALID_ARGUMENT))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INVALID_DEK);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyEntityNotFound) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>(FailureExecutionResult(
          scp::core::errors::SC_CPIO_ENTITY_NOT_FOUND))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INVALID_DEK);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyKeyNotFound) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>(
          FailureExecutionResult(scp::core::errors::SC_CPIO_KEY_NOT_FOUND))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INVALID_DEK);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyInternalError) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>(
          FailureExecutionResult(scp::core::errors::SC_CPIO_INTERNAL_ERROR))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INVALID_DEK);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyMissingCloudWrappedKeyInfo) {
  auto request = std::make_shared<EncryptionKeyInfo>(
      SubstituteAndParseTextToProto<EncryptionKeyInfo>(
          R"pb(
            wrapped_key_info { encrypted_dek: "$0" kek_kms_resource_id: "$1" }
          )pb",
          kTestDek, kGcpKmsResourceName));
  EXPECT_CALL(mock_aws_key_handler_, GetKey).Times(0);
  EXPECT_CALL(mock_gcp_key_handler_, GetKey).Times(0);

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INVALID_DEK);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyGenericFailure) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(
          ExecutionResultOr<std::string>(FailureExecutionResult(SC_UNKNOWN))));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status),
                  Error::WRAPPED_KEY_FETCHING_ERROR);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyKeysetReadFailure) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey)
      .WillOnce(Return(ExecutionResultOr<std::string>("invalid_keyset_data")));

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INVALID_DEK);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyScheduleFailure) {
  mock_async_executor_.schedule_mock = [](const AsyncOperation& work) {
    return FailureExecutionResult(SC_UNKNOWN);
  };
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(mock_gcp_key_handler_, GetKey).Times(0);

  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_FALSE(ctx.status.ok());
        EXPECT_EQ(GetBackendErrorReason(ctx.status), Error::INTERNAL_ERROR);
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::match_service
