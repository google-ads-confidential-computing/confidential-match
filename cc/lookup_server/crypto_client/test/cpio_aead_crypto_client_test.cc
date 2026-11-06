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

#include "cc/lookup_server/crypto_client/src/cpio_aead_crypto_client.h"

#include <atomic>
#include <memory>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/utils/key_fetching/proto/cloud_wrapped_key.pb.h"
#include "cc/public/cpio/utils/key_fetching/src/wrapped_key_handler_with_cache_mock.h"
#include "proto/aes_gcm.pb.h"
#include "proto/tink.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::v1::CloudWrappedKey;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::crypto::tink::AesGcmKey;
using ::google::crypto::tink::Keyset;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::WrappedKeyHandlerWithCacheMock;
using ::testing::Return;

// Valid Decrypted DEK
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
constexpr absl::string_view kVerifiedIdentity =
    "verified@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kAwsKmsResourceName =
    "arn:aws:kms:us-east-2:123456789012:key/test-key";
constexpr absl::string_view kRoleArn =
    "arn:aws:iam::123456789012:role/cfm-test-role";

class CpioAeadCryptoClientTest : public testing::Test {
 protected:
  CpioAeadCryptoClientTest()
      : mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_aws_wrapped_key_handler_(
            std::make_shared<WrappedKeyHandlerWithCacheMock>()),
        mock_gcp_wrapped_key_handler_(
            std::make_shared<WrappedKeyHandlerWithCacheMock>()),
        crypto_client_(std::make_unique<CpioAeadCryptoClient>(
            mock_async_executor_, mock_aws_wrapped_key_handler_,
            mock_gcp_wrapped_key_handler_)) {}

  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::shared_ptr<WrappedKeyHandlerWithCacheMock>
      mock_aws_wrapped_key_handler_;
  std::shared_ptr<WrappedKeyHandlerWithCacheMock>
      mock_gcp_wrapped_key_handler_;
  std::unique_ptr<CryptoClientInterface> crypto_client_;
};

EncryptionKeyInfo GetAwsEncryptionKeyInfo(
    absl::string_view kek_id = kAwsKmsResourceName) {
  EncryptionKeyInfo encryption_key_info;
  auto* wrapped_key_info = encryption_key_info.mutable_wrapped_key_info();
  wrapped_key_info->set_encrypted_dek(kTestDek);
  wrapped_key_info->set_kek_kms_resource_id(kek_id);
  wrapped_key_info->mutable_aws_wrapped_key_info()->set_role_arn(kRoleArn);
  return encryption_key_info;
}

EncryptionKeyInfo GetGcpEncryptionKeyInfo(
    absl::string_view kek_id = kGcpKmsResourceName) {
  EncryptionKeyInfo encryption_key_info;
  auto* wrapped_key_info = encryption_key_info.mutable_wrapped_key_info();
  wrapped_key_info->set_encrypted_dek(kTestDek);
  wrapped_key_info->set_kek_kms_resource_id(kek_id);
  wrapped_key_info->mutable_gcp_wrapped_key_info()->set_wip_provider(
      kWipProvider);
  wrapped_key_info->mutable_gcp_wrapped_key_info()
      ->set_service_account_to_impersonate(kVerifiedIdentity);
  return encryption_key_info;
}

Keyset GetKeysetWithEmptyPrimitive() {
  Keyset keyset;
  keyset.set_primary_key_id(1);
  auto key = keyset.add_key();
  key->set_status(google::crypto::tink::KeyStatusType::ENABLED);
  key->set_key_id(0);
  key->set_output_prefix_type(google::crypto::tink::OutputPrefixType::RAW);
  auto key_data = key->mutable_key_data();
  key_data->set_type_url("type.googleapis.com/google.crypto.tink.AesGcmKey");
  key_data->set_key_material_type(google::crypto::tink::KeyData::SYMMETRIC);
  AesGcmKey aes_gcm_key;
  aes_gcm_key.set_version(0);
  aes_gcm_key.set_key_value("0123456789abcdef");
  key_data->set_value(aes_gcm_key.SerializeAsString());
  return keyset;
}

TEST_F(CpioAeadCryptoClientTest, StartStop) {
  CpioAeadCryptoClient crypto_client(
      std::make_shared<MockAsyncExecutor>(),
      std::make_shared<WrappedKeyHandlerWithCacheMock>(),
      std::make_shared<WrappedKeyHandlerWithCacheMock>());
  EXPECT_SUCCESS(crypto_client.Init());
  EXPECT_SUCCESS(crypto_client.Run());
  EXPECT_SUCCESS(crypto_client.Stop());
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyScheduleFailure) {
  EXPECT_SUCCESS(crypto_client_->Run());
  ExecutionResult schedule_failure = FailureExecutionResult(SC_UNKNOWN);
  mock_async_executor_->schedule_mock =
      [&schedule_failure](const AsyncOperation&) { return schedule_failure; };

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());

  EXPECT_CALL(*mock_gcp_wrapped_key_handler_, GetKey).Times(0);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete, &schedule_failure](
          AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(ctx.result, ResultIs(schedule_failure));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyMissingCloudWrappedKeyInfo) {
  EXPECT_SUCCESS(crypto_client_->Run());
  EncryptionKeyInfo encryption_key_info;
  encryption_key_info.mutable_wrapped_key_info()->set_encrypted_dek(kTestDek);
  encryption_key_info.mutable_wrapped_key_info()->set_kek_kms_resource_id(
      kGcpKmsResourceName);

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request = std::make_shared<EncryptionKeyInfo>(encryption_key_info);

  EXPECT_CALL(*mock_aws_wrapped_key_handler_, GetKey).Times(0);
  EXPECT_CALL(*mock_gcp_wrapped_key_handler_, GetKey).Times(0);

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(
            ctx.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetGcpCryptoKeySuccess) {
  EXPECT_SUCCESS(crypto_client_->Run());
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));

  CloudWrappedKey expected_key;
  auto* gcp_key = expected_key.mutable_gcp_wrapped_key();
  gcp_key->set_encrypted_dek(kTestDek);
  gcp_key->set_kek_uri(kGcpKmsResourceName);
  gcp_key->set_wip_provider(kWipProvider);

  EXPECT_CALL(*mock_gcp_wrapped_key_handler_, GetKey(EqualsProto(expected_key)))
      .WillOnce(Return(decoded_dek));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
        EXPECT_NE(ctx.response, nullptr);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetAwsCryptoKeySuccess) {
  EXPECT_SUCCESS(crypto_client_->Run());
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetAwsEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));

  CloudWrappedKey expected_key;
  auto* aws_key = expected_key.mutable_aws_wrapped_key();
  aws_key->set_encrypted_dek(kTestDek);
  aws_key->set_kek_uri(kAwsKmsResourceName);
  aws_key->set_role_arn(kRoleArn);

  EXPECT_CALL(*mock_aws_wrapped_key_handler_, GetKey(EqualsProto(expected_key)))
      .WillOnce(Return(decoded_dek));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
        EXPECT_NE(ctx.response, nullptr);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyHandlerFailure) {
  EXPECT_SUCCESS(crypto_client_->Run());
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());

  ExecutionResult handler_failure = FailureExecutionResult(SC_UNKNOWN);
  EXPECT_CALL(*mock_gcp_wrapped_key_handler_, GetKey)
      .WillOnce(Return(handler_failure));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(
            ctx.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeysetReadFailure) {
  EXPECT_SUCCESS(crypto_client_->Run());
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(*mock_gcp_wrapped_key_handler_, GetKey)
      .WillOnce(Return(std::string("invalid")));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(
            ctx.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_KEYSET_READ_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(CpioAeadCryptoClientTest, GetCryptoKeyGetAeadFailure) {
  EXPECT_SUCCESS(crypto_client_->Run());
  std::string keyset_bytes;
  auto keyset = GetKeysetWithEmptyPrimitive();
  EXPECT_TRUE(keyset.SerializeToString(&keyset_bytes));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(*mock_gcp_wrapped_key_handler_, GetKey)
      .WillOnce(Return(keyset_bytes));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(
            ctx.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_GET_AEAD_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
