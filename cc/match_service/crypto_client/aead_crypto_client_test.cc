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

#include "cc/match_service/crypto_client/aead_crypto_client.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"
#include "proto/aes_gcm.pb.h"
#include "proto/tink.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/kms_client/mock_kms_client.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::cmrt::sdk::kms_service::v1::DecryptRequest;
using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::Error;
using ::google::crypto::tink::AesGcmKey;
using ::google::crypto::tink::Keyset;
using ::google::scp::core::test::WaitUntil;
using ::testing::_;
using ::testing::Eq;

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
constexpr absl::string_view kAwsKmsRegion = "us-east-2";
constexpr absl::string_view kRoleArn =
    "arn:aws:iam::123456789012:role/cfm-test-role";
constexpr absl::string_view kAudience = "https://confidential-match";
const std::vector<std::string>& kDefaultSignatures = {"123"};

class AeadCryptoClientTest : public testing::Test {
 protected:
  AeadCryptoClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_aws_kms_client_(std::make_shared<MockKmsClient>()),
        mock_gcp_kms_client_(std::make_shared<MockKmsClient>()),
        // Pass raw pointers to the constructor
        crypto_client_(std::make_unique<AeadCryptoClient>(
            mock_aws_kms_client_.get(), mock_gcp_kms_client_.get(),
            kDefaultSignatures, kAudience)) {}

  std::shared_ptr<LoggerInterface> logger_;
  std::shared_ptr<MockKmsClient> mock_aws_kms_client_;
  std::shared_ptr<MockKmsClient> mock_gcp_kms_client_;
  std::unique_ptr<CryptoClientInterface> crypto_client_;
};

EncryptionKeyInfo GetAwsEncryptionKeyInfo(
    absl::string_view kek_id = kAwsKmsResourceName,
    absl::string_view audience = kAudience) {
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
  return encryption_key_info;
}

TEST_F(AeadCryptoClientTest, StartStop) {
  EXPECT_TRUE(crypto_client_->Init().ok());
  EXPECT_TRUE(crypto_client_->Run().ok());
  EXPECT_TRUE(crypto_client_->Stop().ok());
}

TEST_F(AeadCryptoClientTest, GetGcpCryptoKeySuccess) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  EXPECT_CALL(*mock_gcp_kms_client_, Decrypt).WillOnce([&](auto ctx) {
    EXPECT_EQ(ctx.request->key_resource_name(), kGcpKmsResourceName);
    ctx.response = std::make_shared<std::string>(decoded_dek);
    ctx.status = absl::OkStatus();
    ctx.Finish();
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

TEST_F(AeadCryptoClientTest, GetAwsCryptoKeySuccess) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetAwsEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  EXPECT_CALL(*mock_aws_kms_client_, Decrypt).WillOnce([&](auto ctx) {
    EXPECT_EQ(ctx.request->key_resource_name(), kAwsKmsResourceName);
    EXPECT_EQ(ctx.request->kms_region(), kAwsKmsRegion);
    ctx.response = std::make_shared<std::string>(decoded_dek);
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });
  std::atomic<bool> is_complete = false;
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context(
      request,
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        EXPECT_TRUE(ctx.status.ok());
        is_complete = true;
      },
      logger_);

  crypto_client_->GetCryptoKey(context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(AeadCryptoClientTest, GetCryptoKeyKmsFailure) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(*mock_gcp_kms_client_, Decrypt).WillOnce([](auto ctx) {
    ctx.status =
        Status(Error::WRAPPED_KEY_FETCHING_ERROR, "Error fetching wrapped key");
    ctx.Finish();
  });
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

TEST_F(AeadCryptoClientTest, GetCryptoKeyKeysetReadFailure) {
  auto request = std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(*mock_gcp_kms_client_, Decrypt).WillOnce([](auto ctx) {
    ctx.response = std::make_shared<std::string>("invalid_keyset_data");
    ctx.status = absl::OkStatus();
    ctx.Finish();
  });
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

}  // namespace
}  // namespace google::confidential_match::match_service
