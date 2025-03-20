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

#include "cc/lookup_server/crypto_client/src/aead_crypto_client.h"

#include <memory>
#include <vector>

#include "absl/strings/escaping.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "external/tink_cc/proto/aes_gcm.pb.h"
#include "external/tink_cc/proto/tink.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/kms_client/mock/mock_kms_client.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::crypto::tink::AesGcmKey;
using ::google::crypto::tink::Keyset;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;

// Valid Decrypted DEK
constexpr absl::string_view kTestDek =
    "CP24wPEEEmcKWwozdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2"
    "NtU2l2S2V5EiIaIObVHDAKMgQg0ww0qfsLoKzu0ui90WbHAqiJf8qayOqsGAEQARj9uMDxBCA"
    "B";
// Example MatchDataRecord
constexpr absl::string_view kTestRecord =
    "AU4wHH2artjSbMKfjwInDVsOefEjtqGVDcBwNUlQO+"
    "5pmzv3EQQA6htTs3lqMOBr6PpbQ4NGlZ5rd2gcOZUHu52sSXtW8fvC8rAkvDTWpToPXg9MBkeB"
    "FkryL8JMmoKZIzKNfJWLrQGZHtWgmoBYp1zdYDFqW/lAm1oN+oOLICjfTJF2/"
    "fRzlOjU24pR2CiqF69KFmjvSJoxPgIIlNOMdFB5ytfBYKYday8=";

constexpr absl::string_view kGcpKmsResourceName =
    "projects/test-project/locations/test-location/keyRings/test-keyring/"
    "cryptoKeys/test-key";
constexpr absl::string_view kGcpKmsResourceNameWithResourcePrefix =
    "gcp-kms://projects/test-project/locations/test-location/keyRings/"
    "test-keyring/cryptoKeys/test-key";
constexpr absl::string_view kWipProvider =
    "projects/test-project/locations/test-location/"
    "workloadIdentityPools/test-wip/providers/test-provider";
constexpr absl::string_view kVerifiedIdentity =
    "verified@test-project.iam.gserviceaccount.com";
constexpr absl::string_view kAwsKmsResourceName =
    "arn:aws:kms:us-east-2:123456789012:key/test-key";
constexpr absl::string_view kAwsKmsRegion = "us-east-2";
constexpr absl::string_view kAwsKmsResourceNameWithResourcePrefix =
    "aws-kms://arn:aws:kms:us-east-2:123456789012:key/test-key";
constexpr absl::string_view kRoleArn =
    "arn:aws:iam::123456789012:role/cfm-test-role";
constexpr absl::string_view kAudience = "https://confidential-match";

const std::vector<std::string>& kDefaultSignatures = {"123"};

// Helper to simulate a successful response from a KMS decryption operation.
ExecutionResult MockKmsDecryptWithResponse(
    absl::string_view response,
    AsyncContext<DecryptRequest, std::string> decrypt_context) noexcept {
  decrypt_context.result = SuccessExecutionResult();
  decrypt_context.response = std::make_shared<std::string>(response);
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

// Helper to simulate a successful response from a KMS decryption operation.
ExecutionResult MockKmsDecryptWithResponseValidatingRequest(
    const DecryptRequest& expected_request, absl::string_view response,
    AsyncContext<DecryptRequest, std::string> decrypt_context) noexcept {
  EXPECT_THAT(*decrypt_context.request, EqualsProto(expected_request));
  decrypt_context.result = SuccessExecutionResult();
  decrypt_context.response = std::make_shared<std::string>(response);
  decrypt_context.Finish();
  return SuccessExecutionResult();
}

class AeadCryptoClientTest : public testing::Test {
 protected:
  AeadCryptoClientTest()
      : mock_aws_kms_client_(std::make_shared<MockKmsClient>()),
        mock_gcp_kms_client_(std::make_shared<MockKmsClient>()),
        crypto_client_(std::make_unique<AeadCryptoClient>(
            mock_aws_kms_client_, mock_gcp_kms_client_, kDefaultSignatures,
            kAudience)) {}

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
  wrapped_key_info->mutable_aws_wrapped_key_info()->set_audience(audience);
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
  keyset.set_primary_key_id(0);
  auto key = keyset.add_key();
  key->set_status(google::crypto::tink::KeyStatusType::ENABLED);
  key->set_key_id(0);
  key->set_output_prefix_type(google::crypto::tink::OutputPrefixType::RAW);
  auto key_data = key->mutable_key_data();
  key_data->set_type_url("type.googleapis.com/google.crypto.tink.AesGcmKey");
  key_data->set_key_material_type(google::crypto::tink::KeyData::SYMMETRIC);
  AesGcmKey aes_gcm_key;
  aes_gcm_key.set_version(0);
  aes_gcm_key.set_key_value("raw_key_");
  key_data->set_value(aes_gcm_key.SerializeAsString());
  return keyset;
}

TEST_F(AeadCryptoClientTest, StartStop) {
  AeadCryptoClient crypto_client(std::make_shared<MockKmsClient>(),
                                 std::make_shared<MockKmsClient>(),
                                 kDefaultSignatures, kAudience);
  EXPECT_SUCCESS(crypto_client.Init());
  EXPECT_SUCCESS(crypto_client.Run());
  EXPECT_SUCCESS(crypto_client.Stop());
}

TEST_F(AeadCryptoClientTest, GetGcpCryptoKeySuccess) {
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  DecryptRequest expected_request;
  expected_request.set_key_resource_name(kGcpKmsResourceName);
  expected_request.set_ciphertext(kTestDek);
  expected_request.set_gcp_wip_provider(kWipProvider);
  expected_request.set_account_identity(kVerifiedIdentity);
  EXPECT_CALL(*mock_gcp_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockKmsDecryptWithResponseValidatingRequest,
                          expected_request, decoded_dek, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(AeadCryptoClientTest, GetAwsCryptoKeySuccess) {
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetAwsEncryptionKeyInfo());
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  DecryptRequest expected_request;
  expected_request.set_key_resource_name(kAwsKmsResourceName);
  expected_request.set_ciphertext(kTestDek);
  expected_request.set_account_identity(kRoleArn);
  expected_request.set_target_audience_for_web_identity(kAudience);
  expected_request.set_kms_region(kAwsKmsRegion);
  expected_request.add_key_ids(kDefaultSignatures[0]);
  EXPECT_CALL(*mock_aws_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockKmsDecryptWithResponseValidatingRequest,
                          expected_request, decoded_dek, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(AeadCryptoClientTest, GetCryptoKeysetReadFailure) {
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(*mock_gcp_kms_client_, DecryptAsync)
      .WillOnce(
          std::bind(MockKmsDecryptWithResponse, /*response=*/"invalid", _1));

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

TEST_F(AeadCryptoClientTest, GetCryptoKeyFailure) {
  std::string keyset_bytes;
  auto keyset = GetKeysetWithEmptyPrimitive();
  EXPECT_TRUE(keyset.SerializeToString(&keyset_bytes));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(GetGcpEncryptionKeyInfo());
  EXPECT_CALL(*mock_gcp_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockKmsDecryptWithResponse, keyset_bytes, _1));

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

TEST_F(AeadCryptoClientTest, GetGcpCryptoKeyWithGcpPrefixSuccess) {
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request = std::make_shared<EncryptionKeyInfo>(
      GetGcpEncryptionKeyInfo(kGcpKmsResourceNameWithResourcePrefix));
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  DecryptRequest expected_request;
  expected_request.set_key_resource_name(kGcpKmsResourceName);
  expected_request.set_ciphertext(kTestDek);
  expected_request.set_gcp_wip_provider(kWipProvider);
  expected_request.set_account_identity(kVerifiedIdentity);
  EXPECT_CALL(*mock_gcp_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockKmsDecryptWithResponseValidatingRequest,
                          expected_request, decoded_dek, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(AeadCryptoClientTest, GetAwsCryptoKeyWithAwsPrefixSuccess) {
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request = std::make_shared<EncryptionKeyInfo>(
      GetAwsEncryptionKeyInfo(kAwsKmsResourceNameWithResourcePrefix));
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  DecryptRequest expected_request;
  expected_request.set_key_resource_name(kAwsKmsResourceName);
  expected_request.set_ciphertext(kTestDek);
  expected_request.set_account_identity(kRoleArn);
  expected_request.set_target_audience_for_web_identity(kAudience);
  expected_request.set_kms_region(kAwsKmsRegion);
  expected_request.add_key_ids(kDefaultSignatures[0]);
  EXPECT_CALL(*mock_aws_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockKmsDecryptWithResponseValidatingRequest,
                          expected_request, decoded_dek, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(AeadCryptoClientTest, GetAwsCryptoKeyUsesDefaultAudienceSuccess) {
  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request = std::make_shared<EncryptionKeyInfo>(
      GetAwsEncryptionKeyInfo(kAwsKmsResourceNameWithResourcePrefix, " "));
  std::string decoded_dek;
  EXPECT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  DecryptRequest expected_request;
  expected_request.set_key_resource_name(kAwsKmsResourceName);
  expected_request.set_ciphertext(kTestDek);
  expected_request.set_account_identity(kRoleArn);
  expected_request.set_target_audience_for_web_identity(kAudience);
  expected_request.set_kms_region(kAwsKmsRegion);
  expected_request.add_key_ids(kDefaultSignatures[0]);
  EXPECT_CALL(*mock_aws_kms_client_, DecryptAsync)
      .WillOnce(std::bind(MockKmsDecryptWithResponseValidatingRequest,
                          expected_request, decoded_dek, _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
