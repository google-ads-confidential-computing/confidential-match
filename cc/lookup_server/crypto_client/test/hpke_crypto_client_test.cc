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

#include "cc/lookup_server/crypto_client/src/hpke_crypto_client.h"

#include <memory>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/interface/crypto_client/crypto_client_interface.h"
#include "cc/public/cpio/mock/crypto_client/mock_crypto_client.h"
#include "cc/public/cpio/proto/crypto_service/v1/crypto_service.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/core/test/src/parse_text_proto.h"
#include "cc/lookup_server/coordinator_client/mock/mock_coordinator_client.h"
#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::crypto_service::v1::HpkeDecryptRequest;
using ::google::cmrt::sdk::crypto_service::v1::HpkeDecryptResponse;
using ::google::cmrt::sdk::crypto_service::v1::HpkeEncryptRequest;
using ::google::cmrt::sdk::crypto_service::v1::HpkeEncryptResponse;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::MockCryptoClient;
using ::std::placeholders::_1;
using ::testing::Eq;
using ::testing::Return;

constexpr absl::string_view kPublicKey = "sample-public-key";
constexpr absl::string_view kPrivateKey = "sample-private-key";
constexpr absl::string_view kPlaintext = "sample-plaintext";
constexpr absl::string_view kCiphertext = "sample-ciphertext";
constexpr absl::string_view kKeyId = "testKeyId";
constexpr absl::string_view kKeyServiceEndpoint1 =
    "https://test-privatekeyservice-1.google.com/v1alpha";
constexpr absl::string_view kKeyServiceEndpoint2 =
    "https://test-privatekeyservice-2.google.com/v1alpha";
constexpr absl::string_view kAccountIdentity1 =
    "test-verified-user@coordinator1.iam.gserviceaccount.com";
constexpr absl::string_view kAccountIdentity2 =
    "test-verified-user@coordinator2.iam.gserviceaccount.com";
constexpr absl::string_view kWipProvider1 =
    "projects/1/locations/global/workloadIdentityPools/wip/providers/provider";
constexpr absl::string_view kWipProvider2 =
    "projects/2/locations/global/workloadIdentityPools/wip/providers/provider";
constexpr absl::string_view kKeyServiceAudienceUrl1 =
    "https://test-key-service-audience-url-1.a.run.app";
constexpr absl::string_view kKeyServiceAudienceUrl2 =
    "https://test-key-service-audience-url-2.a.run.app";

/**
 * @brief Helper used to construct HpkeCryptoClient with mocked dependencies.
 */
class MockableHpkeCryptoClient : public HpkeCryptoClient {
 public:
  explicit MockableHpkeCryptoClient(
      std::shared_ptr<CoordinatorClientInterface> coordinator_client,
      std::shared_ptr<scp::cpio::CryptoClientInterface> cpio_crypto_client)
      : HpkeCryptoClient(coordinator_client, cpio_crypto_client) {}
};

class HpkeCryptoClientTest : public testing::Test {
 protected:
  HpkeCryptoClientTest()
      : crypto_client_(std::make_shared<MockableHpkeCryptoClient>(
            mock_coordinator_client_, mock_cpio_crypto_client_)) {}

  std::shared_ptr<MockCryptoClient> mock_cpio_crypto_client_ =
      std::make_shared<MockCryptoClient>();
  std::shared_ptr<MockCoordinatorClient> mock_coordinator_client_ =
      std::make_shared<MockCoordinatorClient>();
  std::shared_ptr<HpkeCryptoClient> crypto_client_;
};

// Helper to create EncryptionKeyInfo proto.
EncryptionKeyInfo CreateEncryptionKeyInfo() {
  return ParseTextProtoOrDie(R"pb(
    coordinator_key_info {
      key_id: "testKeyId"
      coordinator_info {
        key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
        kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
        kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
        key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
      }
      coordinator_info {
        key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
        kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
        kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
        key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
      }
    }
  )pb");
}

// Helper to create the expected GetHybridKeyRequest.
GetHybridKeyRequest CreateExpectedGetHybridKeyRequest() {
  GetHybridKeyRequest expected_key_request;
  expected_key_request.set_key_id(kKeyId);
  GetHybridKeyRequest::Coordinator* coordinator1 =
      expected_key_request.add_coordinators();
  coordinator1->set_key_service_endpoint(kKeyServiceEndpoint1);
  coordinator1->set_account_identity(kAccountIdentity1);
  coordinator1->set_kms_wip_provider(kWipProvider1);
  coordinator1->set_key_service_audience_url(kKeyServiceAudienceUrl1);
  GetHybridKeyRequest::Coordinator* coordinator2 =
      expected_key_request.add_coordinators();
  coordinator2->set_key_service_endpoint(kKeyServiceEndpoint2);
  coordinator2->set_account_identity(kAccountIdentity2);
  coordinator2->set_kms_wip_provider(kWipProvider2);
  coordinator2->set_key_service_audience_url(kKeyServiceAudienceUrl2);
  return expected_key_request;
}

// Helper to simulate a successful response from a hybrid key fetch.
void MockGetHybridKeyWithResponse(
    const GetHybridKeyResponse& response,
    const GetHybridKeyRequest& expected_request,
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        get_key_context) noexcept {
  EXPECT_THAT(*get_key_context.request, EqualsProto(expected_request));
  get_key_context.result = SuccessExecutionResult();
  get_key_context.response = std::make_shared<GetHybridKeyResponse>(response);
  get_key_context.Finish();
}

// Helper to simulate a failed response from a hybrid key fetch.
void MockGetHybridKeyWithFailure(
    ExecutionResult failure_result, const GetHybridKeyRequest& expected_request,
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        get_key_context) noexcept {
  EXPECT_THAT(*get_key_context.request, EqualsProto(expected_request));
  get_key_context.result = failure_result;
  get_key_context.Finish();
}

TEST_F(HpkeCryptoClientTest, StartStop) {
  auto crypto_client = std::make_unique<MockableHpkeCryptoClient>(
      mock_coordinator_client_, mock_cpio_crypto_client_);
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));

  EXPECT_SUCCESS(crypto_client->Init());
  EXPECT_SUCCESS(crypto_client->Run());
  EXPECT_SUCCESS(crypto_client->Stop());
}

TEST_F(HpkeCryptoClientTest, GetCryptoKeySuccess) {
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, GetHybridKeyResponse(),
                          CreateExpectedGetHybridKeyRequest(), _1));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, GetCryptoKeyWithoutRunningReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync).Times(0);

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());
  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(
            ctx.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, GetCryptoKeyAfterStoppingReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());
  EXPECT_SUCCESS(crypto_client_->Stop());
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync).Times(0);

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());
  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(
            ctx.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, CryptoKeyEncryptSuccess) {
  HpkeEncryptRequest expected_cpio_request;
  expected_cpio_request.mutable_raw_key_with_params()->set_raw_key(kPublicKey);
  expected_cpio_request.mutable_raw_key_with_params()
      ->mutable_hpke_params()
      ->set_kem(
          cmrt::sdk::crypto_service::v1::HpkeKem::DHKEM_X25519_HKDF_SHA256);
  expected_cpio_request.mutable_raw_key_with_params()
      ->mutable_hpke_params()
      ->set_kdf(cmrt::sdk::crypto_service::v1::HpkeKdf::HKDF_SHA256);
  expected_cpio_request.mutable_raw_key_with_params()
      ->mutable_hpke_params()
      ->set_aead(cmrt::sdk::crypto_service::v1::HpkeAead::CHACHA20_POLY1305);
  expected_cpio_request.set_payload(kPlaintext);
  HpkeEncryptResponse mock_cpio_response;
  mock_cpio_response.mutable_encrypted_data()->set_ciphertext(kCiphertext);
  EXPECT_CALL(*mock_cpio_crypto_client_,
              HpkeEncryptSync(EqualsProto(expected_cpio_request)))
      .WillOnce(Return(mock_cpio_response));

  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());
  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response,
                          CreateExpectedGetHybridKeyRequest(), _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);

        ExecutionResultOr<std::string> ciphertext_or =
            ctx.response->Encrypt(kPlaintext);

        EXPECT_SUCCESS(ciphertext_or);
        EXPECT_EQ(*ciphertext_or, kCiphertext);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, CryptoKeyEncryptFailureReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, HpkeEncryptSync)
      .WillOnce(Return(FailureExecutionResult(1)));
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response,
                          CreateExpectedGetHybridKeyRequest(), _1));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);

        ExecutionResultOr<std::string> ciphertext_or =
            ctx.response->Encrypt(kPlaintext);
        EXPECT_THAT(
            ciphertext_or,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_ENCRYPT_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, CryptoKeyEncryptAfterStoppedReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());
  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response,
                          CreateExpectedGetHybridKeyRequest(), _1));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());

  std::atomic<bool> is_complete = false;
  context.callback =
      [this,
       &is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
        EXPECT_SUCCESS(crypto_client_->Stop());

        ExecutionResultOr<std::string> ciphertext_or =
            ctx.response->Encrypt(kPlaintext);
        EXPECT_THAT(
            ciphertext_or,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, CryptoKeyDecryptSuccess) {
  HpkeDecryptRequest expected_cpio_request;
  expected_cpio_request.set_tink_key_binary(kPrivateKey);
  expected_cpio_request.mutable_encrypted_data()->set_ciphertext(kCiphertext);
  HpkeDecryptResponse mock_cpio_response;
  mock_cpio_response.set_payload(kPlaintext);
  EXPECT_CALL(*mock_cpio_crypto_client_,
              HpkeDecryptSync(EqualsProto(expected_cpio_request)))
      .WillOnce(Return(mock_cpio_response));

  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());
  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response,
                          CreateExpectedGetHybridKeyRequest(), _1));

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);

        ExecutionResultOr<std::string> plaintext_or =
            ctx.response->Decrypt(kCiphertext);

        EXPECT_SUCCESS(plaintext_or);
        EXPECT_EQ(*plaintext_or, kPlaintext);
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, CryptoKeyDecryptFailureReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, HpkeDecryptSync)
      .WillOnce(Return(FailureExecutionResult(1)));
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response,
                          CreateExpectedGetHybridKeyRequest(), _1));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);

        ExecutionResultOr<std::string> plaintext_or =
            ctx.response->Decrypt(kCiphertext);
        EXPECT_THAT(
            plaintext_or,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, CryptoKeyDecryptAfterStoppedReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_private_key(kPrivateKey);
  response.mutable_hybrid_key()->set_public_key(kPublicKey);
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(std::bind(MockGetHybridKeyWithResponse, response,
                          CreateExpectedGetHybridKeyRequest(), _1));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());

  std::atomic<bool> is_complete = false;
  context.callback =
      [this,
       &is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_SUCCESS(ctx.result);
        EXPECT_SUCCESS(crypto_client_->Stop());

        ExecutionResultOr<std::string> plaintext_or =
            ctx.response->Decrypt(kCiphertext);
        EXPECT_THAT(
            plaintext_or,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_NOT_RUNNING_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

TEST_F(HpkeCryptoClientTest, GetCryptoKeyCoordinatorErrorReturnsError) {
  EXPECT_CALL(*mock_cpio_crypto_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_crypto_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_SUCCESS(crypto_client_->Init());
  EXPECT_SUCCESS(crypto_client_->Run());
  EXPECT_CALL(*mock_coordinator_client_, GetHybridKeyAsync)
      .WillOnce(
          std::bind(MockGetHybridKeyWithFailure,
                    FailureExecutionResult(COORDINATOR_CLIENT_KEY_FETCH_ERROR),
                    CreateExpectedGetHybridKeyRequest(), _1));

  AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context;
  context.request =
      std::make_shared<EncryptionKeyInfo>(CreateEncryptionKeyInfo());

  std::atomic<bool> is_complete = false;
  context.callback =
      [&is_complete](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>& ctx) {
        is_complete = true;
        EXPECT_THAT(ctx.result, ResultIs(FailureExecutionResult(
                                    COORDINATOR_CLIENT_KEY_FETCH_ERROR)));
      };

  crypto_client_->GetCryptoKey(context);
  WaitUntil([&]() { return is_complete.load(); });
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
