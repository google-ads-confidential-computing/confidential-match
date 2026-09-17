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

#include "cc/match_service/crypto_client/hybrid_crypto_key.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "cc/match_service/error/error.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/match_service/backend/error.pb.h"
#include "tink/hybrid_decrypt.h"
#include "tink/hybrid_encrypt.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::crypto::tink::HybridDecrypt;
using ::crypto::tink::HybridEncrypt;
using ::google::confidential_match::match_service::backend::Error;
using ::testing::Optional;
using ::testing::Return;

constexpr absl::string_view kPlaintext = "plaintext";
constexpr absl::string_view kCiphertext = "ciphertext";

class MockHybridDecrypt : public HybridDecrypt {
 public:
  MOCK_METHOD(absl::StatusOr<std::string>, Decrypt,
              (absl::string_view, absl::string_view), (const, override));
};

class MockHybridEncrypt : public HybridEncrypt {
 public:
  MOCK_METHOD(absl::StatusOr<std::string>, Encrypt,
              (absl::string_view, absl::string_view), (const, override));
};

class HybridCryptoKeyTest : public testing::Test {
 protected:
  HybridCryptoKeyTest()
      : mock_hybrid_decrypt_(std::make_shared<MockHybridDecrypt>()),
        mock_hybrid_encrypt_(std::make_shared<MockHybridEncrypt>()),
        crypto_key_(mock_hybrid_decrypt_, mock_hybrid_encrypt_) {}

  std::shared_ptr<MockHybridDecrypt> mock_hybrid_decrypt_;
  std::shared_ptr<MockHybridEncrypt> mock_hybrid_encrypt_;
  HybridCryptoKey crypto_key_;
};

TEST_F(HybridCryptoKeyTest, EncryptIsSuccessful) {
  EXPECT_CALL(*mock_hybrid_encrypt_, Encrypt(kPlaintext, ""))
      .WillOnce(Return(absl::StatusOr<std::string>(kCiphertext)));
  auto result = crypto_key_.Encrypt(kPlaintext);

  ASSERT_THAT(result, IsOkAndHolds(kCiphertext));
}

TEST_F(HybridCryptoKeyTest, EncryptWithErrorReturnsFailure) {
  EXPECT_CALL(*mock_hybrid_encrypt_, Encrypt(kPlaintext, ""))
      .WillOnce(Return(absl::InternalError("Failed encrypting")));

  auto result = crypto_key_.Encrypt(kPlaintext);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(GetBackendErrorReason(result.status()),
              Optional(Error::ENCRYPTION_ERROR));
}

TEST_F(HybridCryptoKeyTest, DecryptIsSuccessful) {
  EXPECT_CALL(*mock_hybrid_decrypt_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(absl::StatusOr<std::string>(kPlaintext)));
  auto result = crypto_key_.Decrypt(kCiphertext);

  ASSERT_THAT(result, IsOkAndHolds(kPlaintext));
}

TEST_F(HybridCryptoKeyTest, DecryptWithErrorReturnsFailure) {
  EXPECT_CALL(*mock_hybrid_decrypt_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(absl::InternalError("Failed decrypting")));

  auto result = crypto_key_.Decrypt(kCiphertext);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(GetBackendErrorReason(result.status()),
              Optional(Error::DECRYPTION_ERROR));
}

TEST_F(HybridCryptoKeyTest, EncryptFailsIfNoEncryptPrimitive) {
  HybridCryptoKey key(mock_hybrid_decrypt_, /*hybrid_encrypt=*/nullptr);
  auto result = key.Encrypt(kPlaintext);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(HybridCryptoKeyTest, DecryptFailsIfNoDecryptPrimitive) {
  HybridCryptoKey key(/*hybrid_decrypt=*/nullptr, mock_hybrid_encrypt_);
  auto result = key.Decrypt(kCiphertext);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(HybridCryptoKeyTest, EncryptSucceedsEvenIfNoDecryptPrimitive) {
  HybridCryptoKey key(/*hybrid_decrypt=*/nullptr, mock_hybrid_encrypt_);
  EXPECT_CALL(*mock_hybrid_encrypt_, Encrypt(kPlaintext, ""))
      .WillOnce(Return(absl::StatusOr<std::string>(kCiphertext)));
  auto result = key.Encrypt(kPlaintext);

  ASSERT_THAT(result, IsOkAndHolds(kCiphertext));
}

TEST_F(HybridCryptoKeyTest, DecryptSucceedsEvenIfNoEncryptPrimitive) {
  HybridCryptoKey key(mock_hybrid_decrypt_, /*hybrid_encrypt=*/nullptr);
  EXPECT_CALL(*mock_hybrid_decrypt_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(absl::StatusOr<std::string>(kPlaintext)));
  auto result = key.Decrypt(kCiphertext);

  ASSERT_THAT(result, IsOkAndHolds(kPlaintext));
}

}  // namespace
}  // namespace google::confidential_match::match_service
