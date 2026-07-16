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

#include "cc/match_service/crypto_client/aead_crypto_key.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "tink/aead/mock_aead.h"
#include "tink/util/status.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::crypto::tink::MockAead;
using ::crypto::tink::util::Status;
using ::google::confidential_match::match_service::backend::Error;
using ::testing::Return;

constexpr absl::string_view kPlaintext = "plaintext";
constexpr absl::string_view kCiphertext = "ciphertext";

class AeadCryptoKeyTest : public testing::Test {
 protected:
  AeadCryptoKeyTest()
      : mock_aead_(std::make_shared<MockAead>()), crypto_key_(mock_aead_) {}

  std::shared_ptr<MockAead> mock_aead_;
  AeadCryptoKey crypto_key_;
};

TEST_F(AeadCryptoKeyTest, EncryptIsSuccessful) {
  EXPECT_CALL(*mock_aead_, Encrypt(kPlaintext, ""))
      .WillOnce(Return(std::string(kCiphertext)));

  auto result = crypto_key_.Encrypt(kPlaintext);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, kCiphertext);
}

TEST_F(AeadCryptoKeyTest, EncryptWithErrorReturnsFailure) {
  EXPECT_CALL(*mock_aead_, Encrypt(kPlaintext, ""))
      .WillOnce(Return(Status(absl::StatusCode::kInternal, "Internal error")));

  auto result = crypto_key_.Encrypt(kPlaintext);

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(GetBackendErrorReason(result.status()), Error::ENCRYPTION_ERROR);
}

TEST_F(AeadCryptoKeyTest, DecryptIsSuccessful) {
  EXPECT_CALL(*mock_aead_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(std::string(kPlaintext)));

  auto result = crypto_key_.Decrypt(kCiphertext);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, kPlaintext);
}

TEST_F(AeadCryptoKeyTest, DecryptWithErrorReturnsFailure) {
  EXPECT_CALL(*mock_aead_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(Status(absl::StatusCode::kInternal, "Internal error")));

  auto result = crypto_key_.Decrypt(kCiphertext);

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(GetBackendErrorReason(result.status()), Error::DECRYPTION_ERROR);
}

}  // namespace
}  // namespace google::confidential_match::match_service
