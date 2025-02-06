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

#include "cc/lookup_server/crypto_client/src/aead_crypto_key.h"

#include <memory>

#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "tink/aead/mock_aead.h"
#include "tink/util/status.h"

#include "cc/lookup_server/crypto_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::absl::StatusCode;
using ::crypto::tink::MockAead;
using ::crypto::tink::util::Status;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::testing::_;
using ::testing::Eq;
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

  ExecutionResultOr<std::string> result = crypto_key_.Encrypt(kPlaintext);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(*result, kCiphertext);
}

TEST_F(AeadCryptoKeyTest, EncryptWithErrorReturnsFailure) {
  EXPECT_CALL(*mock_aead_, Encrypt(kPlaintext, ""))
      .WillOnce(Return(Status(absl::StatusCode::kInternal, "Internal error")));

  ExecutionResultOr<std::string> result = crypto_key_.Encrypt(kPlaintext);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(CRYPTO_CLIENT_ENCRYPT_ERROR)));
}

TEST_F(AeadCryptoKeyTest, DecryptIsSuccessful) {
  EXPECT_CALL(*mock_aead_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(std::string(kPlaintext)));

  ExecutionResultOr<std::string> result = crypto_key_.Decrypt(kCiphertext);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(*result, kPlaintext);
}

TEST_F(AeadCryptoKeyTest, DecryptWithErrorReturnsFailure) {
  EXPECT_CALL(*mock_aead_, Decrypt(kCiphertext, ""))
      .WillOnce(Return(Status(absl::StatusCode::kInternal, "Internal error")));

  ExecutionResultOr<std::string> result = crypto_key_.Decrypt(kCiphertext);

  EXPECT_THAT(result,
              ResultIs(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
