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

#include "cc/match_service/crypto_client/tink_utils.h"

#include <string>

#include "absl/status/status_matchers.h"
#include "absl/strings/escaping.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "tink/aead.h"
#include "tink/aead/aead_config.h"
#include "tink/hybrid/hpke_config.h"
#include "tink/hybrid/hybrid_config.h"
#include "tink/hybrid_decrypt.h"
#include "tink/hybrid_encrypt.h"

#include "cc/match_service/error/error.h"

namespace google::confidential_match::match_service {

namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::crypto::tink::Aead;
using ::crypto::tink::AeadConfig;
using ::crypto::tink::HybridConfig;
using ::crypto::tink::HybridDecrypt;
using ::crypto::tink::HybridEncrypt;
using ::crypto::tink::RegisterHpke;
using ::testing::ContainsRegex;
using ::testing::Test;

constexpr absl::string_view kTestDek =
    "CP24wPEEEmcKWwozdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2"
    "NtU2l2S2V5EiIaIObVHDAKMgQg0ww0qfsLoKzu0ui90WbHAqiJf8qayOqsGAEQARj9uMDxBCA"
    "B";
constexpr absl::string_view kTestPublicHpkeP256 =
    "CHsSjgEKhQEKNHR5cGUuZ29vZ2xlYXBpcy5jb20vZ29vZ2xlLmNyeXB0by50aW5rLkhwa2VQdW"
    "JsaWNLZXkSSxIGCAIQARgBGkEE/owZzgkFGR68KYqSRXklMfJvDOziRgY56Lw5y39waoJqd5tM"
    "+Wm4oOU5x/Yvs9MK1qqPgOMPHRKKr9aKLOcuoBgDEAEYeyAD";
constexpr absl::string_view kTestPrivateHpkeP256 =
    "CHsSswEKqgEKNXR5cGUuZ29vZ2xlYXBpcy5jb20vZ29vZ2xlLmNyeXB0by50aW5rLkhwa2VQcm"
    "l2YXRlS2V5Em8SSxIGCAIQARgBGkEE/owZzgkFGR68KYqSRXklMfJvDOziRgY56Lw5y39waoJq"
    "d5tM+Wm4oOU5x/Yvs9MK1qqPgOMPHRKKr9aKLOcuoBog885/2uV+GjENh/HrvebzKL4Kmc28rf"
    "TWWJzyneS4/9IYAhABGHsgAw==";

class TinkUtilsTest : public Test {
 protected:
  TinkUtilsTest() {
    EXPECT_THAT(AeadConfig::Register(), IsOk());
    EXPECT_THAT(HybridConfig::Register(), IsOk());
    // Need to also explicitly register HPKE
    EXPECT_THAT(RegisterHpke(), IsOk());
  }
};

TEST_F(TinkUtilsTest, CreatesAead) {
  std::string decoded_dek;
  ASSERT_TRUE(absl::Base64Unescape(kTestDek, &decoded_dek));
  auto key_or = GetTinkPrimitive<Aead>(decoded_dek);
  ASSERT_THAT(key_or, IsOk());

  auto payload = "some payload";
  const auto& key = **key_or;
  auto ciphertext_or = key.Encrypt(payload, "");
  ASSERT_THAT(ciphertext_or, IsOk());
  auto decrypted_payload_or = key.Decrypt(*ciphertext_or, "");
  ASSERT_THAT(decrypted_payload_or, IsOkAndHolds(payload));
}

TEST_F(TinkUtilsTest, CreatesHpke) {
  // Get an encrypt key
  std::string decoded_key;
  ASSERT_TRUE(absl::Base64Unescape(kTestPublicHpkeP256, &decoded_key));
  auto encrypt_or = GetTinkPrimitive<HybridEncrypt>(decoded_key);
  ASSERT_THAT(encrypt_or, IsOk());

  // Encrypt some data
  const auto& encrypt = **encrypt_or;
  auto payload = "some payload";
  auto ciphertext_or = encrypt.Encrypt(payload, "");
  ASSERT_THAT(ciphertext_or, IsOk());

  // Get a decrypt key
  ASSERT_TRUE(absl::Base64Unescape(kTestPrivateHpkeP256, &decoded_key));
  auto decrypt_or = GetTinkPrimitive<HybridDecrypt>(decoded_key);
  ASSERT_THAT(decrypt_or, IsOk()) << decrypt_or.status().ToString();
  // Assert it decrypts correctly
  const auto& decrypt = **decrypt_or;
  ASSERT_THAT(decrypt.Decrypt(*ciphertext_or, ""), IsOkAndHolds(payload));
}

TEST_F(TinkUtilsTest, FailsIfBadKeyMaterial) {
  EXPECT_THAT(GetTinkPrimitive<Aead>("bad material"),
              StatusIs(absl::StatusCode::kInternal,
                       ContainsRegex("Failed to read keyset")));
}

TEST_F(TinkUtilsTest, FailsIfWrongKeyType) {
  std::string decoded_key;
  ASSERT_TRUE(absl::Base64Unescape(kTestPublicHpkeP256, &decoded_key));
  EXPECT_THAT(GetTinkPrimitive<Aead>(decoded_key),
              StatusIs(absl::StatusCode::kInternal,
                       ContainsRegex("Failed to get .*Aead")));
}

}  // namespace
}  // namespace google::confidential_match::match_service
