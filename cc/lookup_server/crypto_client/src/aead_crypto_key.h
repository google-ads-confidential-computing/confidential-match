/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_AEAD_CRYPTO_KEY_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_AEAD_CRYPTO_KEY_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "tink/aead.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A cryptographic key used for encryption and decryption.
 *
 * This internally wraps a Tink AEAD with empty associated data.
 */
class AeadCryptoKey : public CryptoKeyInterface {
 public:
  explicit AeadCryptoKey(std::shared_ptr<::crypto::tink::Aead> aead)
      : aead_(aead) {}

  /**
   * @brief Encrypts plaintext using the stored key.
   *
   * @param plaintext the plaintext to encrypt
   * @return the encrypted ciphertext or an error if unsuccessful
   */
  scp::core::ExecutionResultOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override;

  /**
   * @brief Decrypts ciphertext using the stored key.
   *
   * @param ciphertext the ciphertext to decrypt
   * @return the decrypted plaintext or an error if unsuccessful
   */
  scp::core::ExecutionResultOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override;

 private:
  std::shared_ptr<::crypto::tink::Aead> aead_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_AEAD_CRYPTO_KEY_H_
