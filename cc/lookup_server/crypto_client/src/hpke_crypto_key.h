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

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_HPKE_CRYPTO_KEY_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_HPKE_CRYPTO_KEY_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/crypto_client/src/hpke_crypto_client.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief An HPKE cryptographic key used for encryption and decryption.
 */
class HpkeCryptoKey : public CryptoKeyInterface {
 public:
  /**
   * Constructs a key used for hybrid encryption and decryption.
   *
   * @param public_key the JSON form of a Tink HpkePublicKey
   * @param private_key the base-64 encoded keyset for a Tink HpkePrivateKey
   * @param crypto_client the HPKE crypto client
   */
  explicit HpkeCryptoKey(absl::string_view public_key,
                         absl::string_view private_key,
                         std::shared_ptr<HpkeCryptoClient> crypto_client)
      : public_key_(public_key),
        private_key_(private_key),
        crypto_client_(crypto_client) {}

  /**
   * @brief Encrypts plaintext using the stored public key.
   *
   * @param plaintext the plaintext to encrypt
   * @return the encrypted ciphertext or an error if unsuccessful
   */
  scp::core::ExecutionResultOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override;

  /**
   * @brief Decrypts ciphertext using the stored private key.
   *
   * @param ciphertext the ciphertext to decrypt
   * @return the decrypted plaintext or an error if unsuccessful
   */
  scp::core::ExecutionResultOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override;

 private:
  const std::string public_key_;
  const std::string private_key_;
  std::shared_ptr<HpkeCryptoClient> crypto_client_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_HPKE_CRYPTO_KEY_H_
