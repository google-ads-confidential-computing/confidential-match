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

#ifndef CC_LOOKUP_SERVER_INTERFACE_CRYPTO_KEY_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_CRYPTO_KEY_INTERFACE_H_

#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Interface wrapping a cryptographic key to allow for encryption and
 * decryption operations.
 */
class CryptoKeyInterface {
 public:
  virtual ~CryptoKeyInterface() = default;

  /**
   * @brief Encrypts plaintext using the stored key.
   *
   * @param plaintext the plaintext to encrypt
   * @return the encrypted ciphertext or an error if unsuccessful
   */
  virtual scp::core::ExecutionResultOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept = 0;

  /**
   * @brief Decrypts ciphertext using the stored key.
   *
   * @param ciphertext the ciphertext to decrypt
   * @return the decrypted plaintext or an error if unsuccessful
   */
  virtual scp::core::ExecutionResultOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_CRYPTO_KEY_INTERFACE_H_
