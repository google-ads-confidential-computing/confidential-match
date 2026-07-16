/*
 * Copyright 2026 Google LLC
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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_AEAD_CRYPTO_KEY_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_AEAD_CRYPTO_KEY_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "tink/aead.h"

#include "cc/match_service/crypto_client/crypto_key_interface.h"

namespace google::confidential_match::match_service {

// A cryptographic key used for encryption and decryption.
// This internally wraps a Tink AEAD (Authenticated Encryption with Associated
// Data) primitive with empty associated data.
class AeadCryptoKey : public CryptoKeyInterface {
 public:
  explicit AeadCryptoKey(std::shared_ptr<::crypto::tink::Aead> aead)
      : aead_(aead) {}

  // Encrypts plaintext using the stored key.
  absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override;

  // Decrypts ciphertext using the stored key.
  absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override;

 private:
  std::shared_ptr<::crypto::tink::Aead> aead_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_AEAD_CRYPTO_KEY_H_
