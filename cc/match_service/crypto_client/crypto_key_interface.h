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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_CRYPTO_KEY_INTERFACE_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_CRYPTO_KEY_INTERFACE_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match::match_service {

// Interface wrapping a cryptographic key to allow for encryption and
//  decryption operations.
class CryptoKeyInterface {
 public:
  virtual ~CryptoKeyInterface() = default;

  // Encrypts plaintext using the stored key.
  virtual absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept = 0;

  // Decrypts ciphertext using the stored key.
  virtual absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept = 0;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_CRYPTO_KEY_INTERFACE_H_
