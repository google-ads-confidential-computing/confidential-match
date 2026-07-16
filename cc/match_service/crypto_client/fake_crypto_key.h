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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_FAKE_CRYPTO_KEY_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_FAKE_CRYPTO_KEY_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "cc/match_service/crypto_client/crypto_key_interface.h"

namespace google::confidential_match::match_service {

// A fake key that performs identity transformations (returns input as-is).
class FakeCryptoKey : public CryptoKeyInterface {
 public:
  absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override {
    return std::string(plaintext);
  }

  absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override {
    return std::string(ciphertext);
  }
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_FAKE_CRYPTO_KEY_H_
