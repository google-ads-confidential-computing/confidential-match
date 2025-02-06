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

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_MOCK_FAKE_CRYPTO_KEY_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_MOCK_FAKE_CRYPTO_KEY_H_

#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"

namespace google::confidential_match::lookup_server {

class FakeCryptoKey : public CryptoKeyInterface {
 public:
  scp::core::ExecutionResultOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override {
    return std::string(plaintext);
  }

  scp::core::ExecutionResultOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override {
    return std::string(ciphertext);
  }
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_MOCK_FAKE_CRYPTO_KEY_H_
