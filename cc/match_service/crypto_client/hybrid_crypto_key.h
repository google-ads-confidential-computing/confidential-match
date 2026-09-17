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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_HYBRID_CRYPTO_KEY_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_HYBRID_CRYPTO_KEY_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "tink/hybrid_decrypt.h"
#include "tink/hybrid_encrypt.h"

namespace google::confidential_match::match_service {

// A cryptographic key used for hybrid encryption and decryption.
// This internally wraps Tink HybridDecrypt and HybridEncrypt primitives.
class HybridCryptoKey : public CryptoKeyInterface {
 public:
  // At least one of hybrid_decrypt or hybrid_encrypt should be provided for
  // meaningful functionality.
  explicit HybridCryptoKey(
      std::shared_ptr<::crypto::tink::HybridDecrypt> hybrid_decrypt = nullptr,
      std::shared_ptr<::crypto::tink::HybridEncrypt> hybrid_encrypt = nullptr);

  absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override;

  absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override;

 private:
  std::shared_ptr<::crypto::tink::HybridDecrypt> hybrid_decrypt_;
  std::shared_ptr<::crypto::tink::HybridEncrypt> hybrid_encrypt_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_HYBRID_CRYPTO_KEY_H_
