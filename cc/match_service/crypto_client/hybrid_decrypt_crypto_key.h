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

#ifndef CONFIDENTIAL_MATCH_HYBRID_DECRYPT_CRYPTO_KEY_H
#define CONFIDENTIAL_MATCH_HYBRID_DECRYPT_CRYPTO_KEY_H

#include "cc/match_service/crypto_client/crypto_key_interface.h"

#include <memory>
#include <string>

#include "tink/hybrid_decrypt.h"

namespace google::confidential_match::match_service {

// A cryptographic key used for decryption.
// This internally wraps a Tink HybridDecrypt primitive.
class HybridDecryptCryptoKey : public CryptoKeyInterface {
 public:
  explicit HybridDecryptCryptoKey(
      std::shared_ptr<::crypto::tink::HybridDecrypt> hybrid_decrypt);

  // Unimplemented for this class. This class will not support encryption but
  // this method is here due to the interface it inherits.
  absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const noexcept override;

  absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const noexcept override;

 private:
  std::shared_ptr<::crypto::tink::HybridDecrypt> hybrid_decrypt_;
};

}  // namespace google::confidential_match::match_service

#endif  // CONFIDENTIAL_MATCH_HYBRID_DECRYPT_CRYPTO_KEY_H
