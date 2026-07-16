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

#include "cc/match_service/crypto_client/hybrid_decrypt_crypto_key.h"

#include <memory>
#include <string>
#include <utility>

#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::crypto::tink::HybridDecrypt;
using ::google::confidential_match::match_service::backend::Error;

HybridDecryptCryptoKey::HybridDecryptCryptoKey(
    std::shared_ptr<HybridDecrypt> hybrid_decrypt)
    : hybrid_decrypt_(std::move(hybrid_decrypt)) {}

// Unimplemented for this class
absl::StatusOr<std::string> HybridDecryptCryptoKey::Encrypt(
    absl::string_view plaintext) const noexcept {
  return absl::UnimplementedError("Cannot encrypt with a HybridDecrypt");
}

absl::StatusOr<std::string> HybridDecryptCryptoKey::Decrypt(
    absl::string_view ciphertext) const noexcept {
  auto plaintext_or = hybrid_decrypt_->Decrypt(ciphertext, /*context_info=*/"");
  if (!plaintext_or.ok()) {
    return Status(
        Error::DECRYPTION_ERROR,
        absl::StrCat("Tink HybridDecrypt failed to decrypt plaintext: ",
                     plaintext_or.status().message()));
  }
  return *plaintext_or;
}

}  // namespace google::confidential_match::match_service
