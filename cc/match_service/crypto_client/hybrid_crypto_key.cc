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

#include "cc/match_service/crypto_client/hybrid_crypto_key.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"
#include "tink/hybrid_decrypt.h"
#include "tink/hybrid_encrypt.h"

namespace google::confidential_match::match_service {

using ::crypto::tink::HybridDecrypt;
using ::crypto::tink::HybridEncrypt;
using ::google::confidential_match::match_service::backend::Error;

HybridCryptoKey::HybridCryptoKey(std::shared_ptr<HybridDecrypt> hybrid_decrypt,
                                 std::shared_ptr<HybridEncrypt> hybrid_encrypt)
    : hybrid_decrypt_(std::move(hybrid_decrypt)),
      hybrid_encrypt_(std::move(hybrid_encrypt)) {}

absl::StatusOr<std::string> HybridCryptoKey::Encrypt(
    absl::string_view plaintext) const noexcept {
  if (hybrid_encrypt_ == nullptr) {
    return absl::FailedPreconditionError("HybridEncrypt primitive is not set.");
  }
  auto ciphertext_or = hybrid_encrypt_->Encrypt(plaintext, /*context_info=*/"");
  if (!ciphertext_or.ok()) {
    return Status(
        Error::ENCRYPTION_ERROR,
        absl::StrCat("Tink HybridEncrypt failed to encrypt plaintext: ",
                     ciphertext_or.status().message()));
  }
  return *ciphertext_or;
}

absl::StatusOr<std::string> HybridCryptoKey::Decrypt(
    absl::string_view ciphertext) const noexcept {
  if (hybrid_decrypt_ == nullptr) {
    return absl::FailedPreconditionError("HybridDecrypt primitive is not set.");
  }
  auto plaintext_or = hybrid_decrypt_->Decrypt(ciphertext, /*context_info=*/"");
  if (!plaintext_or.ok()) {
    return Status(
        Error::DECRYPTION_ERROR,
        absl::StrCat("Tink HybridDecrypt failed to decrypt ciphertext: ",
                     plaintext_or.status().message()));
  }
  return *plaintext_or;
}

}  // namespace google::confidential_match::match_service
