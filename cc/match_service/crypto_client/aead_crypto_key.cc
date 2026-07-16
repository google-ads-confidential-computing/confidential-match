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

#include "cc/match_service/crypto_client/aead_crypto_key.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "tink/aead.h"
#include "tink/util/statusor.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;
template <typename T>
using TinkStatusOr = ::crypto::tink::util::StatusOr<T>;

constexpr absl::string_view kAssociatedData = "";

absl::StatusOr<std::string> AeadCryptoKey::Encrypt(
    absl::string_view plaintext) const noexcept {
  TinkStatusOr<std::string> ciphertext_or =
      aead_->Encrypt(plaintext, kAssociatedData);
  if (!ciphertext_or.ok()) {
    return Status(Error::ENCRYPTION_ERROR,
                  "Tink AEAD failed to encrypt plaintext.");
  }
  return *ciphertext_or;
}

absl::StatusOr<std::string> AeadCryptoKey::Decrypt(
    absl::string_view ciphertext) const noexcept {
  TinkStatusOr<std::string> plaintext_or =
      aead_->Decrypt(ciphertext, kAssociatedData);
  if (!plaintext_or.ok()) {
    return Status(Error::DECRYPTION_ERROR,
                  "Tink AEAD failed to decrypt ciphertext.");
  }
  return *plaintext_or;
}

}  // namespace google::confidential_match::match_service
