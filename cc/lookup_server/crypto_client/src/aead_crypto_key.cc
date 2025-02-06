// Copyright 2025 Google LLC
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

#include "cc/lookup_server/crypto_client/src/aead_crypto_key.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "tink/aead.h"
#include "tink/aead/aead_config.h"
#include "tink/binary_keyset_reader.h"
#include "tink/cleartext_keyset_handle.h"
#include "tink/keyset_handle.h"
#include "tink/util/statusor.h"

#include "cc/lookup_server/crypto_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::crypto::tink::Aead;
using ::crypto::tink::AeadConfig;
using ::crypto::tink::BinaryKeysetReader;
using ::crypto::tink::CleartextKeysetHandle;
using ::crypto::tink::KeysetHandle;
using ::crypto::tink::KeysetReader;
using ::crypto::tink::util::StatusOr;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "AeadCryptoKey";
constexpr absl::string_view kAssociatedData = "";

}  // namespace

ExecutionResultOr<std::string> AeadCryptoKey::Encrypt(
    absl::string_view plaintext) const noexcept {
  StatusOr<std::string> ciphertext_or =
      aead_->Encrypt(plaintext, kAssociatedData);
  if (!ciphertext_or.ok()) {
    auto result = FailureExecutionResult(CRYPTO_CLIENT_ENCRYPT_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrCat("Tink AEAD failed to encrypt plaintext: ",
                           ciphertext_or.status().ToString()));
    return result;
  }
  return *ciphertext_or;
}

ExecutionResultOr<std::string> AeadCryptoKey::Decrypt(
    absl::string_view ciphertext) const noexcept {
  StatusOr<std::string> plaintext_or =
      aead_->Decrypt(ciphertext, kAssociatedData);
  if (!plaintext_or.ok()) {
    auto result = FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrCat("Tink AEAD failed to decrypt ciphertext: ",
                           plaintext_or.status().ToString()));
    return result;
  }
  return *plaintext_or;
}

}  // namespace google::confidential_match::lookup_server
