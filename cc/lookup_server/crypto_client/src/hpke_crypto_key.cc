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

#include "cc/lookup_server/crypto_client/src/hpke_crypto_key.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/crypto_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "HpkeCryptoKey";

}  // namespace

ExecutionResultOr<std::string> HpkeCryptoKey::Encrypt(
    absl::string_view plaintext) const noexcept {
  return crypto_client_->Encrypt(plaintext, public_key_);
}

ExecutionResultOr<std::string> HpkeCryptoKey::Decrypt(
    absl::string_view ciphertext) const noexcept {
  return crypto_client_->Decrypt(ciphertext, private_key_);
}

}  // namespace google::confidential_match::lookup_server
