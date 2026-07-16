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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_TINK_UTILS_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_TINK_UTILS_H_

#include "absl/status/statusor.h"

namespace google::confidential_match::match_service {

// Acquires a Tink key given the (unencoded) key material.
// Valid TinkPrimitive's: Aead, HybridEncrypt, HybridDecrypt.
template <typename TinkPrimitive>
absl::StatusOr<std::unique_ptr<TinkPrimitive>> GetTinkPrimitive(
    absl::string_view key_material);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_TINK_UTILS_H_
