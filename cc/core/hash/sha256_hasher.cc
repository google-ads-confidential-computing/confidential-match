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

#include "cc/core/hash/sha256_hasher.h"

#include <string>

#include "openssl/sha.h"

#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match {

absl::StatusOr<std::string> Sha256Hasher::Base64EncodedHash(
    absl::string_view input) const {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(),
         hash);
  absl::string_view digest(reinterpret_cast<const char*>(hash),
                           SHA256_DIGEST_LENGTH);
  return absl::Base64Escape(digest);
}

}  // namespace google::confidential_match
