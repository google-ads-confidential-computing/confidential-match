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

#ifndef CC_CORE_HASH_SHA256_HASHER_H_
#define CC_CORE_HASH_SHA256_HASHER_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

#include "cc/core/hash/hasher_interface.h"

namespace google::confidential_match {
// A class that can hash a string using SHA-256.
class Sha256Hasher : public HasherInterface {
 public:
  // Constructs a SHA-256 hasher.
  Sha256Hasher() {}

  // Computes the SHA-256 hash of the given input string and returns the
  // result in base64-encoded form.
  absl::StatusOr<std::string> Base64EncodedHash(
      absl::string_view input) const override;
};
}  // namespace google::confidential_match

#endif  // CC_CORE_HASH_SHA256_HASHER_H_
