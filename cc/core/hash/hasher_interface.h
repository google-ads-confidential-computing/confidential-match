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

#ifndef CC_CORE_HASH_HASHER_INTERFACE_H_
#define CC_CORE_HASH_HASHER_INTERFACE_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match {

// Interface for a class that performs hashing operations.
class HasherInterface {
 public:
  virtual ~HasherInterface() = default;

  // Computes the hash of the given input string and returns the result in
  // base64-encoded form.
  virtual absl::StatusOr<std::string> Base64EncodedHash(
      absl::string_view input) const = 0;
};
}  // namespace google::confidential_match

#endif  // CC_CORE_HASH_HASHER_INTERFACE_H_
