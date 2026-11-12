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

#include "cc/match_service/validators/hashed_pii_validator.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/match_service/error/error.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/detector_util.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::cpio::IsValidHash;

// Indices of the name parts within a reconstructed address.
constexpr size_t kAddressFirstNameIndex = 0;
constexpr size_t kAddressLastNameIndex = 1;
constexpr size_t kAddressPartsSize = 4;

}  // namespace

absl::Status ValidateHashedSingleField(absl::string_view value) {
  if (!IsValidHash(std::string(value))) {
    return Status(Error::INVALID_HASHED_VALUE,
                  "Match key is not a valid hash.");
  }
  return absl::OkStatus();
}

absl::Status ValidateHashedAddressField(
    absl::Span<const std::string> address_parts) {
  if (address_parts.size() != kAddressPartsSize) {
    return Status(Error::INVALID_MATCH_KEY_FIELD,
                  "Address must have exactly 4 parts.");
  }
  const std::string& first_name = address_parts[kAddressFirstNameIndex];
  if (!first_name.empty() && !IsValidHash(first_name)) {
    return Status(Error::INVALID_HASHED_VALUE,
                  "Address first name is not a valid hash.");
  }
  const std::string& last_name = address_parts[kAddressLastNameIndex];
  if (!last_name.empty() && !IsValidHash(last_name)) {
    return Status(Error::INVALID_HASHED_VALUE,
                  "Address last name is not a valid hash.");
  }
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
