/*
 * Copyright 2026 Google LLC
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

#ifndef CC_MATCH_SERVICE_VALIDATORS_HASHED_PII_VALIDATOR_H_
#define CC_MATCH_SERVICE_VALIDATORS_HASHED_PII_VALIDATOR_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace google::confidential_match::match_service {

// Validates that a single-field match key (email or phone) is a valid hash.
// Returns an `INVALID_HASHED_VALUE` error if it is not.
absl::Status ValidateHashedSingleField(absl::string_view value);

// Validates that the first and last names of a composite address field are
// valid hashes. `address_parts` holds the reconstructed address parts in the
// order [first_name, last_name, country_code, zip_code]. Empty names are not
// validated, and country code and zip code are not expected to be hashed.
// Returns an `INVALID_MATCH_KEY_FIELD` error if the address does not have
// exactly 4 parts, or an `INVALID_HASHED_VALUE` error if a non-empty name is
// not a valid hash. The error message says whether the first name or the last
// name is invalid; the first name is checked first.
absl::Status ValidateHashedAddressField(
    absl::Span<const std::string> address_parts);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_VALIDATORS_HASHED_PII_VALIDATOR_H_
