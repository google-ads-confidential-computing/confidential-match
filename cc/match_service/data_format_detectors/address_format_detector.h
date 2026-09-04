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

#ifndef CC_MATCH_SERVICE_DATA_FORMAT_DETECTORS_ADDRESS_FORMAT_DETECTOR_H_
#define CC_MATCH_SERVICE_DATA_FORMAT_DETECTORS_ADDRESS_FORMAT_DETECTOR_H_

#include <cstddef>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Returns the minimum length of an encrypted PII value based on the key type
// in EncryptionKey (WrappedKey for AEAD vs CoordinatorKey for Hybrid).
size_t GetMinimumEncryptedPiiLength(
    backend::MatchKeyEncoding encoding,
    const backend::EncryptionKey& encryption_key);

// Checks if the country code and zip code in the given composite field are
// encrypted. Returns INVALID_MATCH_KEY_FIELD error if the composite field is
// not an address composite field, or if either country code or zip code is
// missing.
absl::StatusOr<bool> AreCountryCodeZipCodeEncrypted(
    const backend::CompositeField& composite_field,
    backend::MatchKeyEncoding encoding,
    const backend::EncryptionKey& encryption_key);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_DATA_FORMAT_DETECTORS_ADDRESS_FORMAT_DETECTOR_H_
