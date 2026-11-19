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

#ifndef CC_MATCH_SERVICE_DATA_FORMAT_DETECTORS_USER_DATA_DETECTOR_H_
#define CC_MATCH_SERVICE_DATA_FORMAT_DETECTORS_USER_DATA_DETECTOR_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/detector.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Detects the format (hashed, plaintext, or unknown) of decrypted PII match
// keys using the shared SCP format detectors.
//
// The detector only classifies the format of a value; it does not validate
// that a value is hashed. Use the validators in
// cc/match_service/validators/hashed_pii_validator.h for that.
class UserDataDetector {
 public:
  // Detects the format of a single-field match key (email or phone). Returns
  // an `INVALID_MATCH_KEY_FIELD` error for other field types.
  absl::StatusOr<scp::cpio::UserDataFormat> DetectSingleField(
      backend::FieldType field_type, absl::string_view field_value) const;

  // Detects the format of a composite address match key from its first and
  // last names. `address_parts` holds the reconstructed address parts in the
  // order [first_name, last_name, country_code, zip_code]; only the first and
  // last names are read. Returns an `INVALID_MATCH_KEY_FIELD` error if the
  // address does not have at least a first and last name.
  absl::StatusOr<scp::cpio::UserDataFormat> DetectCompositeAddress(
      absl::Span<const std::string> address_parts) const;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_DATA_FORMAT_DETECTORS_USER_DATA_DETECTOR_H_
