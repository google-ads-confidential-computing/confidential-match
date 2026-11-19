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

#include "cc/match_service/data_format_detectors/user_data_detector.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/match_service/error/error.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/combined_address_detector.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/detector.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/email_detector.h"
#include "cc/public/cpio/utils/data_normalization/format_detection/phone_number_detector.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::cpio::CombinedAddressDetector;
using ::google::scp::cpio::EmailDetector;
using ::google::scp::cpio::PhoneNumberDetector;
using ::google::scp::cpio::UserDataFormat;

// Indices of the name parts within a reconstructed address.
constexpr size_t kAddressFirstNameIndex = 0;
constexpr size_t kAddressLastNameIndex = 1;

}  // namespace

absl::StatusOr<UserDataFormat> UserDataDetector::DetectSingleField(
    backend::FieldType field_type, absl::string_view field_value) const {
  switch (field_type) {
    case backend::FIELD_TYPE_EMAIL:
      return EmailDetector().Detect(std::string(field_value));
    case backend::FIELD_TYPE_PHONE:
      return PhoneNumberDetector().Detect(std::string(field_value));
    default:
      return Status(Error::INVALID_MATCH_KEY_FIELD,
                    "The only supported types for fields are email and phone.");
  }
}

absl::StatusOr<UserDataFormat> UserDataDetector::DetectCompositeAddress(
    absl::Span<const std::string> address_parts) const {
  // Only the first and last names are read, so the country code and zip code
  // parts are not required.
  if (address_parts.size() < kAddressLastNameIndex + 1) {
    return Status(Error::INVALID_MATCH_KEY_FIELD,
                  "Address must have at least a first and last name.");
  }
  return CombinedAddressDetector().Detect(
      address_parts[kAddressFirstNameIndex],
      address_parts[kAddressLastNameIndex]);
}

}  // namespace google::confidential_match::match_service
