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

#ifndef CC_MATCH_SERVICE_TASKS_NORMALIZATION_UTILS_H_
#define CC_MATCH_SERVICE_TASKS_NORMALIZATION_UTILS_H_

#include <string>

#include "absl/strings/string_view.h"

namespace google::confidential_match::match_service {

// Normalizes a country code by trimming whitespace and converting to lowercase.
// Aligns with Java MRP ToLowercaseTransformation.
std::string NormalizeCountryCode(absl::string_view country_code);

// Normalizes a zip code based on the country code.
// Aligns with Java MRP CountryBasedZipTransformation.
// 1. Removes all non-alphanumeric characters.
// 2. Converts to lowercase.
// 3. For US zip codes: pads with leading zeros to 5 or 9 digits,
//    then truncates to the first 5 digits.
std::string NormalizeZipCode(absl::string_view zip_code,
                             absl::string_view country_code);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_TASKS_NORMALIZATION_UTILS_H_
