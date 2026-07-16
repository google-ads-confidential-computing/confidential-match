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

#include "cc/match_service/tasks/normalization_utils.h"

#include <algorithm>
#include <string>

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match::match_service {

namespace {

constexpr absl::string_view kUsCountryCode = "us";

std::string RemoveNonAlphanumeric(absl::string_view s) {
  std::string result;
  result.reserve(s.length());
  for (char c : s) {
    if (absl::ascii_isalnum(c)) {
      result.push_back(c);
    }
  }
  return result;
}

}  // namespace

std::string NormalizeCountryCode(absl::string_view country_code) {
  return absl::AsciiStrToLower(absl::StripAsciiWhitespace(country_code));
}

std::string NormalizeZipCode(absl::string_view zip_code,
                             absl::string_view country_code) {
  std::string alphanumeric = RemoveNonAlphanumeric(zip_code);
  if (alphanumeric.empty()) {
    return std::string(zip_code);
  }

  std::string lowercase = absl::AsciiStrToLower(alphanumeric);
  std::string normalized_country = NormalizeCountryCode(country_code);

  if (normalized_country == kUsCountryCode) {
    if (lowercase.length() <= 5) {
      if (lowercase.length() < 5) {
        return absl::StrCat(std::string(5 - lowercase.length(), '0'),
                            lowercase);
      }
      return lowercase;
    } else {
      std::string padded;
      if (lowercase.length() < 9) {
        padded =
            absl::StrCat(std::string(9 - lowercase.length(), '0'), lowercase);
      } else {
        padded = lowercase;
      }
      return padded.substr(0, 5);
    }
  }

  return lowercase;
}

}  // namespace google::confidential_match::match_service
