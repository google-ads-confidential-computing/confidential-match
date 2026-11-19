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

#include "cc/match_service/data_normalizer/user_data_normalizer.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/core/error/status_macros.h"
#include "cc/match_service/error/error.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/utils/data_normalization/plaintext_pii/pii_normalizer.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::
    EmailNormalizerOptions;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::
    NameNormalizerOptions;
using ::google::confidential_match::match_service::backend::
    UserDataNormalizerOptions;
using ::google::confidential_match::match_service::backend::
    ZipCodeNormalizerOptions;
using ::google::scp::core::ExecutionResultOr;

// Indices of the parts within a reconstructed address.
constexpr size_t kAddressFirstNameIndex = 0;
constexpr size_t kAddressLastNameIndex = 1;
constexpr size_t kAddressCountryCodeIndex = 2;
constexpr size_t kAddressZipCodeIndex = 3;
constexpr size_t kNumAddressParts = 4;

// Canonicalizes a country name or code to a lowercase ISO-3166 alpha-2 code
// where possible. Unknown countries are returned trimmed and lowercased.
std::string NormalizeCountryCode(absl::string_view country) {
  return scp::cpio::NormalizeCountryCode(std::string(country));
}

// Maps the email normalizer options to SCP's email normalization options.
scp::cpio::NormalizeEmailOptions ToScpNormalizeEmailOptions(
    const EmailNormalizerOptions& options) {
  scp::cpio::NormalizeEmailOptions scp_options;
  scp_options.should_strip_parts_after_plus_sign =
      options.strip_parts_after_plus_sign();
  scp_options.should_strip_periods_in_facebookmail =
      options.strip_periods_in_facebookmail();
  scp_options.should_replace_googlemail_with_gmail =
      options.replace_googlemail_with_gmail();
  return scp_options;
}

// Maps the zip code normalizer options to SCP's zip code normalization
// options. `country_code` is the canonicalized country of the same address;
// it is passed to SCP only when
// `pass_country_code_for_us_zip_code_normalization` is true and it is
// non-empty, so that US-specific options apply to US zip codes.
scp::cpio::NormalizeZipCodeOptions ToScpNormalizeZipCodeOptions(
    const ZipCodeNormalizerOptions& options, absl::string_view country_code) {
  scp::cpio::NormalizeZipCodeOptions scp_options;
  scp_options.should_only_trim_surrounding_whitespaces =
      options.only_trim_surrounding_whitespaces();
  scp_options.to_lowercase = options.to_lowercase();
  if (options.pass_country_code_for_us_zip_code_normalization() &&
      !country_code.empty()) {
    scp_options.country_code = std::string(country_code);
  }
  scp_options.zero_pad_us_zip_code = options.zero_pad_us_zip_code();
  scp_options.truncate_us_zip_code = options.truncate_us_zip_code();
  scp_options.fail_on_empty = options.fall_back_to_unnormalized_if_empty();
  return scp_options;
}

// Normalizes the zip code of an address. `country_code` is the canonicalized
// country of the same address.
std::string NormalizeZipCode(absl::string_view zip_code,
                             absl::string_view country_code,
                             const ZipCodeNormalizerOptions& options) {
  std::string zip_code_str(zip_code);
  // Normalization is best-effort if fall_back_to_unnormalized_if_empty is
  // true: SCP function fails if the normalized output becomes empty. The
  // unnormalized zip code is then used. When only_trim_surrounding_whitespaces
  // is true, SCP only trims and ignores all other options, including
  // fall_back_to_unnormalized_if_empty, so a whitespace-only zip code becomes
  // empty.
  std::string normalized =
      scp::cpio::NormalizeZipCode(
          zip_code_str, ToScpNormalizeZipCodeOptions(options, country_code))
          .value_or(zip_code_str);
  return normalized;
}

}  // namespace

absl::StatusOr<std::string> UserDataNormalizer::NormalizeAndHashEmail(
    absl::string_view email, const UserDataNormalizerOptions& options) const {
  // SCP fails if the email does not contain exactly one '@'. Normalization is
  // best-effort, so the unnormalized email is hashed in that case.
  std::string normalized =
      scp::cpio::NormalizeEmail(
          std::string(email),
          ToScpNormalizeEmailOptions(options.email_normalizer_options()))
          .value_or(std::string(email));
  return sha256_hasher_->Base64EncodedHash(normalized);
}

absl::StatusOr<std::string> UserDataNormalizer::NormalizeAndHashPhoneNumber(
    absl::string_view phone, std::optional<absl::string_view> country_code_hint,
    const UserDataNormalizerOptions& options) const {
  std::string phone_str(phone);
  std::optional<std::string> canonical_hint;
  if (options.phone_number_normalizer_options().use_country_code_hint() &&
      country_code_hint.has_value()) {
    std::string hint = NormalizeCountryCode(*country_code_hint);
    if (!hint.empty()) {
      canonical_hint = std::move(hint);
    }
  }
  // SCP only fails when the hint is not a known country. The hint comes from
  // user data, so retry without it; SCP never fails without a hint. If
  // normalization still fails, the unnormalized phone number is hashed
  // (best-effort).
  ExecutionResultOr<std::string> normalized =
      scp::cpio::NormalizePhoneNumber(phone_str, canonical_hint);
  if (!normalized.has_value() && canonical_hint.has_value()) {
    normalized = scp::cpio::NormalizePhoneNumber(phone_str, std::nullopt);
  }
  return sha256_hasher_->Base64EncodedHash(
      std::move(normalized).value_or(std::string(phone)));
}

absl::StatusOr<std::vector<std::string>>
UserDataNormalizer::NormalizeAndHashCompositeAddress(
    const std::vector<std::string>& address_parts,
    const UserDataNormalizerOptions& options) const {
  if (address_parts.size() != kNumAddressParts) {
    return Status(Error::INVALID_MATCH_KEY_FIELD,
                  absl::StrCat("Address must have exactly ", kNumAddressParts,
                               " parts."));
  }
  std::vector<std::string> normalized(kNumAddressParts);
  ASSIGN_OR_RETURN(
      normalized[kAddressFirstNameIndex],
      NormalizeAndHashName(address_parts[kAddressFirstNameIndex], options));
  ASSIGN_OR_RETURN(
      normalized[kAddressLastNameIndex],
      NormalizeAndHashName(address_parts[kAddressLastNameIndex], options));
  normalized[kAddressCountryCodeIndex] =
      NormalizeCountryCode(address_parts[kAddressCountryCodeIndex]);
  normalized[kAddressZipCodeIndex] = NormalizeZipCode(
      address_parts[kAddressZipCodeIndex], normalized[kAddressCountryCodeIndex],
      options.zip_code_normalizer_options());
  return normalized;
}

absl::StatusOr<std::string> UserDataNormalizer::NormalizeAndHashName(
    absl::string_view name, const UserDataNormalizerOptions& options) const {
  // TODO(b/574624182): Update SCP name normalization function to strip
  // whitespace before title stripping as well.
  std::string trimmed(absl::StripAsciiWhitespace(name));
  std::string normalized;
  if (!trimmed.empty()) {  // Only normalize non-empty names.
    const NameNormalizerOptions& name_options =
        options.name_normalizer_options();
    // Normalization is best-effort. If it fails, use the trimmed name.
    normalized = scp::cpio::NormalizeName(
                     trimmed, name_options.strip_common_prefixes(),
                     name_options.remove_numbers_and_special_chars())
                     .value_or(trimmed);
    if (normalized.empty()) {
      normalized = trimmed;
    }
  }
  return sha256_hasher_->Base64EncodedHash(normalized);
}

}  // namespace google::confidential_match::match_service
