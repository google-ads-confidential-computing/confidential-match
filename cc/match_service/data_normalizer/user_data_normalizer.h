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

#ifndef CC_MATCH_SERVICE_DATA_NORMALIZER_USER_DATA_NORMALIZER_H_
#define CC_MATCH_SERVICE_DATA_NORMALIZER_USER_DATA_NORMALIZER_H_

#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "cc/core/hash/hasher_interface.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Normalizes and hashes decrypted plaintext PII match keys using SCP's shared
// `plaintext_pii` normalizers.
//
// Normalization is best-effort: if an SCP normalizer fails for a value, the
// unnormalized value is hashed instead. Errors from the hasher are returned
// to the caller.
//
// The normalization options are passed in per call, typically from
// `MatchRequest.match_request_options.user_data_normalizer_options`. Fields
// that are not set use their proto defaults.
//
// This class never logs PII. It holds no mutable state, so it is thread-safe
// as long as the hasher is.
class UserDataNormalizer {
 public:
  // Constructs a normalizer that uses `sha256_hasher` to compute the Base64
  // SHA-256 hash of normalized values. `sha256_hasher` is not owned and must
  // outlive this object.
  explicit UserDataNormalizer(const HasherInterface* sha256_hasher)
      : sha256_hasher_(sha256_hasher) {}

  // Normalizes a plaintext email and returns its Base64 SHA-256 hash.
  //
  // The email is trimmed and lowercased. Periods in the username of
  // gmail.com and googlemail.com addresses are always removed. The other
  // steps follow `options.email_normalizer_options()`:
  // `strip_parts_after_plus_sign` (gmail.com and googlemail.com only),
  // `strip_periods_in_facebookmail` and `replace_googlemail_with_gmail`.
  //
  // A value that does not contain exactly one '@' (including an empty value)
  // cannot be normalized, so the unnormalized value is hashed.
  absl::StatusOr<std::string> NormalizeAndHashEmail(
      absl::string_view email,
      const backend::UserDataNormalizerOptions& options) const;

  // Normalizes a plaintext phone number and returns its Base64 SHA-256 hash.
  //
  // All characters other than digits and a leading '+' are removed. A number
  // that starts with '+' is kept as is. Otherwise:
  // * If `options.phone_number_normalizer_options().use_country_code_hint()`
  //   is true and `country_code_hint` canonicalizes to a known country, the
  //   number is prefixed with that country's calling code (E.164). The hint
  //   is a country name or code (e.g. from a composite address in the same
  //   data record) and is canonicalized with SCP's NormalizeCountryCode, so
  //   "United Kingdom", "GBR" and "GB" all work.
  // * Otherwise (no hint, hint not used, or unknown country), only "+" is
  //   prepended, so a national number is not real E.164 (e.g. "6502530000"
  //   becomes "+6502530000").
  //
  // A value without digits (including an empty value) becomes "+".
  absl::StatusOr<std::string> NormalizeAndHashPhoneNumber(
      absl::string_view phone,
      std::optional<absl::string_view> country_code_hint,
      const backend::UserDataNormalizerOptions& options) const;

  // Normalizes a non-hashed composite address and hashes its names.
  // `address_parts` holds the decrypted address parts in the order
  // [first_name, last_name, country_code, zip_code], and the normalized parts
  // are returned in the same order:
  //
  // * First and last names are trimmed, normalized with
  //   `options.name_normalizer_options()` and replaced with their Base64
  //   SHA-256 hashes. Names are always hashed: an empty or whitespace-only
  //   name is hashed as the empty string, matching the Match Data Exporter
  //   and correctly hashed requests. A name that becomes empty after
  //   normalization (e.g. "123" when `remove_numbers_and_special_chars` is
  //   true) falls back to the trimmed unnormalized name.
  // * The country is canonicalized to a lowercase ISO-3166 alpha-2 code where
  //   possible (e.g. "United States" and "USA" become "us"); otherwise it is
  //   trimmed and lowercased.
  // * The zip code is normalized with SCP's NormalizeZipCode using
  //   `options.zip_code_normalizer_options()`. When
  //   `pass_country_code_for_us_zip_code_normalization` is true, the
  //   normalized country code is passed so that US zip codes are zero-padded
  //   and truncated as configured. If no characters remain and
  //   `fall_back_to_unnormalized_if_empty` is true, the unnormalized
  //   (untrimmed) zip code is used. When `only_trim_surrounding_whitespaces`
  //   is true, the zip code is only trimmed and all other zip code options,
  //   including `fall_back_to_unnormalized_if_empty`, are ignored (e.g. "   "
  //   becomes "").
  //
  // Returns an INVALID_MATCH_KEY_FIELD error if `address_parts` does not have
  // exactly 4 parts, or the hasher's error if hashing fails.
  absl::StatusOr<std::vector<std::string>> NormalizeAndHashCompositeAddress(
      const std::vector<std::string>& address_parts,
      const backend::UserDataNormalizerOptions& options) const;

 private:
  // Normalizes a plaintext name (best-effort) and returns its Base64 SHA-256
  // hash. An empty or whitespace-only name is hashed as the empty string, and
  // a name that becomes empty after normalization falls back to the trimmed
  // unnormalized name.
  absl::StatusOr<std::string> NormalizeAndHashName(
      absl::string_view name,
      const backend::UserDataNormalizerOptions& options) const;

  // The hasher used to generate Base64 SHA-256 hashes. Not owned.
  const HasherInterface* sha256_hasher_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_DATA_NORMALIZER_USER_DATA_NORMALIZER_H_
