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

#include "cc/match_service/data_format_detectors/address_format_detector.h"

#include <cstddef>
#include <string>

#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "cc/match_service/data_format_detectors/canonical_countries.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/tasks/normalization_utils.h"
#include "cc/match_service/validators/match_key_encoding_validator.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

namespace {

// Absolute minimum raw ciphertext overhead across all supported CFM AEAD
// encryption algorithms (AES-GCM / ChaCha20-Poly1305 with RAW prefix:
// 12 byte IV + 16 byte AEAD Auth Tag = 28 bytes).
constexpr size_t kMinRawEncryptedPiiBytesForAead = 28;
// Absolute minimum raw ciphertext overhead across all supported CFM hybrid
// encryption algorithms (HPKE variants with no prefix:
// 32 byte Encapsulated Key + 16 byte AEAD Auth Tag = 48 bytes).
constexpr size_t kMinRawEncryptedPiiBytesForHybrid = 48;

bool IsInCanonicalCountriesMap(absl::string_view country_code) {
  std::string normalized = NormalizeCountryCode(country_code);

  return GetCanonicalCountriesMap().contains(normalized);
}

size_t GetMinimumEncryptedPiiLengthForAeadEncryption(
    backend::MatchKeyEncoding encoding) {
  switch (encoding) {
    case backend::MATCH_KEY_ENCODING_BASE64:
    case backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE:
      // Minimum 28 raw bytes encoded in Base64: ceil(28 * 4 / 3) = 38
      // characters (or 40 characters with padding).
      return 38;
    case backend::MATCH_KEY_ENCODING_HEX:
      // Minimum 28 raw bytes in Hex: 28 * 2 = 56 characters.
      return 56;
    default:
      return kMinRawEncryptedPiiBytesForAead;
  }
}

size_t GetMinimumEncryptedPiiLengthForHybridEncryption(
    backend::MatchKeyEncoding encoding) {
  switch (encoding) {
    case backend::MATCH_KEY_ENCODING_BASE64:
    case backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE:
      // Minimum 48 raw bytes encoded in Base64: ceil(48 * 4 / 3) = 64
      // characters.
      return 64;
    case backend::MATCH_KEY_ENCODING_HEX:
      // Minimum 48 raw bytes in Hex: 48 * 2 = 96 characters.
      return 96;
    default:
      return kMinRawEncryptedPiiBytesForHybrid;
  }
}

absl::StatusOr<bool> AreCountryAndZipEncryptedInternal(
    const backend::CompositeField& composite_field,
    backend::MatchKeyEncoding encoding, size_t min_length) {
  if (composite_field.type() !=
      backend::CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS) {
    return Status(backend::Error::INVALID_MATCH_KEY_FIELD,
                  "Only address composite fields are supported.");
  }

  absl::string_view country_code;
  absl::string_view zip_code;
  bool has_country_code = false;
  bool has_zip_code = false;

  for (const auto& field : composite_field.values()) {
    if (field.type() == backend::FieldType::FIELD_TYPE_COUNTRY_CODE) {
      country_code = field.value();
      has_country_code = true;
    } else if (field.type() == backend::FieldType::FIELD_TYPE_ZIP_CODE) {
      zip_code = field.value();
      has_zip_code = true;
    }
  }

  if (!has_country_code || !has_zip_code) {
    return Status(backend::Error::INVALID_MATCH_KEY_FIELD,
                  "Address composite field must contain both country code and "
                  "zip code.");
  }

  return (country_code.length() > min_length) &&
         (zip_code.length() > min_length) &&
         ValidateMatchKeyEncoding(encoding, country_code).ok() &&
         ValidateMatchKeyEncoding(encoding, zip_code).ok() &&
         !IsInCanonicalCountriesMap(country_code);
}

}  // namespace

size_t GetMinimumEncryptedPiiLength(
    backend::MatchKeyEncoding encoding,
    const backend::EncryptionKey& encryption_key) {
  if (encryption_key.has_coordinator_key()) {
    return GetMinimumEncryptedPiiLengthForHybridEncryption(encoding);
  } else if (encryption_key.has_wrapped_key()) {
    return GetMinimumEncryptedPiiLengthForAeadEncryption(encoding);
  } else {
    // Default to AEAD encryption minimum length.
    return GetMinimumEncryptedPiiLengthForAeadEncryption(encoding);
  }
}

absl::StatusOr<bool> AreCountryCodeZipCodeEncrypted(
    const backend::CompositeField& composite_field,
    backend::MatchKeyEncoding encoding,
    const backend::EncryptionKey& encryption_key) {
  const size_t min_length =
      GetMinimumEncryptedPiiLength(encoding, encryption_key);
  return AreCountryAndZipEncryptedInternal(composite_field, encoding,
                                           min_length);
}

}  // namespace google::confidential_match::match_service
