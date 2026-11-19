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

#include "cc/match_service/converters/match_request_options_converters.h"

#include "absl/status/status.h"

#include "cc/core/error/status_macros.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::MatchRequestOptions& in,
                       backend::MatchRequestOptions& out) {
  out.Clear();
  if (in.has_enable_encrypted_plaintext_pii_auto_detection()) {
    out.set_enable_encrypted_plaintext_pii_auto_detection(
        in.enable_encrypted_plaintext_pii_auto_detection());
  }
  if (in.has_user_data_normalizer_options()) {
    RETURN_IF_ERROR(ToBackend(in.user_data_normalizer_options(),
                              *out.mutable_user_data_normalizer_options()));
  }
  if (in.has_regional_group()) {
    out.set_regional_group(in.regional_group());
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchRequestOptions& in,
                   api::v1::MatchRequestOptions& out) {
  out.Clear();
  if (in.has_enable_encrypted_plaintext_pii_auto_detection()) {
    out.set_enable_encrypted_plaintext_pii_auto_detection(
        in.enable_encrypted_plaintext_pii_auto_detection());
  }
  if (in.has_user_data_normalizer_options()) {
    RETURN_IF_ERROR(ToApi(in.user_data_normalizer_options(),
                          *out.mutable_user_data_normalizer_options()));
  }
  if (in.has_regional_group()) {
    out.set_regional_group(in.regional_group());
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::UserDataNormalizerOptions& in,
                       backend::UserDataNormalizerOptions& out) {
  out.Clear();
  if (in.has_email_normalizer_options()) {
    RETURN_IF_ERROR(ToBackend(in.email_normalizer_options(),
                              *out.mutable_email_normalizer_options()));
  }
  if (in.has_zip_code_normalizer_options()) {
    RETURN_IF_ERROR(ToBackend(in.zip_code_normalizer_options(),
                              *out.mutable_zip_code_normalizer_options()));
  }
  if (in.has_name_normalizer_options()) {
    RETURN_IF_ERROR(ToBackend(in.name_normalizer_options(),
                              *out.mutable_name_normalizer_options()));
  }
  if (in.has_phone_number_normalizer_options()) {
    RETURN_IF_ERROR(ToBackend(in.phone_number_normalizer_options(),
                              *out.mutable_phone_number_normalizer_options()));
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::UserDataNormalizerOptions& in,
                   api::v1::UserDataNormalizerOptions& out) {
  out.Clear();
  if (in.has_email_normalizer_options()) {
    RETURN_IF_ERROR(ToApi(in.email_normalizer_options(),
                          *out.mutable_email_normalizer_options()));
  }
  if (in.has_zip_code_normalizer_options()) {
    RETURN_IF_ERROR(ToApi(in.zip_code_normalizer_options(),
                          *out.mutable_zip_code_normalizer_options()));
  }
  if (in.has_name_normalizer_options()) {
    RETURN_IF_ERROR(ToApi(in.name_normalizer_options(),
                          *out.mutable_name_normalizer_options()));
  }
  if (in.has_phone_number_normalizer_options()) {
    RETURN_IF_ERROR(ToApi(in.phone_number_normalizer_options(),
                          *out.mutable_phone_number_normalizer_options()));
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::EmailNormalizerOptions& in,
                       backend::EmailNormalizerOptions& out) {
  out.Clear();
  if (in.has_strip_parts_after_plus_sign()) {
    out.set_strip_parts_after_plus_sign(in.strip_parts_after_plus_sign());
  }
  if (in.has_strip_periods_in_facebookmail()) {
    out.set_strip_periods_in_facebookmail(in.strip_periods_in_facebookmail());
  }
  if (in.has_replace_googlemail_with_gmail()) {
    out.set_replace_googlemail_with_gmail(in.replace_googlemail_with_gmail());
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::EmailNormalizerOptions& in,
                   api::v1::EmailNormalizerOptions& out) {
  out.Clear();
  if (in.has_strip_parts_after_plus_sign()) {
    out.set_strip_parts_after_plus_sign(in.strip_parts_after_plus_sign());
  }
  if (in.has_strip_periods_in_facebookmail()) {
    out.set_strip_periods_in_facebookmail(in.strip_periods_in_facebookmail());
  }
  if (in.has_replace_googlemail_with_gmail()) {
    out.set_replace_googlemail_with_gmail(in.replace_googlemail_with_gmail());
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::ZipCodeNormalizerOptions& in,
                       backend::ZipCodeNormalizerOptions& out) {
  out.Clear();
  if (in.has_only_trim_surrounding_whitespaces()) {
    out.set_only_trim_surrounding_whitespaces(
        in.only_trim_surrounding_whitespaces());
  }
  if (in.has_to_lowercase()) {
    out.set_to_lowercase(in.to_lowercase());
  }
  if (in.has_fall_back_to_unnormalized_if_empty()) {
    out.set_fall_back_to_unnormalized_if_empty(
        in.fall_back_to_unnormalized_if_empty());
  }
  if (in.has_pass_country_code_for_us_zip_code_normalization()) {
    out.set_pass_country_code_for_us_zip_code_normalization(
        in.pass_country_code_for_us_zip_code_normalization());
  }
  if (in.has_zero_pad_us_zip_code()) {
    out.set_zero_pad_us_zip_code(in.zero_pad_us_zip_code());
  }
  if (in.has_truncate_us_zip_code()) {
    out.set_truncate_us_zip_code(in.truncate_us_zip_code());
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::ZipCodeNormalizerOptions& in,
                   api::v1::ZipCodeNormalizerOptions& out) {
  out.Clear();
  if (in.has_only_trim_surrounding_whitespaces()) {
    out.set_only_trim_surrounding_whitespaces(
        in.only_trim_surrounding_whitespaces());
  }
  if (in.has_to_lowercase()) {
    out.set_to_lowercase(in.to_lowercase());
  }
  if (in.has_fall_back_to_unnormalized_if_empty()) {
    out.set_fall_back_to_unnormalized_if_empty(
        in.fall_back_to_unnormalized_if_empty());
  }
  if (in.has_pass_country_code_for_us_zip_code_normalization()) {
    out.set_pass_country_code_for_us_zip_code_normalization(
        in.pass_country_code_for_us_zip_code_normalization());
  }
  if (in.has_zero_pad_us_zip_code()) {
    out.set_zero_pad_us_zip_code(in.zero_pad_us_zip_code());
  }
  if (in.has_truncate_us_zip_code()) {
    out.set_truncate_us_zip_code(in.truncate_us_zip_code());
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::NameNormalizerOptions& in,
                       backend::NameNormalizerOptions& out) {
  out.Clear();
  if (in.has_strip_common_prefixes()) {
    out.set_strip_common_prefixes(in.strip_common_prefixes());
  }
  if (in.has_remove_numbers_and_special_chars()) {
    out.set_remove_numbers_and_special_chars(
        in.remove_numbers_and_special_chars());
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::NameNormalizerOptions& in,
                   api::v1::NameNormalizerOptions& out) {
  out.Clear();
  if (in.has_strip_common_prefixes()) {
    out.set_strip_common_prefixes(in.strip_common_prefixes());
  }
  if (in.has_remove_numbers_and_special_chars()) {
    out.set_remove_numbers_and_special_chars(
        in.remove_numbers_and_special_chars());
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::PhoneNumberNormalizerOptions& in,
                       backend::PhoneNumberNormalizerOptions& out) {
  out.Clear();
  if (in.has_use_country_code_hint()) {
    out.set_use_country_code_hint(in.use_country_code_hint());
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::PhoneNumberNormalizerOptions& in,
                   api::v1::PhoneNumberNormalizerOptions& out) {
  out.Clear();
  if (in.has_use_country_code_hint()) {
    out.set_use_country_code_hint(in.use_country_code_hint());
  }
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
