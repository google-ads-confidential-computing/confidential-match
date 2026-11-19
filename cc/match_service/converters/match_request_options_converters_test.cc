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

#include "absl/status/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(MatchRequestOptionsConvertersTest,
     ToBackendEmptyMatchRequestOptionsLeavesFieldsUnset) {
  api::v1::MatchRequestOptions in;
  backend::MatchRequestOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_FALSE(out.has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_FALSE(out.has_user_data_normalizer_options());
  EXPECT_FALSE(out.has_regional_group());
}

TEST(MatchRequestOptionsConvertersTest,
     ToApiEmptyMatchRequestOptionsLeavesFieldsUnset) {
  backend::MatchRequestOptions in;
  api::v1::MatchRequestOptions out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_FALSE(out.has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_FALSE(out.has_user_data_normalizer_options());
  EXPECT_FALSE(out.has_regional_group());
}

TEST(MatchRequestOptionsConvertersTest, ToBackendConvertsRegionalGroup) {
  api::v1::MatchRequestOptions in;
  in.set_regional_group("eu");
  backend::MatchRequestOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_regional_group());
  EXPECT_EQ(out.regional_group(), "eu");
}

TEST(MatchRequestOptionsConvertersTest, ToApiConvertsRegionalGroup) {
  backend::MatchRequestOptions in;
  in.set_regional_group("eu");
  api::v1::MatchRequestOptions out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  ASSERT_TRUE(out.has_regional_group());
  EXPECT_EQ(out.regional_group(), "eu");
}

TEST(MatchRequestOptionsConvertersTest,
     ToBackendClearsPrePopulatedOutputFields) {
  api::v1::MatchRequestOptions in;
  backend::MatchRequestOptions out;
  out.set_enable_encrypted_plaintext_pii_auto_detection(true);
  out.set_regional_group("eu");
  out.mutable_user_data_normalizer_options()
      ->mutable_email_normalizer_options()
      ->set_strip_parts_after_plus_sign(false);

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_FALSE(out.has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_FALSE(out.has_user_data_normalizer_options());
  EXPECT_FALSE(out.has_regional_group());
}

TEST(MatchRequestOptionsConvertersTest, ToApiClearsPrePopulatedOutputFields) {
  backend::MatchRequestOptions in;
  in.mutable_user_data_normalizer_options()
      ->mutable_name_normalizer_options()
      ->set_strip_common_prefixes(false);
  api::v1::MatchRequestOptions out;
  out.set_enable_encrypted_plaintext_pii_auto_detection(true);
  out.set_regional_group("eu");
  out.mutable_user_data_normalizer_options()
      ->mutable_zip_code_normalizer_options()
      ->set_to_lowercase(false);
  out.mutable_user_data_normalizer_options()
      ->mutable_name_normalizer_options()
      ->set_remove_numbers_and_special_chars(true);

  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_FALSE(out.has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_FALSE(out.has_regional_group());
  ASSERT_TRUE(out.has_user_data_normalizer_options());
  EXPECT_FALSE(
      out.user_data_normalizer_options().has_zip_code_normalizer_options());
  const api::v1::NameNormalizerOptions& name =
      out.user_data_normalizer_options().name_normalizer_options();
  EXPECT_TRUE(name.has_strip_common_prefixes());
  EXPECT_FALSE(name.strip_common_prefixes());
  EXPECT_FALSE(name.has_remove_numbers_and_special_chars());
}

TEST(MatchRequestOptionsConvertersTest,
     ToBackendConvertsAutoDetectionAndUserDataNormalizerOptions) {
  api::v1::MatchRequestOptions in;
  in.set_enable_encrypted_plaintext_pii_auto_detection(true);
  in.mutable_user_data_normalizer_options()
      ->mutable_phone_number_normalizer_options()
      ->set_use_country_code_hint(true);
  backend::MatchRequestOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_TRUE(out.has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_TRUE(out.enable_encrypted_plaintext_pii_auto_detection());
  ASSERT_TRUE(out.has_user_data_normalizer_options());
  EXPECT_TRUE(out.user_data_normalizer_options()
                  .phone_number_normalizer_options()
                  .use_country_code_hint());
}

TEST(MatchRequestOptionsConvertersTest,
     ToApiConvertsAutoDetectionAndUserDataNormalizerOptions) {
  backend::MatchRequestOptions in;
  in.set_enable_encrypted_plaintext_pii_auto_detection(true);
  in.mutable_user_data_normalizer_options()
      ->mutable_phone_number_normalizer_options()
      ->set_use_country_code_hint(true);
  api::v1::MatchRequestOptions out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_TRUE(out.has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_TRUE(out.enable_encrypted_plaintext_pii_auto_detection());
  ASSERT_TRUE(out.has_user_data_normalizer_options());
  EXPECT_TRUE(out.user_data_normalizer_options()
                  .phone_number_normalizer_options()
                  .use_country_code_hint());
}

TEST(MatchRequestOptionsConvertersTest,
     ToBackendUnsetNormalizerSubMessagesStayUnset) {
  api::v1::UserDataNormalizerOptions in;
  in.mutable_name_normalizer_options()->set_strip_common_prefixes(false);
  backend::UserDataNormalizerOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_TRUE(out.has_name_normalizer_options());
  EXPECT_FALSE(out.has_email_normalizer_options());
  EXPECT_FALSE(out.has_zip_code_normalizer_options());
  EXPECT_FALSE(out.has_phone_number_normalizer_options());
}

TEST(MatchRequestOptionsConvertersTest,
     ToApiUnsetNormalizerSubMessagesStayUnset) {
  backend::UserDataNormalizerOptions in;
  in.mutable_email_normalizer_options()->set_replace_googlemail_with_gmail(
      false);
  api::v1::UserDataNormalizerOptions out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_TRUE(out.has_email_normalizer_options());
  EXPECT_FALSE(out.has_zip_code_normalizer_options());
  EXPECT_FALSE(out.has_name_normalizer_options());
  EXPECT_FALSE(out.has_phone_number_normalizer_options());
}

TEST(MatchRequestOptionsConvertersTest,
     ToBackendLeavesUnsetNormalizerOptionsAtBackendDefaults) {
  api::v1::MatchRequestOptions in;
  api::v1::UserDataNormalizerOptions& in_options =
      *in.mutable_user_data_normalizer_options();
  in_options.mutable_phone_number_normalizer_options();
  in_options.mutable_name_normalizer_options();
  in_options.mutable_email_normalizer_options();
  in_options.mutable_zip_code_normalizer_options();
  backend::MatchRequestOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_user_data_normalizer_options());
  const backend::UserDataNormalizerOptions& options =
      out.user_data_normalizer_options();

  // Empty sub-messages stay set, and their unset fields read the backend
  // proto defaults.
  ASSERT_TRUE(options.has_phone_number_normalizer_options());
  const backend::PhoneNumberNormalizerOptions& phone =
      options.phone_number_normalizer_options();
  EXPECT_FALSE(phone.has_use_country_code_hint());
  EXPECT_FALSE(phone.use_country_code_hint());

  ASSERT_TRUE(options.has_name_normalizer_options());
  const backend::NameNormalizerOptions& name =
      options.name_normalizer_options();
  EXPECT_FALSE(name.has_strip_common_prefixes());
  EXPECT_TRUE(name.strip_common_prefixes());
  EXPECT_FALSE(name.has_remove_numbers_and_special_chars());
  EXPECT_FALSE(name.remove_numbers_and_special_chars());

  ASSERT_TRUE(options.has_email_normalizer_options());
  const backend::EmailNormalizerOptions& email =
      options.email_normalizer_options();
  EXPECT_FALSE(email.has_strip_parts_after_plus_sign());
  EXPECT_TRUE(email.strip_parts_after_plus_sign());
  EXPECT_FALSE(email.has_strip_periods_in_facebookmail());
  EXPECT_TRUE(email.strip_periods_in_facebookmail());
  EXPECT_FALSE(email.has_replace_googlemail_with_gmail());
  EXPECT_TRUE(email.replace_googlemail_with_gmail());

  ASSERT_TRUE(options.has_zip_code_normalizer_options());
  const backend::ZipCodeNormalizerOptions& zip =
      options.zip_code_normalizer_options();
  EXPECT_FALSE(zip.has_only_trim_surrounding_whitespaces());
  EXPECT_FALSE(zip.only_trim_surrounding_whitespaces());
  EXPECT_FALSE(zip.has_to_lowercase());
  EXPECT_TRUE(zip.to_lowercase());
  EXPECT_FALSE(zip.has_fall_back_to_unnormalized_if_empty());
  EXPECT_TRUE(zip.fall_back_to_unnormalized_if_empty());
  EXPECT_FALSE(zip.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_TRUE(zip.pass_country_code_for_us_zip_code_normalization());
  EXPECT_FALSE(zip.has_zero_pad_us_zip_code());
  EXPECT_TRUE(zip.zero_pad_us_zip_code());
  EXPECT_FALSE(zip.has_truncate_us_zip_code());
  EXPECT_TRUE(zip.truncate_us_zip_code());
}

TEST(MatchRequestOptionsConvertersTest,
     ToBackendPartialZipCodeNormalizerOptionsCopiesOnlySetFields) {
  api::v1::ZipCodeNormalizerOptions in;
  in.set_to_lowercase(false);
  in.set_truncate_us_zip_code(false);
  backend::ZipCodeNormalizerOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_TRUE(out.has_to_lowercase());
  EXPECT_FALSE(out.to_lowercase());
  EXPECT_TRUE(out.has_truncate_us_zip_code());
  EXPECT_FALSE(out.truncate_us_zip_code());
  EXPECT_FALSE(out.has_only_trim_surrounding_whitespaces());
  EXPECT_FALSE(out.has_fall_back_to_unnormalized_if_empty());
  EXPECT_FALSE(out.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_FALSE(out.has_zero_pad_us_zip_code());
}

TEST(MatchRequestOptionsConvertersTest,
     ToApiPartialEmailNormalizerOptionsCopiesOnlySetFields) {
  backend::EmailNormalizerOptions in;
  in.set_strip_parts_after_plus_sign(false);
  api::v1::EmailNormalizerOptions out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_TRUE(out.has_strip_parts_after_plus_sign());
  EXPECT_FALSE(out.strip_parts_after_plus_sign());
  EXPECT_FALSE(out.has_strip_periods_in_facebookmail());
  EXPECT_FALSE(out.has_replace_googlemail_with_gmail());
}

TEST(MatchRequestOptionsConvertersTest,
     ToBackendPartialNameNormalizerOptionsCopiesOnlySetFields) {
  api::v1::NameNormalizerOptions in;
  in.set_remove_numbers_and_special_chars(true);
  backend::NameNormalizerOptions out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_TRUE(out.has_remove_numbers_and_special_chars());
  EXPECT_TRUE(out.remove_numbers_and_special_chars());
  EXPECT_FALSE(out.has_strip_common_prefixes());
}

TEST(MatchRequestOptionsConvertersTest,
     ToApiUnsetPhoneNumberNormalizerOptionFieldStaysUnset) {
  backend::PhoneNumberNormalizerOptions in;
  api::v1::PhoneNumberNormalizerOptions out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_FALSE(out.has_use_country_code_hint());
}

// The empty-input tests check that unset fields stay unset in both directions,
// even for backend fields with a non-false [default]. The presence tests set
// each field to the value its getter returns when unset (false on the API side,
// the [default] on the backend side), so they fail if a converter copies a
// field based on its value instead of its presence.

TEST(MatchRequestOptionsConvertersTest,
     EmptyEmailNormalizerOptionsLeavesFieldsUnset) {
  backend::EmailNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api::v1::EmailNormalizerOptions(), backend_out),
              IsOk());
  EXPECT_FALSE(backend_out.has_strip_parts_after_plus_sign());
  EXPECT_FALSE(backend_out.has_strip_periods_in_facebookmail());
  EXPECT_FALSE(backend_out.has_replace_googlemail_with_gmail());

  api::v1::EmailNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend::EmailNormalizerOptions(), api_out), IsOk());
  EXPECT_FALSE(api_out.has_strip_parts_after_plus_sign());
  EXPECT_FALSE(api_out.has_strip_periods_in_facebookmail());
  EXPECT_FALSE(api_out.has_replace_googlemail_with_gmail());
}

TEST(MatchRequestOptionsConvertersTest,
     EmailNormalizerOptionsSetToGetterDefaultsKeepPresence) {
  api::v1::EmailNormalizerOptions api_in;
  api_in.set_strip_parts_after_plus_sign(false);
  api_in.set_strip_periods_in_facebookmail(false);
  api_in.set_replace_googlemail_with_gmail(false);
  backend::EmailNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api_in, backend_out), IsOk());
  EXPECT_TRUE(backend_out.has_strip_parts_after_plus_sign());
  EXPECT_FALSE(backend_out.strip_parts_after_plus_sign());
  EXPECT_TRUE(backend_out.has_strip_periods_in_facebookmail());
  EXPECT_FALSE(backend_out.strip_periods_in_facebookmail());
  EXPECT_TRUE(backend_out.has_replace_googlemail_with_gmail());
  EXPECT_FALSE(backend_out.replace_googlemail_with_gmail());

  backend::EmailNormalizerOptions backend_in;
  backend_in.set_strip_parts_after_plus_sign(true);
  backend_in.set_strip_periods_in_facebookmail(true);
  backend_in.set_replace_googlemail_with_gmail(true);
  api::v1::EmailNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend_in, api_out), IsOk());
  EXPECT_TRUE(api_out.has_strip_parts_after_plus_sign());
  EXPECT_TRUE(api_out.strip_parts_after_plus_sign());
  EXPECT_TRUE(api_out.has_strip_periods_in_facebookmail());
  EXPECT_TRUE(api_out.strip_periods_in_facebookmail());
  EXPECT_TRUE(api_out.has_replace_googlemail_with_gmail());
  EXPECT_TRUE(api_out.replace_googlemail_with_gmail());
}

TEST(MatchRequestOptionsConvertersTest,
     EmptyZipCodeNormalizerOptionsLeavesFieldsUnset) {
  backend::ZipCodeNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api::v1::ZipCodeNormalizerOptions(), backend_out),
              IsOk());
  EXPECT_FALSE(backend_out.has_only_trim_surrounding_whitespaces());
  EXPECT_FALSE(backend_out.has_to_lowercase());
  EXPECT_FALSE(backend_out.has_fall_back_to_unnormalized_if_empty());
  EXPECT_FALSE(
      backend_out.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_FALSE(backend_out.has_zero_pad_us_zip_code());
  EXPECT_FALSE(backend_out.has_truncate_us_zip_code());

  api::v1::ZipCodeNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend::ZipCodeNormalizerOptions(), api_out), IsOk());
  EXPECT_FALSE(api_out.has_only_trim_surrounding_whitespaces());
  EXPECT_FALSE(api_out.has_to_lowercase());
  EXPECT_FALSE(api_out.has_fall_back_to_unnormalized_if_empty());
  EXPECT_FALSE(api_out.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_FALSE(api_out.has_zero_pad_us_zip_code());
  EXPECT_FALSE(api_out.has_truncate_us_zip_code());
}

TEST(MatchRequestOptionsConvertersTest,
     ZipCodeNormalizerOptionsSetToGetterDefaultsKeepPresence) {
  api::v1::ZipCodeNormalizerOptions api_in;
  api_in.set_only_trim_surrounding_whitespaces(false);
  api_in.set_to_lowercase(false);
  api_in.set_fall_back_to_unnormalized_if_empty(false);
  api_in.set_pass_country_code_for_us_zip_code_normalization(false);
  api_in.set_zero_pad_us_zip_code(false);
  api_in.set_truncate_us_zip_code(false);
  backend::ZipCodeNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api_in, backend_out), IsOk());
  EXPECT_TRUE(backend_out.has_only_trim_surrounding_whitespaces());
  EXPECT_FALSE(backend_out.only_trim_surrounding_whitespaces());
  EXPECT_TRUE(backend_out.has_to_lowercase());
  EXPECT_FALSE(backend_out.to_lowercase());
  EXPECT_TRUE(backend_out.has_fall_back_to_unnormalized_if_empty());
  EXPECT_FALSE(backend_out.fall_back_to_unnormalized_if_empty());
  EXPECT_TRUE(
      backend_out.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_FALSE(backend_out.pass_country_code_for_us_zip_code_normalization());
  EXPECT_TRUE(backend_out.has_zero_pad_us_zip_code());
  EXPECT_FALSE(backend_out.zero_pad_us_zip_code());
  EXPECT_TRUE(backend_out.has_truncate_us_zip_code());
  EXPECT_FALSE(backend_out.truncate_us_zip_code());

  backend::ZipCodeNormalizerOptions backend_in;
  backend_in.set_only_trim_surrounding_whitespaces(false);
  backend_in.set_to_lowercase(true);
  backend_in.set_fall_back_to_unnormalized_if_empty(true);
  backend_in.set_pass_country_code_for_us_zip_code_normalization(true);
  backend_in.set_zero_pad_us_zip_code(true);
  backend_in.set_truncate_us_zip_code(true);
  api::v1::ZipCodeNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend_in, api_out), IsOk());
  EXPECT_TRUE(api_out.has_only_trim_surrounding_whitespaces());
  EXPECT_FALSE(api_out.only_trim_surrounding_whitespaces());
  EXPECT_TRUE(api_out.has_to_lowercase());
  EXPECT_TRUE(api_out.to_lowercase());
  EXPECT_TRUE(api_out.has_fall_back_to_unnormalized_if_empty());
  EXPECT_TRUE(api_out.fall_back_to_unnormalized_if_empty());
  EXPECT_TRUE(api_out.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_TRUE(api_out.pass_country_code_for_us_zip_code_normalization());
  EXPECT_TRUE(api_out.has_zero_pad_us_zip_code());
  EXPECT_TRUE(api_out.zero_pad_us_zip_code());
  EXPECT_TRUE(api_out.has_truncate_us_zip_code());
  EXPECT_TRUE(api_out.truncate_us_zip_code());
}

TEST(MatchRequestOptionsConvertersTest,
     EmptyNameNormalizerOptionsLeavesFieldsUnset) {
  backend::NameNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api::v1::NameNormalizerOptions(), backend_out), IsOk());
  EXPECT_FALSE(backend_out.has_strip_common_prefixes());
  EXPECT_FALSE(backend_out.has_remove_numbers_and_special_chars());

  api::v1::NameNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend::NameNormalizerOptions(), api_out), IsOk());
  EXPECT_FALSE(api_out.has_strip_common_prefixes());
  EXPECT_FALSE(api_out.has_remove_numbers_and_special_chars());
}

TEST(MatchRequestOptionsConvertersTest,
     NameNormalizerOptionsSetToGetterDefaultsKeepPresence) {
  api::v1::NameNormalizerOptions api_in;
  api_in.set_strip_common_prefixes(false);
  api_in.set_remove_numbers_and_special_chars(false);
  backend::NameNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api_in, backend_out), IsOk());
  EXPECT_TRUE(backend_out.has_strip_common_prefixes());
  EXPECT_FALSE(backend_out.strip_common_prefixes());
  EXPECT_TRUE(backend_out.has_remove_numbers_and_special_chars());
  EXPECT_FALSE(backend_out.remove_numbers_and_special_chars());

  backend::NameNormalizerOptions backend_in;
  backend_in.set_strip_common_prefixes(true);
  backend_in.set_remove_numbers_and_special_chars(false);
  api::v1::NameNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend_in, api_out), IsOk());
  EXPECT_TRUE(api_out.has_strip_common_prefixes());
  EXPECT_TRUE(api_out.strip_common_prefixes());
  EXPECT_TRUE(api_out.has_remove_numbers_and_special_chars());
  EXPECT_FALSE(api_out.remove_numbers_and_special_chars());
}

TEST(MatchRequestOptionsConvertersTest,
     EmptyPhoneNumberNormalizerOptionsLeavesFieldsUnset) {
  backend::PhoneNumberNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api::v1::PhoneNumberNormalizerOptions(), backend_out),
              IsOk());
  EXPECT_FALSE(backend_out.has_use_country_code_hint());

  api::v1::PhoneNumberNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend::PhoneNumberNormalizerOptions(), api_out), IsOk());
  EXPECT_FALSE(api_out.has_use_country_code_hint());
}

TEST(MatchRequestOptionsConvertersTest,
     PhoneNumberNormalizerOptionsSetToGetterDefaultsKeepPresence) {
  api::v1::PhoneNumberNormalizerOptions api_in;
  api_in.set_use_country_code_hint(false);
  backend::PhoneNumberNormalizerOptions backend_out;
  EXPECT_THAT(ToBackend(api_in, backend_out), IsOk());
  EXPECT_TRUE(backend_out.has_use_country_code_hint());
  EXPECT_FALSE(backend_out.use_country_code_hint());

  backend::PhoneNumberNormalizerOptions backend_in;
  backend_in.set_use_country_code_hint(false);
  api::v1::PhoneNumberNormalizerOptions api_out;
  EXPECT_THAT(ToApi(backend_in, api_out), IsOk());
  EXPECT_TRUE(api_out.has_use_country_code_hint());
  EXPECT_FALSE(api_out.use_country_code_hint());
}

namespace {

// Returns API UserDataNormalizerOptions with every normalizer option set to
// the opposite of its backend proto default.
api::v1::UserDataNormalizerOptions AllNonDefaultApiOptions() {
  api::v1::UserDataNormalizerOptions options;
  api::v1::EmailNormalizerOptions& email =
      *options.mutable_email_normalizer_options();
  email.set_strip_parts_after_plus_sign(false);
  email.set_strip_periods_in_facebookmail(false);
  email.set_replace_googlemail_with_gmail(false);
  api::v1::ZipCodeNormalizerOptions& zip =
      *options.mutable_zip_code_normalizer_options();
  zip.set_only_trim_surrounding_whitespaces(true);
  zip.set_to_lowercase(false);
  zip.set_fall_back_to_unnormalized_if_empty(false);
  zip.set_pass_country_code_for_us_zip_code_normalization(false);
  zip.set_zero_pad_us_zip_code(false);
  zip.set_truncate_us_zip_code(false);
  api::v1::NameNormalizerOptions& name =
      *options.mutable_name_normalizer_options();
  name.set_strip_common_prefixes(false);
  name.set_remove_numbers_and_special_chars(true);
  options.mutable_phone_number_normalizer_options()->set_use_country_code_hint(
      true);
  return options;
}

// Expects every normalizer option in `options` to be set to the value used by
// AllNonDefaultApiOptions().
template <typename UserDataNormalizerOptionsT>
void ExpectAllNonDefaultOptions(const UserDataNormalizerOptionsT& options) {
  const auto& email = options.email_normalizer_options();
  EXPECT_TRUE(email.has_strip_parts_after_plus_sign());
  EXPECT_FALSE(email.strip_parts_after_plus_sign());
  EXPECT_TRUE(email.has_strip_periods_in_facebookmail());
  EXPECT_FALSE(email.strip_periods_in_facebookmail());
  EXPECT_TRUE(email.has_replace_googlemail_with_gmail());
  EXPECT_FALSE(email.replace_googlemail_with_gmail());

  const auto& zip = options.zip_code_normalizer_options();
  EXPECT_TRUE(zip.has_only_trim_surrounding_whitespaces());
  EXPECT_TRUE(zip.only_trim_surrounding_whitespaces());
  EXPECT_TRUE(zip.has_to_lowercase());
  EXPECT_FALSE(zip.to_lowercase());
  EXPECT_TRUE(zip.has_fall_back_to_unnormalized_if_empty());
  EXPECT_FALSE(zip.fall_back_to_unnormalized_if_empty());
  EXPECT_TRUE(zip.has_pass_country_code_for_us_zip_code_normalization());
  EXPECT_FALSE(zip.pass_country_code_for_us_zip_code_normalization());
  EXPECT_TRUE(zip.has_zero_pad_us_zip_code());
  EXPECT_FALSE(zip.zero_pad_us_zip_code());
  EXPECT_TRUE(zip.has_truncate_us_zip_code());
  EXPECT_FALSE(zip.truncate_us_zip_code());

  const auto& name = options.name_normalizer_options();
  EXPECT_TRUE(name.has_strip_common_prefixes());
  EXPECT_FALSE(name.strip_common_prefixes());
  EXPECT_TRUE(name.has_remove_numbers_and_special_chars());
  EXPECT_TRUE(name.remove_numbers_and_special_chars());

  const auto& phone = options.phone_number_normalizer_options();
  EXPECT_TRUE(phone.has_use_country_code_hint());
  EXPECT_TRUE(phone.use_country_code_hint());
}

}  // namespace

TEST(MatchRequestOptionsConvertersTest,
     UserDataNormalizerOptionsRoundTripsAllFields) {
  api::v1::UserDataNormalizerOptions api_in = AllNonDefaultApiOptions();

  backend::UserDataNormalizerOptions backend_out;
  ASSERT_THAT(ToBackend(api_in, backend_out), IsOk());
  ExpectAllNonDefaultOptions(backend_out);

  api::v1::UserDataNormalizerOptions api_roundtrip;
  ASSERT_THAT(ToApi(backend_out, api_roundtrip), IsOk());
  ExpectAllNonDefaultOptions(api_roundtrip);
}

}  // namespace google::confidential_match::match_service
