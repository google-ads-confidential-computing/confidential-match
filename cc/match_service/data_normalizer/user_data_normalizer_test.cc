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

#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "cc/core/hash/mock_hasher.h"
#include "cc/core/hash/sha256_hasher.h"
#include "cc/match_service/error/error.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::
    UserDataNormalizerOptions;
using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

// Base64-encoded SHA-256 of "johndoe@gmail.com".
constexpr char kJohnDoeGmailHash[] =
    "BqJA0RzCAWdtqXb3tJNBGB/RgNo3y+QKd0MsCjZsgMM=";

class UserDataNormalizerTest : public ::testing::Test {
 protected:
  // Returns the Base64 SHA-256 hash of `value`.
  std::string Hash(absl::string_view value) const {
    absl::StatusOr<std::string> hash = hasher_.Base64EncodedHash(value);
    EXPECT_TRUE(hash.ok());
    return hash.value_or("");
  }

  Sha256Hasher hasher_;
  UserDataNormalizer normalizer_{&hasher_};
  UserDataNormalizerOptions options_;
};

TEST_F(UserDataNormalizerTest, EmailDefaultOptionsCanonicalizesGmail) {
  EXPECT_THAT(normalizer_.NormalizeAndHashEmail("John.Doe+tag@GoogleMail.com",
                                                options_),
              IsOkAndHolds(kJohnDoeGmailHash));
}

TEST_F(UserDataNormalizerTest, EmailGooglemailReplacementDisabled) {
  options_.mutable_email_normalizer_options()
      ->set_replace_googlemail_with_gmail(false);

  EXPECT_THAT(normalizer_.NormalizeAndHashEmail("John.Doe+tag@GoogleMail.com",
                                                options_),
              IsOkAndHolds(Hash("johndoe@googlemail.com")));
}

TEST_F(UserDataNormalizerTest, EmailSurroundingWhitespaceTrimmed) {
  EXPECT_THAT(
      normalizer_.NormalizeAndHashEmail(" \tJohn.Doe@gmail.com \n", options_),
      IsOkAndHolds(kJohnDoeGmailHash));
}

TEST_F(UserDataNormalizerTest, EmailPlusSignStrippingDisabledKeepsTag) {
  options_.mutable_email_normalizer_options()->set_strip_parts_after_plus_sign(
      false);

  EXPECT_THAT(
      normalizer_.NormalizeAndHashEmail("John.Doe+tag@gmail.com", options_),
      IsOkAndHolds(Hash("johndoe+tag@gmail.com")));
}

TEST_F(UserDataNormalizerTest, EmailPlusSignStrippingOnlyAppliesToGmail) {
  EXPECT_THAT(
      normalizer_.NormalizeAndHashEmail("John.Doe+tag@example.com", options_),
      IsOkAndHolds(Hash("john.doe+tag@example.com")));
}

TEST_F(UserDataNormalizerTest, EmailFacebookmailPeriodsStrippedByDefault) {
  EXPECT_THAT(
      normalizer_.NormalizeAndHashEmail("John.Doe@facebookmail.com", options_),
      IsOkAndHolds(Hash("johndoe@facebookmail.com")));
}

TEST_F(UserDataNormalizerTest, EmailFacebookmailPeriodStrippingDisabled) {
  options_.mutable_email_normalizer_options()
      ->set_strip_periods_in_facebookmail(false);

  EXPECT_THAT(
      normalizer_.NormalizeAndHashEmail("John.Doe@facebookmail.com", options_),
      IsOkAndHolds(Hash("john.doe@facebookmail.com")));
}

TEST_F(UserDataNormalizerTest, MalformedEmailFallsBackToUnnormalizedValue) {
  EXPECT_THAT(normalizer_.NormalizeAndHashEmail("John Doe", options_),
              IsOkAndHolds(Hash("John Doe")));
  EXPECT_THAT(normalizer_.NormalizeAndHashEmail("A@B@Example.com", options_),
              IsOkAndHolds(Hash("A@B@Example.com")));
}

TEST_F(UserDataNormalizerTest, EmptyEmailHashesEmptyValue) {
  EXPECT_THAT(normalizer_.NormalizeAndHashEmail("", options_),
              IsOkAndHolds(Hash("")));
}

TEST_F(UserDataNormalizerTest, EmailGmailPeriodsAlwaysStripped) {
  options_.mutable_email_normalizer_options()->set_strip_parts_after_plus_sign(
      false);
  options_.mutable_email_normalizer_options()
      ->set_strip_periods_in_facebookmail(false);

  EXPECT_THAT(normalizer_.NormalizeAndHashEmail("J.o.h.n@gmail.com", options_),
              IsOkAndHolds(Hash("john@gmail.com")));
}

TEST_F(UserDataNormalizerTest, PhoneHintDisabledByDefaultIgnoresHint) {
  EXPECT_THAT(
      normalizer_.NormalizeAndHashPhoneNumber("(650) 253-0000", "US", options_),
      IsOkAndHolds(Hash("+6502530000")));
}

TEST_F(UserDataNormalizerTest, PhoneHintEnabledPrefixesCountryCallingCode) {
  options_.mutable_phone_number_normalizer_options()->set_use_country_code_hint(
      true);

  EXPECT_THAT(normalizer_.NormalizeAndHashPhoneNumber("(650) 253-0000", " US ",
                                                      options_),
              IsOkAndHolds(Hash("+16502530000")));
  EXPECT_THAT(normalizer_.NormalizeAndHashPhoneNumber(
                  "020 1234 5678", "United Kingdom", options_),
              IsOkAndHolds(Hash("+4402012345678")));
}

TEST_F(UserDataNormalizerTest, PhoneHintEnabledWithoutHint) {
  options_.mutable_phone_number_normalizer_options()->set_use_country_code_hint(
      true);

  EXPECT_THAT(normalizer_.NormalizeAndHashPhoneNumber("650-253-0000",
                                                      std::nullopt, options_),
              IsOkAndHolds(Hash("+6502530000")));
  EXPECT_THAT(
      normalizer_.NormalizeAndHashPhoneNumber("650-253-0000", "  ", options_),
      IsOkAndHolds(Hash("+6502530000")));
}

TEST_F(UserDataNormalizerTest, PhoneWithPlusSignIgnoresHint) {
  options_.mutable_phone_number_normalizer_options()->set_use_country_code_hint(
      true);

  EXPECT_THAT(normalizer_.NormalizeAndHashPhoneNumber("+44 20 1234 5678", "us",
                                                      options_),
              IsOkAndHolds(Hash("+442012345678")));
}

TEST_F(UserDataNormalizerTest, PhoneInvalidHintRetriesWithoutHint) {
  options_.mutable_phone_number_normalizer_options()->set_use_country_code_hint(
      true);

  EXPECT_THAT(normalizer_.NormalizeAndHashPhoneNumber("(650) 253-0000",
                                                      "invalid", options_),
              IsOkAndHolds(Hash("+6502530000")));
}

TEST_F(UserDataNormalizerTest, PhoneHintAcceptsAlpha3CountryCode) {
  options_.mutable_phone_number_normalizer_options()->set_use_country_code_hint(
      true);

  EXPECT_THAT(
      normalizer_.NormalizeAndHashPhoneNumber("020 1234 5678", "GBR", options_),
      IsOkAndHolds(Hash("+4402012345678")));
}

TEST_F(UserDataNormalizerTest, PhoneWithoutDigitsBecomesPlusSign) {
  EXPECT_THAT(
      normalizer_.NormalizeAndHashPhoneNumber("", std::nullopt, options_),
      IsOkAndHolds(Hash("+")));
  EXPECT_THAT(
      normalizer_.NormalizeAndHashPhoneNumber("abc", std::nullopt, options_),
      IsOkAndHolds(Hash("+")));
}

TEST_F(UserDataNormalizerTest, AddressDefaultOptions) {
  std::vector<std::string> parts = {"Mr. John", " DOE ", " United States ",
                                    "94043-1351"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash("john"), Hash("doe"), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressPrefixStrippingDisabled) {
  options_.mutable_name_normalizer_options()->set_strip_common_prefixes(false);
  std::vector<std::string> parts = {"Mr. John", "Dr. Doe", "us", "94043"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash("mr. john"), Hash("dr. doe"), "us",
                                       "94043")));
}

TEST_F(UserDataNormalizerTest, AddressSpecialCharsKeptByDefault) {
  std::vector<std::string> parts = {"Jo-hn 2", "O'Doe", "us", "94043"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash("jo-hn 2"), Hash("o'doe"), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressSpecialCharsRemovedWhenEnabled) {
  options_.mutable_name_normalizer_options()
      ->set_remove_numbers_and_special_chars(true);
  std::vector<std::string> parts = {"Mr. Jo-hn 2", "Doe.Smith", "us", "94043"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash("john"), Hash("doesmith"), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressEmptyNamesHashedAsEmptyString) {
  std::vector<std::string> parts = {"", "", "CA", "K1A 0B1"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "ca", "k1a0b1")));
}

TEST_F(UserDataNormalizerTest, AddressWhitespaceOnlyNamesHashedAsEmptyString) {
  std::vector<std::string> parts = {"  ", "\t", "us", "94043"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressCountryNameCanonicalized) {
  std::vector<std::string> parts = {"", "", "Canada", "K1A-0B1"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "ca", "k1a0b1")));
}

TEST_F(UserDataNormalizerTest, AddressZipOnlyTrimmedWhenEnabled) {
  options_.mutable_zip_code_normalizer_options()
      ->set_only_trim_surrounding_whitespaces(true);
  std::vector<std::string> parts = {"", "", "US", "  94043-1351 \t"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "94043-1351")));
}

TEST_F(UserDataNormalizerTest, AddressUsFiveDigitZipUnchanged) {
  std::vector<std::string> parts = {"", "", "US", " 94043 "};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressUsZipPlusFourTruncatedByDefault) {
  std::vector<std::string> hyphenated = {"", "", "us", "94043-1351"};
  std::vector<std::string> unhyphenated = {"", "", "United States",
                                           "940431351"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(hyphenated, options_),
      IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "94043")));
  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(unhyphenated, options_),
      IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressUsZipPlusFourKeptWhenTruncationDisabled) {
  options_.mutable_zip_code_normalizer_options()->set_truncate_us_zip_code(
      false);
  std::vector<std::string> parts = {"", "", "us", "94043-1351"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "940431351")));
}

TEST_F(UserDataNormalizerTest, AddressUsShortZipZeroPaddedByDefault) {
  std::vector<std::string> parts = {"", "", "United States", "2134"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "02134")));
}

TEST_F(UserDataNormalizerTest,
       AddressUsSevenDigitZipPaddedToNineThenTruncatedByDefault) {
  std::vector<std::string> parts = {"", "", "us", "4304313"};

  // Padded to "004304313", then truncated to the first 5 characters.
  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "00430")));
}

TEST_F(UserDataNormalizerTest, AddressUsShortZipNotPaddedWhenZeroPadDisabled) {
  options_.mutable_zip_code_normalizer_options()->set_zero_pad_us_zip_code(
      false);
  std::vector<std::string> parts = {"", "", "us", "2134"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "2134")));
}

TEST_F(UserDataNormalizerTest,
       AddressUsZipNotPaddedOrTruncatedWhenCountryCodeNotPassed) {
  options_.mutable_zip_code_normalizer_options()
      ->set_pass_country_code_for_us_zip_code_normalization(false);
  std::vector<std::string> zip_plus_four = {"", "", "us", "94043-1351"};
  std::vector<std::string> short_zip = {"", "", "us", "2134"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(zip_plus_four, options_),
      IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "940431351")));
  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(short_zip, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "2134")));
}

TEST_F(UserDataNormalizerTest, AddressNonUsZipNotPaddedOrTruncated) {
  std::vector<std::string> short_zip = {"", "", "DE", "1234"};
  std::vector<std::string> nine_digit_zip = {"", "", "DE", "123456789"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(short_zip, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "de", "1234")));
  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(nine_digit_zip, options_),
      IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "de", "123456789")));
}

TEST_F(UserDataNormalizerTest, AddressZipWithEmptyCountryNotPadded) {
  std::vector<std::string> parts = {"", "", "", "2134"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "", "2134")));
}

TEST_F(UserDataNormalizerTest, AddressZipLowercasedByDefault) {
  std::vector<std::string> parts = {"", "", "GB", "SW1A 1AA"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "gb", "sw1a1aa")));
}

TEST_F(UserDataNormalizerTest, AddressZipCaseKeptWhenLowercasingDisabled) {
  options_.mutable_zip_code_normalizer_options()->set_to_lowercase(false);
  std::vector<std::string> parts = {"", "", "GB", "SW1A 1AA"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "gb", "SW1A1AA")));
}

TEST_F(UserDataNormalizerTest,
       AddressZipWithoutAlphanumericsFallsBackToUnnormalizedByDefault) {
  std::vector<std::string> parts = {"", "", "us", "--"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "--")));
}

TEST_F(UserDataNormalizerTest,
       AddressZipWithoutAlphanumericsBecomesEmptyWhenFallbackDisabled) {
  options_.mutable_zip_code_normalizer_options()
      ->set_fall_back_to_unnormalized_if_empty(false);
  std::vector<std::string> parts = {"", "", "us", "--"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "")));
}

TEST_F(UserDataNormalizerTest, AddressZipOnlyTrimmedKeepsInnerCharacters) {
  options_.mutable_zip_code_normalizer_options()
      ->set_only_trim_surrounding_whitespaces(true);
  std::vector<std::string> parts = {"", "", "GB", " SW1A 1AA "};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "gb", "SW1A 1AA")));
}

TEST_F(UserDataNormalizerTest,
       AddressWhitespaceOnlyZipBecomesEmptyWhenOnlyTrimmed) {
  options_.mutable_zip_code_normalizer_options()
      ->set_only_trim_surrounding_whitespaces(true);
  // Trim-only ignores fall_back_to_unnormalized_if_empty, which is true by
  // default.
  std::vector<std::string> parts = {"", "", "us", "   "};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "")));
}

TEST_F(UserDataNormalizerTest,
       AddressWhitespaceOnlyZipFallsBackToUnnormalized) {
  std::vector<std::string> parts = {"", "", "us", "   "};

  // The fallback is the unnormalized (untrimmed) zip code.
  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "   ")));
}

TEST_F(UserDataNormalizerTest,
       AddressWhitespaceOnlyZipBecomesEmptyWhenFallbackDisabled) {
  options_.mutable_zip_code_normalizer_options()
      ->set_fall_back_to_unnormalized_if_empty(false);
  std::vector<std::string> parts = {"", "", "us", "   "};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "")));
}

TEST_F(UserDataNormalizerTest, AddressLeadingWhitespaceBeforeTitleIsStripped) {
  std::vector<std::string> parts = {" Mr. John", "\tDr. Doe ", "us", "94043"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash("john"), Hash("doe"), "us", "94043")));
}

TEST_F(UserDataNormalizerTest,
       AddressNamesEmptyAfterNormalizationFallBackToTrimmedName) {
  options_.mutable_name_normalizer_options()
      ->set_remove_numbers_and_special_chars(true);
  std::vector<std::string> parts = {"123", "--", "us", "94043"};
  std::vector<std::string> padded = {" 123 ", "\t--\n", "us", "94043"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash("123"), Hash("--"), "us", "94043")));
  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(padded, options_),
      IsOkAndHolds(ElementsAre(Hash("123"), Hash("--"), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressNonAsciiNameOnlyAsciiLowercased) {
  // SCP's NormalizeName only lowercases ASCII characters.
  std::vector<std::string> parts = {"José", "ÉLAN", "us", "94043"};

  EXPECT_THAT(
      normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
      IsOkAndHolds(ElementsAre(Hash("josé"), Hash("Élan"), "us", "94043")));
}

TEST_F(UserDataNormalizerTest, AddressCountryAliasesCanonicalized) {
  std::vector<std::string> usa = {"", "", "USA", "94043"};
  std::vector<std::string> uk = {"", "", "uk", "SW1A 1AA"};
  std::vector<std::string> england = {"", "", " England ", "SW1A 1AA"};

  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(usa, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "us", "94043")));
  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(uk, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "gb", "sw1a1aa")));
  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(england, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "gb", "sw1a1aa")));
}

TEST_F(UserDataNormalizerTest, AddressUnknownCountryTrimmedAndLowercased) {
  std::vector<std::string> parts = {"", "", " Narnia ", "2134"};

  // Not a US address, so the zip code is not zero-padded.
  EXPECT_THAT(normalizer_.NormalizeAndHashCompositeAddress(parts, options_),
              IsOkAndHolds(ElementsAre(Hash(""), Hash(""), "narnia", "2134")));
}

TEST_F(UserDataNormalizerTest, AddressWrongPartCountFails) {
  std::vector<std::string> too_few = {"John", "Doe", "us"};
  std::vector<std::string> too_many = {"John", "Doe", "us", "94043", "extra"};

  absl::StatusOr<std::vector<std::string>> too_few_result =
      normalizer_.NormalizeAndHashCompositeAddress(too_few, options_);
  absl::StatusOr<std::vector<std::string>> too_many_result =
      normalizer_.NormalizeAndHashCompositeAddress(too_many, options_);

  EXPECT_THAT(too_few_result, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(GetBackendErrorReason(too_few_result.status()),
            Error::INVALID_MATCH_KEY_FIELD);
  EXPECT_THAT(too_many_result, StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(GetBackendErrorReason(too_many_result.status()),
            Error::INVALID_MATCH_KEY_FIELD);
}

TEST(UserDataNormalizerHasherErrorTest, HasherErrorsArePropagated) {
  MockHasher mock_hasher;
  EXPECT_CALL(mock_hasher, Base64EncodedHash(_))
      .WillRepeatedly(Return(absl::InternalError("hash failure")));
  UserDataNormalizer normalizer(&mock_hasher);
  UserDataNormalizerOptions options;
  std::vector<std::string> parts = {"John", "Doe", "US", "94043-1351"};

  EXPECT_THAT(normalizer.NormalizeAndHashEmail("john@example.com", options),
              StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(normalizer.NormalizeAndHashPhoneNumber("+16502530000",
                                                     std::nullopt, options),
              StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(normalizer.NormalizeAndHashCompositeAddress(parts, options),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(UserDataNormalizerHasherErrorTest, AddressLastNameHashErrorIsPropagated) {
  MockHasher mock_hasher;
  EXPECT_CALL(mock_hasher, Base64EncodedHash(_))
      .WillOnce(Return("first_name_hash"))
      .WillOnce(Return(absl::InternalError("hash failure")));
  UserDataNormalizer normalizer(&mock_hasher);
  UserDataNormalizerOptions options;
  std::vector<std::string> parts = {"John", "Doe", "US", "94043-1351"};

  EXPECT_THAT(normalizer.NormalizeAndHashCompositeAddress(parts, options),
              StatusIs(absl::StatusCode::kInternal));
}

}  // namespace
}  // namespace google::confidential_match::match_service
