// Copyright 2025 Google LLC
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

#include "cc/match_service/converters/match_request_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

namespace {

// Expects `options` to read the backend proto defaults of every
// UserDataNormalizerOptions field.
void ExpectUserDataNormalizerOptionsDefaults(
    const backend::UserDataNormalizerOptions& options) {
  EXPECT_FALSE(
      options.phone_number_normalizer_options().use_country_code_hint());

  EXPECT_TRUE(options.name_normalizer_options().strip_common_prefixes());
  EXPECT_FALSE(
      options.name_normalizer_options().remove_numbers_and_special_chars());

  EXPECT_TRUE(options.email_normalizer_options().strip_parts_after_plus_sign());
  EXPECT_TRUE(
      options.email_normalizer_options().strip_periods_in_facebookmail());
  EXPECT_TRUE(
      options.email_normalizer_options().replace_googlemail_with_gmail());

  const backend::ZipCodeNormalizerOptions& zip_options =
      options.zip_code_normalizer_options();
  EXPECT_FALSE(zip_options.only_trim_surrounding_whitespaces());
  EXPECT_TRUE(zip_options.to_lowercase());
  EXPECT_TRUE(zip_options.fall_back_to_unnormalized_if_empty());
  EXPECT_TRUE(zip_options.pass_country_code_for_us_zip_code_normalization());
  EXPECT_TRUE(zip_options.zero_pad_us_zip_code());
  EXPECT_TRUE(zip_options.truncate_us_zip_code());
}

}  // namespace

TEST(MatchRequestConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::MatchRequest in;
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.application(), backend::APPLICATION_UNSPECIFIED);
  EXPECT_EQ(out.data_records_size(), 0);
  EXPECT_EQ(out.match_key_format(), backend::MATCH_KEY_FORMAT_UNSPECIFIED);
  EXPECT_EQ(out.key_encoding(), backend::MATCH_KEY_ENCODING_UNSPECIFIED);
  EXPECT_FALSE(out.has_encryption_key());
  EXPECT_EQ(out.associated_data_types_size(), 0);
  EXPECT_EQ(out.metadata_size(), 0);
  EXPECT_FALSE(out.has_match_request_options());
}

TEST(MatchRequestConvertersTest, ToBackendConvertsApplication) {
  api::v1::MatchRequest in;
  in.set_application(api::v1::APPLICATION_ECL);
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.application(), backend::APPLICATION_ECL);
}

TEST(MatchRequestConvertersTest, ToBackendConvertsDataRecords) {
  api::v1::MatchRequest in;
  in.add_data_records();
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.data_records_size(), 1);
}

TEST(MatchRequestConvertersTest, ToBackendConvertsMatchKeyFormat) {
  api::v1::MatchRequest in;
  in.set_match_key_format(api::v1::MATCH_KEY_FORMAT_HASHED);
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.match_key_format(), backend::MATCH_KEY_FORMAT_HASHED);
}

TEST(MatchRequestConvertersTest, ToBackendConvertsKeyEncoding) {
  api::v1::MatchRequest in;
  in.set_key_encoding(api::v1::MATCH_KEY_ENCODING_BASE64);
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_encoding(), backend::MATCH_KEY_ENCODING_BASE64);
}

TEST(MatchRequestConvertersTest, ToBackendConvertsEncryptionKey) {
  api::v1::MatchRequest in;
  in.mutable_encryption_key();
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_encryption_key());
}

TEST(MatchRequestConvertersTest, ToBackendConvertsAssociatedDataTypes) {
  api::v1::MatchRequest in;
  in.add_associated_data_types(
      api::v1::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER);
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.associated_data_types_size(), 1);
  EXPECT_EQ(out.associated_data_types(0),
            backend::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER);
}

TEST(MatchRequestConvertersTest, ToBackendConvertsMetadata) {
  api::v1::MatchRequest in;
  in.add_metadata();
  backend::MatchRequest out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

TEST(MatchRequestConvertersTest,
     ToBackendConvertsMatchRequestOptionsAutoDetectionEnabled) {
  api::v1::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(true);
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_TRUE(out.match_request_options()
                  .enable_encrypted_plaintext_pii_auto_detection());
}

TEST(MatchRequestConvertersTest,
     ToBackendConvertsMatchRequestOptionsAutoDetectionDisabled) {
  api::v1::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(false);
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_TRUE(out.match_request_options()
                  .has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_FALSE(out.match_request_options()
                   .enable_encrypted_plaintext_pii_auto_detection());
}

TEST(MatchRequestConvertersTest, ToApiConvertsEmptyObject) {
  backend::MatchRequest in;
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.application(), api::v1::APPLICATION_UNSPECIFIED);
  EXPECT_EQ(out.data_records_size(), 0);
  EXPECT_EQ(out.match_key_format(), api::v1::MATCH_KEY_FORMAT_UNSPECIFIED);
  EXPECT_EQ(out.key_encoding(), api::v1::MATCH_KEY_ENCODING_UNSPECIFIED);
  EXPECT_FALSE(out.has_encryption_key());
  EXPECT_EQ(out.associated_data_types_size(), 0);
  EXPECT_EQ(out.metadata_size(), 0);
  EXPECT_FALSE(out.has_match_request_options());
}

TEST(MatchRequestConvertersTest, ToApiConvertsApplication) {
  backend::MatchRequest in;
  in.set_application(backend::APPLICATION_ECL);
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.application(), api::v1::APPLICATION_ECL);
}

TEST(MatchRequestConvertersTest, ToApiConvertsDataRecords) {
  backend::MatchRequest in;
  in.add_data_records();
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.data_records_size(), 1);
}

TEST(MatchRequestConvertersTest, ToApiConvertsMatchKeyFormat) {
  backend::MatchRequest in;
  in.set_match_key_format(backend::MATCH_KEY_FORMAT_HASHED);
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.match_key_format(), api::v1::MATCH_KEY_FORMAT_HASHED);
}

TEST(MatchRequestConvertersTest, ToApiConvertsKeyEncoding) {
  backend::MatchRequest in;
  in.set_key_encoding(backend::MATCH_KEY_ENCODING_BASE64);
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_encoding(), api::v1::MATCH_KEY_ENCODING_BASE64);
}

TEST(MatchRequestConvertersTest, ToApiConvertsEncryptionKey) {
  backend::MatchRequest in;
  in.mutable_encryption_key();
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_encryption_key());
}

TEST(MatchRequestConvertersTest, ToApiConvertsAssociatedDataTypes) {
  backend::MatchRequest in;
  in.add_associated_data_types(
      backend::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER);
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.associated_data_types_size(), 1);
  EXPECT_EQ(out.associated_data_types(0),
            api::v1::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER);
}

TEST(MatchRequestConvertersTest, ToApiConvertsMetadata) {
  backend::MatchRequest in;
  in.add_metadata();
  api::v1::MatchRequest out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.metadata_size(), 1);
}

TEST(MatchRequestConvertersTest,
     ToApiConvertsMatchRequestOptionsAutoDetectionEnabled) {
  backend::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(true);
  api::v1::MatchRequest out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_TRUE(out.match_request_options()
                  .enable_encrypted_plaintext_pii_auto_detection());
}

TEST(MatchRequestConvertersTest,
     ToApiConvertsMatchRequestOptionsAutoDetectionDisabled) {
  backend::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(false);
  api::v1::MatchRequest out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_TRUE(out.match_request_options()
                  .has_enable_encrypted_plaintext_pii_auto_detection());
  EXPECT_FALSE(out.match_request_options()
                   .enable_encrypted_plaintext_pii_auto_detection());
}

TEST(MatchRequestConvertersTest,
     ToBackendConvertsMatchRequestOptionsUserDataNormalizerOptions) {
  api::v1::MatchRequest in;
  in.mutable_match_request_options()
      ->mutable_user_data_normalizer_options()
      ->mutable_phone_number_normalizer_options()
      ->set_use_country_code_hint(true);
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.match_request_options().has_user_data_normalizer_options());
  EXPECT_TRUE(out.match_request_options()
                  .user_data_normalizer_options()
                  .phone_number_normalizer_options()
                  .use_country_code_hint());
}

TEST(MatchRequestConvertersTest,
     ToApiConvertsMatchRequestOptionsUserDataNormalizerOptions) {
  backend::MatchRequest in;
  in.mutable_match_request_options()
      ->mutable_user_data_normalizer_options()
      ->mutable_phone_number_normalizer_options()
      ->set_use_country_code_hint(true);
  api::v1::MatchRequest out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  ASSERT_TRUE(out.match_request_options().has_user_data_normalizer_options());
  EXPECT_TRUE(out.match_request_options()
                  .user_data_normalizer_options()
                  .phone_number_normalizer_options()
                  .use_country_code_hint());
}

TEST(MatchRequestConvertersTest,
     ToBackendConvertsMatchRequestOptionsRegionalGroup) {
  api::v1::MatchRequest in;
  in.mutable_match_request_options()->set_regional_group("eu");
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.match_request_options().has_regional_group());
  EXPECT_EQ(out.match_request_options().regional_group(), "eu");
}

TEST(MatchRequestConvertersTest,
     ToApiConvertsMatchRequestOptionsRegionalGroup) {
  backend::MatchRequest in;
  in.mutable_match_request_options()->set_regional_group("eu");
  api::v1::MatchRequest out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  ASSERT_TRUE(out.match_request_options().has_regional_group());
  EXPECT_EQ(out.match_request_options().regional_group(), "eu");
}

TEST(MatchRequestConvertersTest,
     ToBackendWithoutRegionalGroupLeavesRegionalGroupUnset) {
  api::v1::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(true);
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_FALSE(out.match_request_options().has_regional_group());
}

TEST(MatchRequestConvertersTest,
     ToApiWithoutRegionalGroupLeavesRegionalGroupUnset) {
  backend::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(true);
  api::v1::MatchRequest out;

  EXPECT_THAT(ToApi(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_FALSE(out.match_request_options().has_regional_group());
}

TEST(MatchRequestConvertersTest,
     ToBackendWithoutMatchRequestOptionsReadsNormalizerDefaults) {
  // match_request_options is not set at all.
  api::v1::MatchRequest in;
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_FALSE(out.has_match_request_options());
  ExpectUserDataNormalizerOptionsDefaults(
      out.match_request_options().user_data_normalizer_options());
}

TEST(MatchRequestConvertersTest,
     ToBackendWithoutUserDataNormalizerOptionsReadsNormalizerDefaults) {
  // match_request_options is set, but user_data_normalizer_options is not.
  api::v1::MatchRequest in;
  in.mutable_match_request_options()
      ->set_enable_encrypted_plaintext_pii_auto_detection(true);
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  EXPECT_FALSE(out.match_request_options().has_user_data_normalizer_options());
  ExpectUserDataNormalizerOptionsDefaults(
      out.match_request_options().user_data_normalizer_options());
}

TEST(MatchRequestConvertersTest,
     ToBackendEmptyUserDataNormalizerOptionsReadsNormalizerDefaults) {
  // user_data_normalizer_options is set, but none of its fields are.
  api::v1::MatchRequest in;
  in.mutable_match_request_options()->mutable_user_data_normalizer_options();
  backend::MatchRequest out;

  EXPECT_THAT(ToBackend(in, out), IsOk());
  ASSERT_TRUE(out.has_match_request_options());
  ASSERT_TRUE(out.match_request_options().has_user_data_normalizer_options());
  ExpectUserDataNormalizerOptionsDefaults(
      out.match_request_options().user_data_normalizer_options());
}

}  // namespace google::confidential_match::match_service
