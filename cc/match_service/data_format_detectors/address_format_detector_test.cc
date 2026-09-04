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

#include "cc/match_service/data_format_detectors/address_format_detector.h"

#include <string>

#include "cc/match_service/error/error.h"
#include "gtest/gtest.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

// A valid base64 ciphertext string that exceeds the minimum encrypted PII
// length.
constexpr absl::string_view kValidEncryptedCiphertext1 =
    "AQ0bWpDaxQ4RSyEjXGazp+LtrKwSgBcHq7LXUUIRdrVKxijGxIMOMnfwZsJ3mWVV";
constexpr absl::string_view kValidEncryptedCiphertext2 =
    "CP24wPEEEmcKWwozdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbms=";

TEST(AddressFormatDetectorTest, GetMinimumEncryptedPiiLengthDefault) {
  backend::EncryptionKey default_encryption_key;
  EXPECT_EQ(GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_BASE64,
                                         default_encryption_key),
            38);
  EXPECT_EQ(
      GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE,
                                   default_encryption_key),
      38);
  EXPECT_EQ(GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_HEX,
                                         default_encryption_key),
            56);
}

TEST(AddressFormatDetectorTest, GetMinimumEncryptedPiiLengthWithWrappedKey) {
  backend::EncryptionKey wrapped_encryption_key;
  wrapped_encryption_key.mutable_wrapped_key();

  EXPECT_EQ(GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_BASE64,
                                         wrapped_encryption_key),
            38);
  EXPECT_EQ(
      GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE,
                                   wrapped_encryption_key),
      38);
  EXPECT_EQ(GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_HEX,
                                         wrapped_encryption_key),
            56);
}

TEST(AddressFormatDetectorTest,
     GetMinimumEncryptedPiiLengthWithCoordinatorKey) {
  backend::EncryptionKey coordinator_encryption_key;
  coordinator_encryption_key.mutable_coordinator_key();

  EXPECT_EQ(GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_BASE64,
                                         coordinator_encryption_key),
            64);
  EXPECT_EQ(
      GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE,
                                   coordinator_encryption_key),
      64);
  EXPECT_EQ(GetMinimumEncryptedPiiLength(backend::MATCH_KEY_ENCODING_HEX,
                                         coordinator_encryption_key),
            96);
}

backend::CompositeField CreateAddressCompositeField(
    absl::string_view country_code, absl::string_view zip_code,
    absl::string_view first_name = "John",
    absl::string_view last_name = "Doe") {
  backend::CompositeField composite_field;
  composite_field.set_type(
      backend::CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS);

  auto* fn = composite_field.add_values();
  fn->set_type(backend::FieldType::FIELD_TYPE_FIRST_NAME);
  fn->set_value(std::string(first_name));

  auto* ln = composite_field.add_values();
  ln->set_type(backend::FieldType::FIELD_TYPE_LAST_NAME);
  ln->set_value(std::string(last_name));

  auto* cc = composite_field.add_values();
  cc->set_type(backend::FieldType::FIELD_TYPE_COUNTRY_CODE);
  cc->set_value(std::string(country_code));

  auto* zc = composite_field.add_values();
  zc->set_type(backend::FieldType::FIELD_TYPE_ZIP_CODE);
  zc->set_value(std::string(zip_code));

  return composite_field;
}

TEST(AddressFormatDetectorTest, PlaintextCountryAndZipReturnsFalse) {
  backend::EncryptionKey encryption_key;
  auto result1 = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("US", "94043"),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result1.ok());
  EXPECT_FALSE(*result1);

  auto result2 = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("usa", "90210"),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result2.ok());
  EXPECT_FALSE(*result2);
}

TEST(AddressFormatDetectorTest, EncryptedCountryAndZipReturnsTrue) {
  backend::EncryptionKey encryption_key;
  auto result = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField(kValidEncryptedCiphertext1,
                                  kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(*result);
}

TEST(AddressFormatDetectorTest, CanonicalCountryCodeReturnsFalseEvenIfLong) {
  backend::EncryptionKey encryption_key;
  // First position (name/alias/alpha-3)
  auto result1 = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("United States", kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result1.ok());
  EXPECT_FALSE(*result1);

  auto result2 = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("usa", kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result2.ok());
  EXPECT_FALSE(*result2);

  // Country name longer than minimum encrypted length (52 chars > 38)
  auto result_long = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField(
          "united kingdom of great britain and northern ireland",
          kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result_long.ok());
  EXPECT_FALSE(*result_long);

  // Second position (canonical 2-letter alpha-2)
  auto result3 = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("US", kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result3.ok());
  EXPECT_FALSE(*result3);

  auto result4 = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("gb", kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result4.ok());
  EXPECT_FALSE(*result4);
}

TEST(AddressFormatDetectorTest, ShortEncryptedStringReturnsFalse) {
  backend::EncryptionKey encryption_key;
  // String is too short (< min length)
  auto result = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField("AQ0b", kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result.ok());
  EXPECT_FALSE(*result);
}

TEST(AddressFormatDetectorTest, InvalidBase64ReturnsFalse) {
  backend::EncryptionKey encryption_key;
  std::string invalid_base64 = std::string(kValidEncryptedCiphertext1) + "@@@";
  auto result = AreCountryCodeZipCodeEncrypted(
      CreateAddressCompositeField(invalid_base64, kValidEncryptedCiphertext2),
      backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_TRUE(result.ok());
  EXPECT_FALSE(*result);
}

TEST(AddressFormatDetectorTest, MissingCountryCodeOrZipCodeReturnsError) {
  backend::EncryptionKey encryption_key;
  backend::CompositeField missing_country_code;
  missing_country_code.set_type(
      backend::CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS);
  auto* zc = missing_country_code.add_values();
  zc->set_type(backend::FieldType::FIELD_TYPE_ZIP_CODE);
  zc->set_value(std::string(kValidEncryptedCiphertext2));

  auto result1 = AreCountryCodeZipCodeEncrypted(
      missing_country_code, backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_FALSE(result1.ok());
  EXPECT_EQ(GetBackendErrorReason(result1.status()),
            backend::Error::INVALID_MATCH_KEY_FIELD);

  backend::CompositeField missing_zip_code;
  missing_zip_code.set_type(
      backend::CompositeFieldType::COMPOSITE_FIELD_TYPE_ADDRESS);
  auto* cc = missing_zip_code.add_values();
  cc->set_type(backend::FieldType::FIELD_TYPE_COUNTRY_CODE);
  cc->set_value(std::string(kValidEncryptedCiphertext1));

  auto result2 = AreCountryCodeZipCodeEncrypted(
      missing_zip_code, backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_FALSE(result2.ok());
  EXPECT_EQ(GetBackendErrorReason(result2.status()),
            backend::Error::INVALID_MATCH_KEY_FIELD);
}

TEST(AddressFormatDetectorTest, NonAddressCompositeFieldReturnsError) {
  backend::EncryptionKey encryption_key;
  backend::CompositeField non_address;
  non_address.set_type(
      backend::CompositeFieldType::COMPOSITE_FIELD_TYPE_UNSPECIFIED);
  auto* cc = non_address.add_values();
  cc->set_type(backend::FieldType::FIELD_TYPE_COUNTRY_CODE);
  cc->set_value(std::string(kValidEncryptedCiphertext1));
  auto* zc = non_address.add_values();
  zc->set_type(backend::FieldType::FIELD_TYPE_ZIP_CODE);
  zc->set_value(std::string(kValidEncryptedCiphertext2));

  auto result = AreCountryCodeZipCodeEncrypted(
      non_address, backend::MATCH_KEY_ENCODING_BASE64, encryption_key);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(GetBackendErrorReason(result.status()),
            backend::Error::INVALID_MATCH_KEY_FIELD);
}

}  // namespace
}  // namespace google::confidential_match::match_service
