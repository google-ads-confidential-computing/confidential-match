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

#include "absl/status/status.h"
#include "gtest/gtest.h"

namespace google::confidential_match::match_service {
namespace {

TEST(NormalizationUtilsTest, NormalizeCountryCode) {
  EXPECT_EQ(NormalizeCountryCode("US"), "us");
  EXPECT_EQ(NormalizeCountryCode("  uS  "), "us");
  EXPECT_EQ(NormalizeCountryCode("uk"), "uk");
}

TEST(NormalizationUtilsTest, NormalizeZipCodeGeneral) {
  // Non-US normalization (just lowercase and alphanumeric)
  EXPECT_EQ(NormalizeZipCode("12345UK", "uk"), "12345uk");
  EXPECT_EQ(NormalizeZipCode("UK$%^&-123", "uk"), "uk123");
  // If it becomes empty, return original
  EXPECT_EQ(NormalizeZipCode("_&|$%^", "uk"), "_&|$%^");
}

TEST(NormalizationUtilsTest, NormalizeZipCodeUS) {
  // US 5-digit unchanged
  EXPECT_EQ(NormalizeZipCode("12345", "US"), "12345");
  // US 5-digit with hyphen
  EXPECT_EQ(NormalizeZipCode("123-45", "US"), "12345");
  // US padding 0 to 4 digits
  EXPECT_EQ(NormalizeZipCode("2345", "us"), "02345");
  EXPECT_EQ(NormalizeZipCode("234-5", "US"), "02345");
  EXPECT_EQ(NormalizeZipCode("abc", "Us"), "00abc");

  // US 9-digit with hyphen -> take first 5
  EXPECT_EQ(NormalizeZipCode("12345-6789", "US"), "12345");
  // US 9-digit no hyphen -> take first 5
  EXPECT_EQ(NormalizeZipCode("123456789", "us"), "12345");

  // US 6 to 8 digits -> pad to 9, then take first 5
  // "1234569" -> "001234569" -> "00123"
  EXPECT_EQ(NormalizeZipCode("1234569", "US"), "00123");
  // "1235-6789" -> "12356789" -> "012356789" -> "01235"
  EXPECT_EQ(NormalizeZipCode("1235-6789", "US"), "01235");

  // US over 9 digits -> take first 5
  EXPECT_EQ(NormalizeZipCode("1234567890", "US"), "12345");
}

}  // namespace
}  // namespace google::confidential_match::match_service
