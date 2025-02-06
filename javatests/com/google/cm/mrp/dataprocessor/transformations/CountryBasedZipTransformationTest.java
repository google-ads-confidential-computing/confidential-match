/*
 * Copyright 2025 Google LLC
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

package com.google.cm.mrp.dataprocessor.transformations;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.dataprocessor.transformations.Transformation.TransformationException;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CountryBasedZipTransformationTest {

  private static final ImmutableList<KeyValue> US_COUNTRYCODE =
      ImmutableList.of(KeyValue.newBuilder().setKey("country_code").setStringValue("us").build());

  private static final String TEST_KEY = "testKey";

  private static final CountryBasedZipTransformation transformation =
      new CountryBasedZipTransformation();

  @Test
  public void transform_isWellFormedUS5Digit_takeUnchanged() throws Exception {
    String zip = "12345";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo(zip);
  }

  @Test
  public void transform_isWellFormedUS9DigitWithHyphen_take5() throws Exception {
    String zip = "12345-6789";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("12345");
  }

  @Test
  public void transform_isWellFormedUS9DigitNoHyphen_take5() throws Exception {
    String zip = "123456789";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("12345");
  }

  @Test
  public void transform_isUS0to4DigitsNoHyphen_take5FromPadded5() throws Exception {
    String zip = "2345";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("02345");
  }

  @Test
  public void transform_isUS0to4DigitsWithHyphen_take5FromPadded5NoHyphen() throws Exception {
    String zip = "234-5";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("02345");
  }

  @Test
  public void transform_isUS6To8DigitsNoHyphen_take5FromPadded9() throws Exception {
    String zip = "1234569";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("00123");
  }

  @Test
  public void transform_isUS6To8DigitsWithHyphen_take5FromPadded9NoHyphen() throws Exception {
    String zip = "1235-6789";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("01235");
  }

  @Test
  public void transform_isUSOver9DigitsNoHyphen_take5() throws Exception {
    String zip = "1234567890";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("12345");
  }

  @Test
  public void transform_isUSOver9DigitsWithHyphen_take5NoHyphen() throws Exception {
    String zip = "123456789-00";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("12345");
  }

  @Test
  public void transform_isUSEmptyString_takeUnchanged() throws Exception {
    String zip = "";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo(zip);
  }

  @Test
  public void transform_isUS5DigitsMultipleHyphens_take5NoHyphen() throws Exception {
    String zip = "1-2-3-4-5";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("12345");
  }

  @Test
  public void transform_isUS0to4NonNumericDigitsNoHyphen_take5FromPadded5() throws Exception {
    String zip = "aBc";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("00abc");
  }

  @Test
  public void transform_isUS5OnlyAlphabetDigitsNoHyphen_takeUnchanged() throws Exception {
    String zip = "abCDe";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo("abcde");
  }

  @Test
  public void transform_isUSAllNonAlphanumericDigits_takeUnchanged() throws Exception {
    String zip = "_&|$%^";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();

    KeyValue result = transformation.transform(input, US_COUNTRYCODE);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo(zip);
  }

  @Test
  public void transform_NonUSZipCode_ToLowercase() throws Exception {
    String test = "12345UK";
    var input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(test).build();
    var countryKv =
        ImmutableList.of(KeyValue.newBuilder().setKey("country").setStringValue("uk").build());

    var result = transformation.transform(input, countryKv);

    // No-op
    assertThat(result.getStringValue()).isEqualTo("12345uk");
  }

  @Test
  public void transform_NonUSZipCode_RemoveNonAlphanumeric() throws Exception {
    String test = "UK$%^&-123";
    var input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(test).build();
    var countryKv =
        ImmutableList.of(KeyValue.newBuilder().setKey("country").setStringValue("uk").build());

    var result = transformation.transform(input, countryKv);

    // No-op
    assertThat(result.getStringValue()).isEqualTo("uk123");
  }

  @Test
  public void transform_isNonUSAllNonAlphanumericDigits_takeUnchanged() throws Exception {
    String zip = "_&|$%^";
    KeyValue input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue(zip).build();
    var countryKv =
        ImmutableList.of(KeyValue.newBuilder().setKey("country").setStringValue("uk").build());

    KeyValue result = transformation.transform(input, countryKv);

    assertThat(result.getKey()).isEqualTo(TEST_KEY);
    assertThat(result.getStringValue()).isEqualTo(zip);
  }

  @Test
  public void transform_noStringValueInSource_fails() {
    var input = KeyValue.newBuilder().setKey(TEST_KEY).setIntValue(1).build();

    var ex =
        assertThrows(
            TransformationException.class, () -> transformation.transform(input, US_COUNTRYCODE));

    assertThat(ex.getMessage()).isEqualTo("Input does not contain string value");
  }

  @Test
  public void transform_noCountryCodeGiven_fails() {
    var input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue("12345").build();

    var ex =
        assertThrows(
            TransformationException.class, () -> transformation.transform(input, List.of()));

    assertThat(ex.getMessage()).isEqualTo("One Country code dependent KeyValue is required");
  }

  @Test
  public void transform_tooManyCountryCodeGiven_fails() {
    var input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue("12345").build();
    var countryKv = KeyValue.newBuilder().setKey("country").setStringValue("uk").build();

    var ex =
        assertThrows(
            TransformationException.class,
            () -> transformation.transform(input, List.of(countryKv, countryKv)));

    assertThat(ex.getMessage()).isEqualTo("One Country code dependent KeyValue is required");
  }

  @Test
  public void transform_noStringValueInCountryCode_fails() {
    var input = KeyValue.newBuilder().setKey(TEST_KEY).setStringValue("12345").build();
    var countryKv = KeyValue.newBuilder().setKey("country").setIntValue(1).build();

    var ex =
        assertThrows(
            TransformationException.class,
            () -> transformation.transform(input, List.of(countryKv)));

    assertThat(ex.getMessage())
        .isEqualTo("Country code dependent KeyValue does not contain string value");
  }
}
