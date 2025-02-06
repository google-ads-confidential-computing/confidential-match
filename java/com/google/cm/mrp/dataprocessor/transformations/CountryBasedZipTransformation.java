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

import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Transformation for zipcodes dependent on country code */
public class CountryBasedZipTransformation implements Transformation {

  private static final String US_COUNTRY_CODE = "us";
  private static final int ZIPCODE_SIZE = 5;
  private static final int LONG_ZIPCODE_SIZE = 9;
  private static final Pattern NON_ALPHANUMERIC_PATTERN = Pattern.compile("[^a-zA-Z0-9]");

  /**
   * Performs the following transformations in order:
   *
   * <p>For US-based Zipcodes
   *
   * <ul>
   *   <li>Removes non alphanumeric characters from zipcodes *
   *   <li>Lowercases all zipcodes
   *   <li>Adds zero-padding 5-digit zip codes.
   *   <li>Converts 9-digit zip codes to 5-digit zip codes.
   * </ul>
   *
   * <p>* For non US-based Zipcodes
   *
   * <ul>
   *   <li>Removes non alphanumeric characters from zipcodes
   *   <li>Lowercases all zipcodes
   * </ul>
   *
   * @param sourceKeyValue The zipCode to be transformed.
   * @param dependentKeyValues The countryCode that affects the zipCode transformation. Fails if
   *     more than one is included. CountryCode must be lowercase.
   * @return The transformed zipCode keyValue.
   * @throws TransformationException on any validation error
   */
  @Override
  public KeyValue transform(KeyValue sourceKeyValue, List<KeyValue> dependentKeyValues)
      throws TransformationException {
    if (!sourceKeyValue.hasStringValue()) {
      throw new TransformationException("Input does not contain string value");
    }
    if (dependentKeyValues.size() != 1) {
      throw new TransformationException("One Country code dependent KeyValue is required");
    }
    KeyValue countryKeyValue = dependentKeyValues.get(0);
    if (!countryKeyValue.hasStringValue()) {
      throw new TransformationException(
          "Country code dependent KeyValue does not contain string value");
    }
    // Remove non-alphanumeric characters
    String alphanumeric =
        NON_ALPHANUMERIC_PATTERN.matcher(sourceKeyValue.getStringValue()).replaceAll("");
    if (alphanumeric.isEmpty()) {
      return sourceKeyValue;
    }
    // Set to lowercase
    String lowercase = alphanumeric.toLowerCase(Locale.US);

    if (US_COUNTRY_CODE.equalsIgnoreCase(countryKeyValue.getStringValue())) {
      // For length < 5, assume a 5-digit zip. Pad zeroes up to 5 digits
      // For length > 5, assume a 9-digit zip. Pad zeroes up to 9 digits, then take 5 digits
      // For length == 5, do nothing. Methods exit quickly when there is no work to do.
      int assumedSize = lowercase.length() <= ZIPCODE_SIZE ? ZIPCODE_SIZE : LONG_ZIPCODE_SIZE;
      String result = Strings.padStart(lowercase, assumedSize, '0').substring(0, ZIPCODE_SIZE);

      return sourceKeyValue.toBuilder().setStringValue(result).build();
    } else {
      return sourceKeyValue.toBuilder()
          // Set lowercase
          .setStringValue(lowercase)
          .build();
    }
  }
}
