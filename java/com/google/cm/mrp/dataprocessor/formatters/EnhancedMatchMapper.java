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

package com.google.cm.mrp.dataprocessor.formatters;

import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.io.BaseEncoding;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Contains utilities for parsing the tagging Enhanced Match parameter value and validating the
 * schema for tagging Enhanced Match.
 */
public final class EnhancedMatchMapper {

  // Prevents processing of unreasonably large inputs.
  private static final int MAX_INPUT_LENGTH = 20000;
  private static final String TAG_VERSION_KEY = "tv";
  private static final String EMAIL_KEY = "em";
  private static final String PHONE_NUMBER_KEY = "pn";
  private static final String FIRST_NAME_KEY = "fn";
  private static final String LAST_NAME_KEY = "ln";
  private static final String POSTAL_CODE_KEY = "pc";
  private static final String COUNTRY_KEY = "co";
  private static final String ERROR_CODE_IN_KEY = "ec";
  private static final String ERROR_CODES_OUT_KEY = "error_codes";
  private static final Pattern ADDRESS_PATTERN =
      Pattern.compile(
          String.format(
              "^(?:%s|%s|%s|%s)(\\d{1,2})$",
              Field.FIRST_NAME.getInKey(),
              Field.LAST_NAME.getInKey(),
              Field.POSTAL_CODE.getInKey(),
              Field.COUNTRY.getInKey()));

  /**
   * Parses the GTAG encoded input into a data record, extracting only the following fields:
   *
   * <ul>
   *   <li>email
   * </ul>
   *
   * The input must be a non-null string, 20000 characters or fewer, representing an enhanced match
   * key-value pair string. Throws an {@link IllegalArgumentException} on invalid input or if the
   * string does not specify tag version 1.
   *
   * <p>Unrecognized keys are ignored.
   *
   * <p>Fields with an error code value are skipped, and the error code is added to the
   * `error_codes` output field. Hashed fields are normalized to standard base64 encoding. If the
   * normalization fails, the value is passed through unchanged.
   *
   * <p>Example input: "tv.1~em.<base64_hash>"
   */
  public static DataRecord mapGtagToDataRecord(String encodedValue)
      throws IllegalArgumentException {
    validateInput(encodedValue);

    var addressIndexes = new ArrayList<String>();
    ImmutableListMultimap<String, String> entries =
        Arrays.stream(encodedValue.split("[~]"))
            .map(Entry::toKeyValue)
            .filter(Entry::notNull)
            .map(
                (entry) -> {
                  Matcher addressMatcher = ADDRESS_PATTERN.matcher(entry.getKey());
                  if (addressMatcher.matches()) {
                    addressIndexes.add(addressMatcher.group(1));
                  }
                  return entry;
                })
            .collect(toImmutableListMultimap(Entry::getKey, Entry::getValue));
    int maxAddressIndex = addressIndexes.stream().mapToInt(Integer::parseInt).max().orElse(-1);

    var errors = new ArrayList<Entry>();
    Consumer<Entry> reportError = errors::add;

    validateInput(entries, reportError);

    var dataRecordBuilder = DataRecord.newBuilder();
    createSingleFields(entries, reportError, Field.EMAIL).stream()
        .forEach(dataRecordBuilder::addKeyValues);
    createSingleFields(entries, reportError, Field.PHONE_NUMBER).stream()
        .forEach(dataRecordBuilder::addKeyValues);
    createAddress(entries, maxAddressIndex, reportError).stream()
        .forEach(dataRecordBuilder::addKeyValues);
    if (errors.size() > 0) {
      dataRecordBuilder.addKeyValues(createErrorCodesField(errors));
    }

    return dataRecordBuilder.build();
  }

  /**
   * Throws an {@link IllegalArgumentException} if input is null or greater than 1000 characters.
   */
  private static void validateInput(String encodedValue) {
    if (encodedValue == null) {
      throw new IllegalArgumentException("Input must not be null.");
    }
    if (encodedValue.length() > MAX_INPUT_LENGTH) {
      throw new IllegalArgumentException(
          String.format("Input may not exceed %s characters.", MAX_INPUT_LENGTH));
    }
  }

  /**
   * Throws an {@link IllegalArgumentException} if entries does not contain tag version 1. Reports
   * an error if the input contains an error code.
   */
  private static void validateInput(
      ImmutableListMultimap<String, String> entries, Consumer<Entry> reportError) {
    if (!(entries.containsKey(TAG_VERSION_KEY)
        && entries.get(TAG_VERSION_KEY).size() == 1
        && entries.get(TAG_VERSION_KEY).get(0).equals("1"))) {
      throw new IllegalArgumentException("Only tag version 1 is supported.");
    }
    for (String errorCode : entries.get(ERROR_CODE_IN_KEY)) {
      reportError.accept(new Entry(ERROR_CODE_IN_KEY, errorCode));
    }
  }

  /**
   * Returns list of fields based on the provided field. If the input contains an error code, then
   * the error is reported and the field is skipped.
   */
  private static List<KeyValue.Builder> createSingleFields(
      ImmutableListMultimap<String, String> entries, Consumer<Entry> reportError, Field field) {
    return createSingleFields(entries, reportError, field, 0);
  }

  /**
   * Returns list of fields based on the provided field. If the input contains an error code, then
   * the error is reported and the field is skipped. The index is added to the output key if the
   * field indicates that the index should be included.
   */
  private static List<KeyValue.Builder> createSingleFields(
      ImmutableListMultimap<String, String> entries,
      Consumer<Entry> reportError,
      Field field,
      int index) {
    var outValues = new ArrayList<KeyValue.Builder>();
    String inKey = field.includeIndex() ? field.getInKey(index) : field.getInKey();
    var inValues = entries.get(inKey);
    for (int i = 0; i < inValues.size(); i++) {
      String value = inValues.get(i);
      if (value.matches("^e\\d{1,5}$")) {
        reportError.accept(new Entry(inKey, value));
        continue;
      }
      outValues.add(
          KeyValue.newBuilder()
              .setKey(field.includeIndex() ? field.getOutKey(index) : field.getOutKey())
              .setStringValue(
                  field.isHashed()
                      ? normalizeHash(inValues.get(i))
                      : inValues.get(i).toLowerCase()));
    }
    return outValues;
  }

  /** Returns a list of the address fields. */
  private static List<KeyValue.Builder> createAddress(
      ImmutableListMultimap<String, String> entries,
      int maxAddressIndex,
      Consumer<Entry> reportError) {
    var outValues = new ArrayList<List<KeyValue.Builder>>();
    for (int i = 0; i <= maxAddressIndex; i++) {
      outValues.add(createSingleFields(entries, reportError, Field.FIRST_NAME, i));
      outValues.add(createSingleFields(entries, reportError, Field.LAST_NAME, i));
      outValues.add(createSingleFields(entries, reportError, Field.POSTAL_CODE, i));
      outValues.add(createSingleFields(entries, reportError, Field.COUNTRY, i));
    }
    return outValues.stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  /** Returns an `error_codes` field. If there are no errors, the value is empty. */
  private static KeyValue.Builder createErrorCodesField(List<Entry> errors) {
    KeyValue.Builder errorsRow = KeyValue.newBuilder().setKey(ERROR_CODES_OUT_KEY);
    if (!errors.isEmpty()) {
      StringBuilder errorCodes = new StringBuilder();
      errors.stream().forEach(error -> errorCodes.append("~" + error.toString()));
      errorsRow.setStringValue(errorCodes.substring(1));
    }
    return errorsRow;
  }

  private static String normalizeHash(String hash) {
    if (hash.length() == 43) {
      hash += "=";
    }
    try {
      if (hash.length() == 44) {
        return BaseEncoding.base64().encode(BaseEncoding.base64Url().decode(hash));
      }
      if (hash.matches("^[0-9A-Za-z]{64}$")) {
        return BaseEncoding.base64().encode(BaseEncoding.base16().decode(hash.toUpperCase()));
      }
    } catch (IllegalArgumentException e) {
      // Bad encoding. Ignore.
    }
    return hash;
  }

  /** Represents an enhanced match field such as email or phone number. */
  private enum Field {
    EMAIL(EMAIL_KEY, EMAIL_KEY, true, false),
    PHONE_NUMBER(PHONE_NUMBER_KEY, PHONE_NUMBER_KEY, true, false),
    FIRST_NAME(FIRST_NAME_KEY, FIRST_NAME_KEY, true, true),
    LAST_NAME(LAST_NAME_KEY, LAST_NAME_KEY, true, true),
    POSTAL_CODE(POSTAL_CODE_KEY, POSTAL_CODE_KEY, false, true),
    COUNTRY(COUNTRY_KEY, COUNTRY_KEY, false, true);

    final String inKey;
    final String outKey;
    final boolean isHashed;
    final boolean includeIndex;

    Field(String inKey, String outKey, boolean isHashed, boolean includeIndex) {
      this.inKey = inKey;
      this.outKey = outKey;
      this.isHashed = isHashed;
      this.includeIndex = includeIndex;
    }

    String getInKey() {
      return inKey;
    }

    String getInKey(int index) {
      return inKey + Integer.toString(index);
    }

    String getOutKey() {
      return outKey;
    }

    String getOutKey(int index) {
      return outKey + Integer.toString(index);
    }

    boolean isHashed() {
      return isHashed;
    }

    boolean includeIndex() {
      return includeIndex;
    }
  }

  /** Holds a key-value pair for enhanced match. */
  private static class Entry {
    final String key;
    final String value;

    private Entry(String key, String value) {
      this.key = key;
      this.value = value;
    }

    String getKey() {
      return key;
    }

    String getValue() {
      return value;
    }

    public String toString() {
      return key + "." + value;
    }

    /**
     * Returns a populated {@link Entry} if the input represents a valid entry. Otherwise, returns
     * an entry populated with null for key and value.
     */
    private static Entry toKeyValue(String entry) {
      String[] tokens = entry.split("[.]");

      if (tokens.length != 2 || tokens[0].isEmpty() || tokens[1].isEmpty()) {
        return new Entry(null, null);
      }
      return new Entry(tokens[0], tokens[1]);
    }

    /** Returns true iff the key and value are not null. */
    private static boolean notNull(Entry entry) {
      return !(entry.getKey() == null || entry.getValue() == null);
    }
  }

  // Validates the nested schema, ensuring all columns have a valid alias, with the exception of the
  // error_codes column. This verification ensures that personally identifiable information (PII)
  // found within the Enhanced Match data will be successfully processed for matching purposes in
  // the MRP.
  public static boolean isValidEnhancedMatchSchema(Schema schema, MatchConfig matchConfig) {
    if (!hasValidEncryptedPiiColumns(schema)) {
      return false;
    }
    return schema.getColumnsList().stream()
        .filter(column -> !column.getColumnName().equals(ERROR_CODES_OUT_KEY))
        .map(Schema.Column::getColumnAlias)
        .allMatch(
            alias ->
                matchConfig.getMatchConditionsList().stream()
                    .flatMap(
                        matchCondition ->
                            matchCondition.getDataSource1Column().getColumnsList().stream()
                                .map(MatchConfig.Column::getColumnAlias))
                    .anyMatch(matchColumn -> matchColumn.equalsIgnoreCase(alias)));
  }

  // Validates the schema has valid `encrypted` flag for all columns. Pii columns should have
  // 'encrypted=true'. Only "error_codes", "pc", "co" columns have "encrypted=false".
  private static boolean hasValidEncryptedPiiColumns(Schema schema) {
    return schema.getColumnsList().stream()
        .filter(column -> !column.getEncrypted())
        .map(Schema.Column::getColumnName)
        .allMatch(
            name ->
                name.equals(ERROR_CODES_OUT_KEY)
                    || name.equals(POSTAL_CODE_KEY)
                    || name.equals(COUNTRY_KEY));
  }
}
