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

import static com.google.common.truth.Truth.assertThat;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import org.junit.Test;

public class DataErrorMatcherTest {

  @Test
  public void matchErrors_NoErrors_ReturnsOriginal() {
    String[][] testRecord0 = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk original =
        DataChunk.builder()
            .setSchema(getSchema(testRecord0))
            .addRecord(getDataRecord(testRecord0))
            .build();
    DataChunk errorChunk = DataChunk.builder().setSchema(getSchema(testRecord0)).build();

    DataChunk result = DataErrorMatcher.matchErrors(original, errorChunk);

    assertThat(result).isEqualTo(original);
  }

  @Test
  public void matchErrors_success() {
    String[][] testRecord0 = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    String[][] testRecord1 = {
      {"email", "email", "fake.email1@google.com"},
      {"phone", "phone", "199-999-9999"},
      {"first_name", "first_name", "fake_first_name1"},
      {"last_name", "last_name", "fake_last_name1"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk original =
        DataChunk.builder()
            .setSchema(getSchema(testRecord0))
            .addRecord(getDataRecord(testRecord0))
            .addRecord(getDataRecord(testRecord1))
            .build();

    String[][] errorRecordData = {
      {"pii_value", "pii_value", "fake.email@google.com"},
    };
    DataRecord errorRecord =
        getDataRecord(errorRecordData).toBuilder()
            .setErrorCode(JobResultCode.DECRYPTION_ERROR)
            .build();
    DataChunk errorChunk =
        DataChunk.builder().setSchema(getSchema(errorRecordData)).addRecord(errorRecord).addRecord(errorRecord).build();

    DataChunk result = DataErrorMatcher.matchErrors(original, errorChunk);

    assertThat(result.schema()).isEqualTo(original.schema());
    assertThat(result.encryptionColumns()).isEqualTo(original.encryptionColumns());
    assertThat(result.inputSchema()).isEqualTo(original.inputSchema());
    assertThat(result.records()).hasSize(original.records().size());
    DataRecord newDataRecord = result.records().get(0);
    assertThat(newDataRecord.getKeyValuesList())
        .containsExactlyElementsIn(original.records().get(0).getKeyValuesList());
    assertThat(newDataRecord.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);
    assertThat(result.records().get(1)).isEqualTo(original.records().get(1));
  }

  @Test
  public void matchErrors_duplicate_success() {
    String[][] testRecord0 = {
        {"email", "email", "fake.email@google.com"},
        {"phone", "phone", "999-999-9999"},
        {"first_name", "first_name", "fake_first_name"},
        {"last_name", "last_name", "fake_last_name"},
        {"zip_code", "zip_code", "99999"},
        {"country_code", "country_code", "US"},
    };
    String[][] testRecord1 = {
        {"email", "email", "fake.email1@google.com"},
        {"phone", "phone", "199-999-9999"},
        {"first_name", "first_name", "fake_first_name1"},
        {"last_name", "last_name", "fake_last_name1"},
        {"zip_code", "zip_code", "99999"},
        {"country_code", "country_code", "US"},
    };
    DataChunk original =
        DataChunk.builder()
            .setSchema(getSchema(testRecord0))
            .addRecord(getDataRecord(testRecord0))
            .addRecord(getDataRecord(testRecord1))
            .build();

    String[][] errorRecordData = {
        {"pii_value", "pii_value", "fake.email@google.com"},
    };
    DataRecord errorRecord =
        getDataRecord(errorRecordData).toBuilder()
            .setErrorCode(JobResultCode.DECRYPTION_ERROR)
            .build();
    DataChunk errorChunk =
        DataChunk.builder().setSchema(getSchema(errorRecordData)).addRecord(errorRecord).addRecord(errorRecord).build();

    DataChunk result = DataErrorMatcher.matchErrors(original, errorChunk);

    assertThat(result.schema()).isEqualTo(original.schema());
    assertThat(result.encryptionColumns()).isEqualTo(original.encryptionColumns());
    assertThat(result.inputSchema()).isEqualTo(original.inputSchema());
    assertThat(result.records()).hasSize(original.records().size());
    DataRecord newDataRecord = result.records().get(0);
    assertThat(newDataRecord.getKeyValuesList())
        .containsExactlyElementsIn(original.records().get(0).getKeyValuesList());
    assertThat(newDataRecord.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);
    assertThat(result.records().get(1)).isEqualTo(original.records().get(1));
  }

  private Schema getSchema(String[][] keyValueQuads) {
    Schema.Builder builder = Schema.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addColumns(
          Column.newBuilder()
              .setColumnName(keyValue[0])
              .setColumnAlias(keyValue[1])
              .setColumnGroup(keyValue.length > 3 ? Integer.parseInt(keyValue[3]) : 0)
              .build());
    }
    return builder.build();
  }

  private DataRecord getDataRecord(String[][] keyValueQuads) {
    DataRecord.Builder builder = DataRecord.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addKeyValues(KeyValue.newBuilder().setKey(keyValue[0]).setStringValue(keyValue[2]));
    }
    return builder.build();
  }
}
