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

package com.google.cm.mrp.dataprocessor.preparers;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnFormat;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatter;
import com.google.cm.mrp.dataprocessor.formatters.EnhancedMatchMapperTest.Hashes;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.util.Objects;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class NestedDataSourcePreparerTest {
  private static final Hashes EMAIL_HASH = new Hashes("fake.email@google.com");
  private static final Hashes PHONE_NUMBER_HASH = new Hashes("+1-999-999-9999");
  private static final Hashes FIRST_NAME_HASH = new Hashes("Lorem");
  private static final Hashes LAST_NAME_HASH = new Hashes("Ipsum");
  private static final String POSTAL_CODE = "12345";
  private static final String COUNTRY = "us";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataSourceFormatter mockDataSourceFormatter;

  private DataSourcePreparer dataSourcePreparer;
  private Schema expectedSchema;
  private Schema undefinedColumnFormatSchema;

  @Before
  public void setUp() throws Exception {
    dataSourcePreparer =
        new NestedDataSourcePreparer(mockDataSourceFormatter, SuccessMode.ONLY_COMPLETE_SUCCESS);
    expectedSchema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_parsed_schema.json")),
                UTF_8),
            Schema.class);
    undefinedColumnFormatSchema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_undefined_column_format_schema.json")),
                UTF_8),
            Schema.class);
  }

  @Test
  public void prepare_noNestedColumn_returnsOriginal() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "us"},
    };
    Schema schema = getSchema(testData, Optional.empty());
    DataRecord dataRecord = getDataRecord(testData);
    DataChunk inputChunk =
        DataChunk.builder().setSchema(schema).setRecords(ImmutableList.of(dataRecord)).build();

    DataChunk outputChunk = dataSourcePreparer.prepare(inputChunk);

    assertEquals(outputChunk, inputChunk);
  }

  @Test
  public void prepare_oneNestedColumnWithoutEncryption() throws Exception {
    var groupedPii =
        String.format(
            "tv.1~em.%s~pn.%s~fn0.%s~ln0.%s~pc0.%s~co0.%s",
            EMAIL_HASH.getWebHashBase64(),
            PHONE_NUMBER_HASH.getWebHashBase64(),
            FIRST_NAME_HASH.getWebHashBase64(),
            LAST_NAME_HASH.getWebHashBase64(),
            POSTAL_CODE,
            COUNTRY);
    String[][] inputData = {
      {"gtag_grouped_pii", "gtag_grouped_pii", groupedPii},
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"}
    };
    DataRecord inputRecord = getDataRecord(inputData);
    Schema schemaWithoutEncryption =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_unencrypted_grouped_pii_schema.json")),
                UTF_8),
            Schema.class);
    DataChunk inputChunk =
        DataChunk.builder()
            .setRecords(ImmutableList.of(inputRecord))
            .setSchema(schemaWithoutEncryption)
            .build();
    String[][] outputData = {
      {"em", "email", EMAIL_HASH.getCfmHashBase64()},
      {"pn", "phone", PHONE_NUMBER_HASH.getCfmHashBase64()},
      {"fn0", "first_name", FIRST_NAME_HASH.getCfmHashBase64()},
      {"ln0", "last_name", LAST_NAME_HASH.getCfmHashBase64()},
      {"pc0", "zip_code", POSTAL_CODE},
      {"co0", "country_code", COUNTRY},
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"},
      {"row_marker", "row_marker", "fake_row_marker"},
    };
    DataRecord expectedRecord = getDataRecord(outputData);
    when(mockDataSourceFormatter.format(any())).thenReturn(ImmutableList.of(expectedRecord));
    when(mockDataSourceFormatter.getFormattedSchema()).thenReturn((expectedSchema));
    when(mockDataSourceFormatter.getDataRecordEncryptionColumns()).thenReturn((Optional.empty()));

    DataChunk actual = dataSourcePreparer.prepare(inputChunk);

    verify(mockDataSourceFormatter, times(1)).format(any());
    verify(mockDataSourceFormatter, times(1)).getFormattedSchema();
    verify(mockDataSourceFormatter, times(1)).getDataRecordEncryptionColumns();
    assertEquals(inputChunk.encryptionColumns(), actual.encryptionColumns());
    assertEquals(expectedRecord, actual.records().get(0));
    assertThat(actual.records()).hasSize(1);
    assertEquals(expectedSchema, actual.schema());
    assertEquals(schemaWithoutEncryption, actual.inputSchema().get());
  }

  @Test
  public void prepare_inputDataRecordWithErrorCode() throws Exception {
    Schema schemaWithoutEncryption =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_unencrypted_grouped_pii_schema.json")),
                UTF_8),
            Schema.class);
    var groupedPii =
        String.format(
            "abc.%s~pn.%s~fn0.%s~ln0.%s~pc0.%s~co0.%s",
            EMAIL_HASH.getWebHashBase64(),
            PHONE_NUMBER_HASH.getWebHashBase64(),
            FIRST_NAME_HASH.getWebHashBase64(),
            LAST_NAME_HASH.getWebHashBase64(),
            POSTAL_CODE,
            COUNTRY);
    String[][] inputData = {
      {"gtag_grouped_pii", "gtag_grouped_pii", groupedPii},
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"}
    };
    DataRecord inputRecord =
        getDataRecord(inputData).toBuilder().setErrorCode(JobResultCode.DECRYPTION_ERROR).build();
    DataChunk inputChunk =
        DataChunk.builder()
            .setRecords(ImmutableList.of(inputRecord))
            .setSchema(schemaWithoutEncryption)
            .build();
    String[][] outputData = {
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"},
      {"row_marker", "row_marker", "fake_row_marker"},
    };

    DataRecord expectedRecord =
        getDataRecord(outputData).toBuilder().setErrorCode(JobResultCode.DECRYPTION_ERROR).build();
    when(mockDataSourceFormatter.format(any())).thenReturn(ImmutableList.of(expectedRecord));
    when(mockDataSourceFormatter.getFormattedSchema()).thenReturn((expectedSchema));

    DataChunk actual = dataSourcePreparer.prepare(inputChunk);

    assertEquals(expectedRecord, actual.records().get(0));
    assertThat(actual.records()).hasSize(1);
    assertEquals(expectedSchema, actual.schema());
    assertEquals(schemaWithoutEncryption, actual.inputSchema().get());
  }

  @Test
  public void prepare_invalidGroupedPIIOnlyCompleteSuccess() throws Exception {
    Schema schemaWithoutEncryption =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_unencrypted_grouped_pii_schema.json")),
                UTF_8),
            Schema.class);
    var groupedPii =
        String.format(
            "abc.%s~pn.%s~fn0.%s~ln0.%s~pc0.%s~co0.%s",
            EMAIL_HASH.getWebHashBase64(),
            PHONE_NUMBER_HASH.getWebHashBase64(),
            FIRST_NAME_HASH.getWebHashBase64(),
            LAST_NAME_HASH.getWebHashBase64(),
            POSTAL_CODE,
            COUNTRY);
    String[][] inputData = {
      {"gtag_grouped_pii", "gtag_grouped_pii", groupedPii},
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"}
    };
    DataRecord inputRecord = getDataRecord(inputData);
    DataChunk inputChunk =
        DataChunk.builder()
            .setRecords(ImmutableList.of(inputRecord))
            .setSchema(schemaWithoutEncryption)
            .build();

    assertThrows(JobProcessorException.class, () -> dataSourcePreparer.prepare(inputChunk));
  }

  @Test
  public void prepare_invalidGroupedPIIAllowPartialSuccess() throws Exception {
    dataSourcePreparer =
        new NestedDataSourcePreparer(mockDataSourceFormatter, SuccessMode.ALLOW_PARTIAL_SUCCESS);
    Schema schemaWithoutEncryption =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_unencrypted_grouped_pii_schema.json")),
                UTF_8),
            Schema.class);
    // Invalid tag string version
    var groupedPii =
        String.format(
            "tv.78623187263~em.%s~pn.%s~fn0.%s~ln0.%s~pc0.%s~co0.%s",
            EMAIL_HASH.getWebHashBase64(),
            PHONE_NUMBER_HASH.getWebHashBase64(),
            FIRST_NAME_HASH.getWebHashBase64(),
            LAST_NAME_HASH.getWebHashBase64(),
            POSTAL_CODE,
            COUNTRY);
    String[][] inputData = {
      {"gtag_grouped_pii", "gtag_grouped_pii", groupedPii},
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"}
    };

    // Just add some DataRecord to return
    DataRecord expectedRecord =
        getDataRecord(inputData).toBuilder()
            .setErrorCode(JobResultCode.NESTED_COLUMN_PARSING_ERROR)
            .build();
    when(mockDataSourceFormatter.format(any())).thenReturn(ImmutableList.of(expectedRecord));
    when(mockDataSourceFormatter.getFormattedSchema()).thenReturn((expectedSchema));
    when(mockDataSourceFormatter.getDataRecordEncryptionColumns()).thenReturn((Optional.empty()));

    DataRecord inputRecord = getDataRecord(inputData);
    DataChunk inputChunk =
        DataChunk.builder()
            .setRecords(ImmutableList.of(inputRecord))
            .setSchema(schemaWithoutEncryption)
            .build();

    DataChunk updatedChunk = dataSourcePreparer.prepare(inputChunk);

    assertEquals(
        JobResultCode.NESTED_COLUMN_PARSING_ERROR, updatedChunk.records().get(0).getErrorCode());
    assertEquals(expectedSchema, updatedChunk.schema());
    verify(mockDataSourceFormatter, times(1)).format(any());
  }

  @Test
  public void prepare_throwErrorWhenSchemaUndefinedColumnFormat() {
    var groupedPii =
        String.format(
            "tv.1~em.%s~pn.%s~fn0.%s~ln0.%s~pc0.%s~co0.%s",
            EMAIL_HASH.getWebHashBase64(),
            PHONE_NUMBER_HASH.getWebHashBase64(),
            FIRST_NAME_HASH.getWebHashBase64(),
            LAST_NAME_HASH.getWebHashBase64(),
            POSTAL_CODE,
            COUNTRY);
    String[][] inputData = {
      {"gtag_grouped_pii", "gtag_grouped_pii", groupedPii},
      {"coordinator_key_id", "coordinator_key_id", "fake_key"},
      {"row_metadata", "row_metadata", "fake_metadata"}
    };
    DataRecord inputRecord = getDataRecord(inputData);
    DataChunk inputChunk =
        DataChunk.builder()
            .setRecords(ImmutableList.of(inputRecord))
            .setSchema(undefinedColumnFormatSchema)
            .build();

    var ex =
        assertThrows(JobProcessorException.class, () -> dataSourcePreparer.prepare(inputChunk));
    assertThat(ex.getMessage()).isEqualTo("Undefined column format for the nested schema column");
  }

  private static DataRecordProto.DataRecord getDataRecord(String[][] keyValueQuads) {
    return getDataRecord(keyValueQuads, new String[0][0]);
  }

  private static DataRecordProto.DataRecord getDataRecord(
      String[][] keyValueQuads, String[][] encryptedKeyValues) {
    DataRecordProto.DataRecord.Builder builder = DataRecordProto.DataRecord.newBuilder();
    for (int i = 0; i < keyValueQuads.length; ++i) {
      builder.addKeyValues(
          DataRecordProto.DataRecord.KeyValue.newBuilder()
              .setKey(keyValueQuads[i][0])
              .setStringValue(keyValueQuads[i][2]));
      if (encryptedKeyValues.length > i && encryptedKeyValues[i].length >= 1) {
        builder.putEncryptedKeyValues(i, encryptedKeyValues[i][1]);
      }
    }
    return builder.build();
  }

  private static Schema getSchema(String[][] keyValueQuads, Optional<Schema> nestedSchema) {
    Schema.Builder builder = Schema.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      Column.Builder column =
          Column.newBuilder().setColumnName(keyValue[0]).setColumnAlias(keyValue[1]);
      if (keyValue.length > 6) {
        column.setColumnGroup(Integer.parseInt(keyValue[6]));
      }
      if (keyValue.length > 3) {
        column.setEncrypted(Boolean.valueOf(keyValue[3]));
      }
      if (keyValue.length > 4 && Boolean.valueOf(keyValue[4])) {
        column.setNestedSchema(nestedSchema.get());
        column.setColumnFormat(ColumnFormat.valueOf(keyValue[5]));
      }

      builder.addColumns(column.build());
    }
    return builder.build();
  }
}
