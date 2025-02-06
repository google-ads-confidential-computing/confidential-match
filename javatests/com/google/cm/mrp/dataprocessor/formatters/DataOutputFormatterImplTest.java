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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.backend.SchemaProto.Schema.DataFormat.CSV;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.PartialSuccessAttributes;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.util.List;
import java.util.Map;
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
public final class DataOutputFormatterImplTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataOutputCondenser mockDataOutputCondenser;
  private MatchConfig matchConfig;
  Map<String, String> compositeColumnMap; // Condenser input
  private Map<String, String> ungroupedPiiMap; // Result validation
  private List<Map<String, String>> addressGroupMaps; // Result validation
  private List<Map<String, String>> socialGroupMaps; // Result validation
  private List<Integer> indices;
  private List<DataRecord> fannedOutDataRecords;

  public DataOutputFormatterImplTest() throws Exception {}

  @Before
  public void setup() throws Exception {
    when(mockDataOutputCondenser.condense(any(), any()))
        .thenReturn(
            (KeyValue.newBuilder().setKey("pii_proto").setStringValue("condensed_value").build()));
    matchConfig =
        MatchConfig.newBuilder()
            .setSuccessConfig(
                SuccessConfig.newBuilder()
                    .setPartialSuccessAttributes(
                        PartialSuccessAttributes.newBuilder()
                            .setRecordStatusFieldName("row_status")
                            .build())
                    .build())
            .build();
  }

  @Test
  public void dataOutputFormatter_fannedOutRecords() throws Exception {
    Schema nestedInputSchema = getSchema("testdata/nested_data_output_formatter_schema.json");
    Schema fannedOutSchema =
        getSchema("testdata/fanned_out_flat_data_output_formatter_schema.json");

    var fannedOutDataRecord1 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.1@google.com"),
                getKeyValue("pn", "555-555-5551"),
                getKeyValue("fn", "John"),
                getKeyValue("ln", "Doe"),
                getKeyValue("pc", "99999"),
                getKeyValue("co", "US"),
                getKeyValue("ig", "@fake-insta"),
                getKeyValue("tt", "@fake-tiktok"),
                getKeyValue("key_id", "123"),
                getKeyValue("metadata", "metadata"),
                getKeyValue("error_codes", "12345"),
                getKeyValue("row_status", SUCCESS.name()),
                getKeyValue(ROW_MARKER_COLUMN_NAME, "1")),
            SUCCESS);
    var fannedOutDataRecord2 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.2@google.com"),
                getKeyValue("pn", "555-555-5552"),
                getKeyValue("fn", "Jane"),
                getKeyValue("ln", "Deer"),
                getKeyValue("pc", "V6Z 2H7"),
                getKeyValue("co", "CA"),
                getKeyValue("ig", ""),
                getKeyValue("tt", ""),
                getKeyValue("key_id", "123"),
                getKeyValue("metadata", ""),
                getKeyValue("error_codes", ""),
                getKeyValue("row_status", DECRYPTION_ERROR.name()),
                getKeyValue(ROW_MARKER_COLUMN_NAME, "1")),
            SUCCESS);
    var fannedOutDataRecord3 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.3@google.com"),
                getKeyValue("pn", ""),
                getKeyValue("fn", "UNMATCHED"),
                getKeyValue("ln", "UNMATCHED"),
                getKeyValue("pc", "UNMATCHED"),
                getKeyValue("co", ""),
                getKeyValue("ig", ""),
                getKeyValue("tt", ""),
                getKeyValue("key_id", "123"),
                getKeyValue("metadata", ""),
                getKeyValue("error_codes", ""),
                getKeyValue("row_status", SUCCESS.name()),
                getKeyValue(ROW_MARKER_COLUMN_NAME, "1")),
            SUCCESS);

    var fannedOutDataChunk =
        getDataChunk(
            List.of(fannedOutDataRecord1, fannedOutDataRecord2, fannedOutDataRecord3),
            nestedInputSchema,
            fannedOutSchema);

    DataOutputFormatter dataOutputFormatter =
        new DataOutputFormatterImpl(
            matchConfig, nestedInputSchema, Optional.of(mockDataOutputCondenser));
    DataChunk formattedDataChunk = dataOutputFormatter.format(fannedOutDataChunk);

    // Validate DataChunk Schema
    Schema formattedSchema = formattedDataChunk.schema();
    assertEquals(4, formattedSchema.getColumnsList().size());
    assertEquals("key_id", formattedSchema.getColumnsList().get(0).getColumnName());
    assertEquals("pii_proto", formattedSchema.getColumnsList().get(1).getColumnName());
    assertEquals("metadata", formattedSchema.getColumnsList().get(2).getColumnName());
    assertEquals("row_status", formattedSchema.getColumnsList().get(3).getColumnName());
    assertFalse(formattedSchema.getSkipHeaderRecord());
    assertEquals(CSV, formattedSchema.getDataFormat());
    assertEquals(nestedInputSchema, formattedDataChunk.inputSchema().get());

    // Validate DataChunk DataRecords
    assertEquals(1, formattedDataChunk.records().size());
    assertEquals(4, formattedDataChunk.records().get(0).getKeyValuesCount());
    assertEquals("key_id", formattedDataChunk.records().get(0).getKeyValuesList().get(0).getKey());
    assertEquals(
        "123", formattedDataChunk.records().get(0).getKeyValuesList().get(0).getStringValue());
    assertEquals(
        "pii_proto", formattedDataChunk.records().get(0).getKeyValuesList().get(1).getKey());
    assertEquals(
        "condensed_value",
        formattedDataChunk.records().get(0).getKeyValuesList().get(1).getStringValue());
    assertEquals(
        "metadata", formattedDataChunk.records().get(0).getKeyValuesList().get(2).getKey());
    assertEquals(
        "metadata", formattedDataChunk.records().get(0).getKeyValuesList().get(2).getStringValue());
    assertEquals(
        "row_status", formattedDataChunk.records().get(0).getKeyValuesList().get(3).getKey());
    assertEquals(
        DECRYPTION_ERROR.name(),
        formattedDataChunk.records().get(0).getKeyValuesList().get(3).getStringValue());
    assertEquals(SUCCESS, formattedDataChunk.records().get(0).getErrorCode());
  }

  @Test
  public void dataOutputFormatter_condensedRecords() throws Exception {
    Schema flatSchema =
        getSchema("testdata/flat_input_with_condensing_data_output_formatter_schema.json");
    var condensingNeededDataRecord1 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.1@google.com"),
                getKeyValue("pn", "555-555-5551"),
                getKeyValue("fn", "John"),
                getKeyValue("ln", "Doe"),
                getKeyValue("pc", "99999"),
                getKeyValue("co", "US"),
                getKeyValue("ig", "@fake-insta"),
                getKeyValue("tt", "@fake-tiktok"),
                getKeyValue("key_id", "123"),
                getKeyValue("metadata", "metadata"),
                getKeyValue("error_codes", "12345")),
            SUCCESS);
    var condensingNeededDataRecord2 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.2@google.com"),
                getKeyValue("pn", "555-555-5552"),
                getKeyValue("fn", "Jane"),
                getKeyValue("ln", "Deer"),
                getKeyValue("pc", "V6Z 2H7"),
                getKeyValue("co", "CA"),
                getKeyValue("ig", ""),
                getKeyValue("tt", ""),
                getKeyValue("key_id", "321"),
                getKeyValue("metadata", ""),
                getKeyValue("error_codes", "54321")),
            PARTIAL_SUCCESS);

    var condensingNeededDataChunk =
        getDataChunk(
            List.of(condensingNeededDataRecord1, condensingNeededDataRecord2),
            flatSchema,
            flatSchema);

    DataOutputFormatter dataOutputFormatter =
        new DataOutputFormatterImpl(matchConfig, flatSchema, Optional.of(mockDataOutputCondenser));
    DataChunk formattedDataChunk = dataOutputFormatter.format(condensingNeededDataChunk);

    // Validate DataChunk Schema
    Schema formattedSchema = formattedDataChunk.schema();
    assertEquals(3, formattedSchema.getColumnsList().size());
    assertEquals("key_id", formattedSchema.getColumnsList().get(0).getColumnName());
    assertEquals("pii_proto", formattedSchema.getColumnsList().get(1).getColumnName());
    assertEquals("metadata", formattedSchema.getColumnsList().get(2).getColumnName());
    assertFalse(formattedSchema.getSkipHeaderRecord());
    assertEquals(CSV, formattedSchema.getDataFormat());
    assertEquals(flatSchema, formattedDataChunk.inputSchema().get());

    // Validate DataChunk DataRecords
    assertEquals(2, formattedDataChunk.records().size());
    assertEquals(3, formattedDataChunk.records().get(0).getKeyValuesCount());
    assertEquals("key_id", formattedDataChunk.records().get(0).getKeyValuesList().get(0).getKey());
    assertEquals(
        "123", formattedDataChunk.records().get(0).getKeyValuesList().get(0).getStringValue());
    assertEquals(
        "pii_proto", formattedDataChunk.records().get(0).getKeyValuesList().get(1).getKey());
    assertEquals(
        "condensed_value",
        formattedDataChunk.records().get(0).getKeyValuesList().get(1).getStringValue());
    assertEquals(
        "metadata", formattedDataChunk.records().get(0).getKeyValuesList().get(2).getKey());
    assertEquals(
        "metadata", formattedDataChunk.records().get(0).getKeyValuesList().get(2).getStringValue());
    assertEquals(2, formattedDataChunk.records().size());
    assertEquals(SUCCESS, formattedDataChunk.records().get(0).getErrorCode());
    assertEquals(3, formattedDataChunk.records().get(1).getKeyValuesCount());
    assertEquals("key_id", formattedDataChunk.records().get(1).getKeyValuesList().get(0).getKey());
    assertEquals(
        "321", formattedDataChunk.records().get(1).getKeyValuesList().get(0).getStringValue());
    assertEquals(
        "pii_proto", formattedDataChunk.records().get(1).getKeyValuesList().get(1).getKey());
    assertEquals(
        "condensed_value",
        formattedDataChunk.records().get(1).getKeyValuesList().get(1).getStringValue());
    assertEquals(
        "metadata", formattedDataChunk.records().get(1).getKeyValuesList().get(2).getKey());
    assertEquals(
        "", formattedDataChunk.records().get(1).getKeyValuesList().get(2).getStringValue());
    assertEquals(PARTIAL_SUCCESS, formattedDataChunk.records().get(1).getErrorCode());
  }

  @Test
  public void dataOutputFormatter_orderedRecords() throws Exception {
    Schema flatSchema = getSchema("testdata/flat_data_output_formatter_schema.json");
    var orderingNeededDataRecord1 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.1@google.com"),
                getKeyValue("pn", "555-555-5551"),
                getKeyValue("fn", "John"),
                getKeyValue("ln", "Doe"),
                getKeyValue("pc", "99999"),
                getKeyValue("co", "US"),
                getKeyValue("ig", "@fake-insta"),
                getKeyValue("tt", "@fake-tiktok"),
                getKeyValue("key_id", "123"),
                getKeyValue("metadata", "metadata"),
                getKeyValue("error_codes", "12345")),
            SUCCESS);
    var orderingNeededDataRecord2 =
        getDataRecord(
            List.of(
                getKeyValue("em", "FAKE.2@google.com"),
                getKeyValue("pn", "555-555-5552"),
                getKeyValue("fn", "Jane"),
                getKeyValue("ln", "Deer"),
                getKeyValue("pc", "V6Z 2H7"),
                getKeyValue("co", "CA"),
                getKeyValue("ig", ""),
                getKeyValue("tt", ""),
                getKeyValue("key_id", "321"),
                getKeyValue("metadata", ""),
                getKeyValue("error_codes", "54321")),
            PARTIAL_SUCCESS);
    var orderingNeededDataChunk =
        getDataChunk(
            List.of(orderingNeededDataRecord1, orderingNeededDataRecord2), flatSchema, flatSchema);

    DataOutputFormatter dataOutputFormatter =
        new DataOutputFormatterImpl(matchConfig, flatSchema, Optional.of(mockDataOutputCondenser));
    DataChunk formattedDataChunk = dataOutputFormatter.format(orderingNeededDataChunk);

    // Validate DataChunk Schema
    Schema formattedSchema = formattedDataChunk.schema();
    assertEquals(5, formattedSchema.getColumnsList().size());
    assertEquals("key_id", formattedSchema.getColumnsList().get(0).getColumnName());
    assertEquals("em", formattedSchema.getColumnsList().get(1).getColumnName());
    assertEquals("pn", formattedSchema.getColumnsList().get(2).getColumnName());
    assertEquals("metadata", formattedSchema.getColumnsList().get(3).getColumnName());
    assertEquals("error_codes", formattedSchema.getColumnsList().get(4).getColumnName());
    assertFalse(formattedSchema.getSkipHeaderRecord());
    assertEquals(CSV, formattedSchema.getDataFormat());
    assertEquals(flatSchema, formattedDataChunk.inputSchema().get());

    // Validate DataChunk DataRecords
    assertEquals(2, formattedDataChunk.records().size());
    assertEquals(5, formattedDataChunk.records().get(0).getKeyValuesCount());
    assertEquals("key_id", formattedDataChunk.records().get(0).getKeyValuesList().get(0).getKey());
    assertEquals(
        "123", formattedDataChunk.records().get(0).getKeyValuesList().get(0).getStringValue());
    assertEquals("em", formattedDataChunk.records().get(0).getKeyValuesList().get(1).getKey());
    assertEquals(
        "FAKE.1@google.com",
        formattedDataChunk.records().get(0).getKeyValuesList().get(1).getStringValue());
    assertEquals("pn", formattedDataChunk.records().get(0).getKeyValuesList().get(2).getKey());
    assertEquals(
        "555-555-5551",
        formattedDataChunk.records().get(0).getKeyValuesList().get(2).getStringValue());
    assertEquals(
        "metadata", formattedDataChunk.records().get(0).getKeyValuesList().get(3).getKey());
    assertEquals(
        "metadata", formattedDataChunk.records().get(0).getKeyValuesList().get(3).getStringValue());
    assertEquals(
        "error_codes", formattedDataChunk.records().get(0).getKeyValuesList().get(4).getKey());
    assertEquals(
        "12345", formattedDataChunk.records().get(0).getKeyValuesList().get(4).getStringValue());
    assertEquals(SUCCESS, formattedDataChunk.records().get(0).getErrorCode());
    assertEquals(2, formattedDataChunk.records().size());
    assertEquals(5, formattedDataChunk.records().get(1).getKeyValuesCount());
    assertEquals("key_id", formattedDataChunk.records().get(1).getKeyValuesList().get(0).getKey());
    assertEquals(
        "321", formattedDataChunk.records().get(1).getKeyValuesList().get(0).getStringValue());
    assertEquals("em", formattedDataChunk.records().get(1).getKeyValuesList().get(1).getKey());
    assertEquals(
        "FAKE.2@google.com",
        formattedDataChunk.records().get(1).getKeyValuesList().get(1).getStringValue());
    assertEquals("pn", formattedDataChunk.records().get(1).getKeyValuesList().get(2).getKey());
    assertEquals(
        "555-555-5552",
        formattedDataChunk.records().get(1).getKeyValuesList().get(2).getStringValue());
    assertEquals(
        "metadata", formattedDataChunk.records().get(1).getKeyValuesList().get(3).getKey());
    assertEquals(
        "", formattedDataChunk.records().get(1).getKeyValuesList().get(3).getStringValue());
    assertEquals(
        "error_codes", formattedDataChunk.records().get(1).getKeyValuesList().get(4).getKey());
    assertEquals(
        "54321", formattedDataChunk.records().get(1).getKeyValuesList().get(4).getStringValue());
    assertEquals(PARTIAL_SUCCESS, formattedDataChunk.records().get(1).getErrorCode());
  }

  private DataChunk getDataChunk(
      List<DataRecord> dataRecords, Schema inputSchema, Schema currentSchema) {
    return DataChunk.builder()
        .setRecords(dataRecords)
        .setInputSchema(inputSchema)
        .setSchema(currentSchema)
        .build();
  }

  private DataRecord getDataRecord(List<KeyValue> keyValues, JobResultCode jobResultCode) {
    return DataRecord.newBuilder().addAllKeyValues(keyValues).setErrorCode(jobResultCode).build();
  }

  private KeyValue getKeyValue(String key, String value) {
    return KeyValue.newBuilder().setKey(key).setStringValue(value).build();
  }

  private Schema getSchema(String path) throws Exception {
    return ProtoUtils.getProtoFromJson(
        Resources.toString(Objects.requireNonNull(getClass().getResource(path)), UTF_8),
        Schema.class);
  }
}
