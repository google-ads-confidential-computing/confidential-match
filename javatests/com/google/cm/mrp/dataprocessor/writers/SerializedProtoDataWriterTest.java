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

package com.google.cm.mrp.dataprocessor.writers;

import static com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProtoEncryptionLevel.ROW_LEVEL;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARTIAL_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchOutputDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProcessingMetadata;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.util.ProtoUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class SerializedProtoDataWriterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private MatchConfig matchConfig;

  @Before
  public void setup() throws Exception {
    matchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(getClass().getResource("testdata/proto_match_config.json")),
                UTF_8),
            MatchConfig.class);
  }

  @Test
  public void write_differentValueTypes() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_data_types.json");

    // Metadata KeyValues of different data type
    // No grouping records
    // No encryption
    List<DataRecord.KeyValue> keyValues = new ArrayList<>();
    keyValues.add(
        DataRecord.KeyValue.newBuilder().setKey("md_string").setStringValue("value").build());
    keyValues.add(DataRecord.KeyValue.newBuilder().setKey("md_int").setIntValue(1).build());
    keyValues.add(DataRecord.KeyValue.newBuilder().setKey("md_bool").setBoolValue(true).build());
    keyValues.add(DataRecord.KeyValue.newBuilder().setKey("md_double").setDoubleValue(0.5).build());
    DataChunk dataChunk =
        DataChunk.builder()
            .addRecord(DataRecord.newBuilder().addAllKeyValues(keyValues).build())
            .setSchema(schema)
            .build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            1000, testDataDestination, "proto_writer_test.txt", schema, matchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out file
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("proto_writer_test_1.txt");
    assertThat(testDataDestination.fileLines).hasSize(1);

    // Validate the file's record
    ConfidentialMatchOutputDataRecord returnedRecord =
        base64Decode(testDataDestination.fileLines.get(0)).get();
    assertEquals("", returnedRecord.getStatus());
    assertThat(returnedRecord.getMatchKeysCount()).isEqualTo(0);
    assertThat(returnedRecord.getMetadataCount()).isEqualTo(4);
    returnedRecord
        .getMetadataList()
        .forEach(
            keyValue -> {
              switch (keyValue.getValueCase()) {
                case INT_VALUE:
                  assertThat(keyValue.getKey()).isEqualTo("md_int");
                  assertThat(keyValue.getIntValue()).isEqualTo(1);
                  keyValue.getIntValue();
                  break;
                case BOOL_VALUE:
                  assertThat(keyValue.getKey()).isEqualTo("md_bool");
                  assertThat(keyValue.getBoolValue()).isEqualTo(true);
                  break;
                case DOUBLE_VALUE:
                  assertThat(keyValue.getKey()).isEqualTo("md_double");
                  assertThat(keyValue.getDoubleValue()).isEqualTo(0.5);
                  break;
                case STRING_VALUE:
                  assertThat(keyValue.getKey()).isEqualTo("md_string");
                  assertThat(keyValue.getStringValue()).isEqualTo("value");
                  break;
                case VALUE_NOT_SET:
                default:
                  break;
              }
            });
  }

  @Test
  public void write_withWrappedKey() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_wrapped_key.json");

    // One MatchKey using wrapped key encryption
    List<Entry<String, Optional<String>>> keyValues = new ArrayList<>();
    keyValues.add(Map.entry("email", Optional.of("FAKE.1@google.com")));
    keyValues.add(Map.entry("encrypted_dek", Optional.of("dek")));
    keyValues.add(Map.entry("kek_uri", Optional.of("kek")));
    keyValues.add(Map.entry("wip_alias", Optional.of("wip")));
    DataChunk dataChunk =
        DataChunk.builder().addRecord(getDataRecord(keyValues)).setSchema(schema).build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            1000, testDataDestination, "proto_writer_test.txt", schema, matchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out file
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("proto_writer_test_1.txt");
    assertThat(testDataDestination.fileLines).hasSize(1);

    // Validate the file's record
    ConfidentialMatchOutputDataRecord returnedRecord =
        base64Decode(testDataDestination.fileLines.get(0)).get();
    assertEquals("", returnedRecord.getStatus());
    assertThat(returnedRecord.getMetadataCount()).isEqualTo(0);
    assertThat(returnedRecord.getMatchKeysCount()).isEqualTo(1);
    var matchKey = returnedRecord.getMatchKeys(0);
    assertThat(matchKey.getField().getKeyValue().getKey()).isEqualTo("email");
    assertThat(matchKey.getField().getKeyValue().getStringValue()).isEqualTo("FAKE.1@google.com");
    var encryptionKey = returnedRecord.getMatchKeys(0).getEncryptionKey().getWrappedKey();
    assertThat(encryptionKey.getEncryptedDek()).isEqualTo("dek");
    assertThat(encryptionKey.getKekUri()).isEqualTo("kek");
    assertThat(encryptionKey.getWip()).isEqualTo("wip");
  }

  @Test
  public void write_withRowLevelEncryptionKey() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_wrapped_key.json");

    // One MatchKey using row level wrapped key encryption
    List<Entry<String, Optional<String>>> keyValues = new ArrayList<>();
    keyValues.add(Map.entry("email", Optional.of("FAKE.1@google.com")));
    keyValues.add(Map.entry("encrypted_dek", Optional.of("dek")));
    keyValues.add(Map.entry("kek_uri", Optional.of("kek")));
    keyValues.add(Map.entry("wip_alias", Optional.of("wip")));
    DataRecord dataRecord =
        getDataRecord(keyValues)
            .setProcessingMetadata(
                ProcessingMetadata.newBuilder().setProtoEncryptionLevel(ROW_LEVEL).build())
            .build();
    DataChunk dataChunk = DataChunk.builder().addRecord(dataRecord).setSchema(schema).build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            1000, testDataDestination, "proto_writer_test.txt", schema, matchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out file
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("proto_writer_test_1.txt");
    assertThat(testDataDestination.fileLines).hasSize(1);

    // Validate the file's record
    ConfidentialMatchOutputDataRecord returnedRecord =
        base64Decode(testDataDestination.fileLines.get(0)).get();
    assertEquals("", returnedRecord.getStatus());
    assertThat(returnedRecord.getMetadataCount()).isEqualTo(0);
    assertThat(returnedRecord.getMatchKeysCount()).isEqualTo(1);
    var matchKey = returnedRecord.getMatchKeys(0);
    assertThat(matchKey.getField().getKeyValue().getKey()).isEqualTo("email");
    assertThat(matchKey.getField().getKeyValue().getStringValue()).isEqualTo("FAKE.1@google.com");
    assertFalse(returnedRecord.getMatchKeys(0).hasEncryptionKey());
    assertTrue(returnedRecord.hasEncryptionKey());
    var encryptionKey = returnedRecord.getEncryptionKey().getWrappedKey();
    assertThat(encryptionKey.getEncryptedDek()).isEqualTo("dek");
    assertThat(encryptionKey.getKekUri()).isEqualTo("kek");
    assertThat(encryptionKey.getWip()).isEqualTo("wip");
  }

  @Test
  public void write_groupByRowMarker() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema.json");
    List<DataRecord> dataRecords = new ArrayList<>();
    // Uses coordinator key
    String encryptionKey1 = "123";
    String encryptionKey2 = "321";

    // These two data records will be combined into one output record proto due to the row marker
    // DataRecord 1
    List<Entry<String, Optional<String>>> keyValueEntries1 = new ArrayList<>();
    keyValueEntries1.add(Map.entry("em", Optional.of("FAKE.1@google.com")));
    keyValueEntries1.add(Map.entry("ph", Optional.of("555-555-5551")));
    keyValueEntries1.add(Map.entry("fn", Optional.of("John")));
    keyValueEntries1.add(Map.entry("ln", Optional.of("Doe")));
    keyValueEntries1.add(Map.entry("zc", Optional.of("99999")));
    keyValueEntries1.add(Map.entry("cc", Optional.of("US")));
    keyValueEntries1.add(Map.entry("insta", Optional.of("@fake-instagram")));
    keyValueEntries1.add(Map.entry("tik", Optional.of("@fake-tiktok")));
    keyValueEntries1.add(Map.entry("coord_key_id", Optional.of(encryptionKey1)));
    keyValueEntries1.add(Map.entry("metadata", Optional.of("metadata_value")));
    keyValueEntries1.add(Map.entry("row_status", Optional.of(SUCCESS.toString())));
    keyValueEntries1.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("1")));
    dataRecords.add(getDataRecord(keyValueEntries1).build());
    // DataRecord 2
    List<Entry<String, Optional<String>>> keyValueEntries2 = new ArrayList<>();
    keyValueEntries2.add(Map.entry("em", Optional.of("FAKE.2@google.com")));
    keyValueEntries2.add(Map.entry("ph", Optional.of("555-555-5552")));
    keyValueEntries2.add(Map.entry("fn", Optional.of("Jane")));
    keyValueEntries2.add(Map.entry("ln", Optional.of("Deer")));
    keyValueEntries2.add(Map.entry("zc", Optional.of("V6Z 2H7")));
    keyValueEntries2.add(Map.entry("cc", Optional.of("CA")));
    keyValueEntries2.add(Map.entry("insta", Optional.empty()));
    keyValueEntries2.add(Map.entry("tik", Optional.empty()));
    keyValueEntries2.add(Map.entry("coord_key_id", Optional.of(encryptionKey1)));
    keyValueEntries2.add(Map.entry("metadata", Optional.empty()));
    keyValueEntries2.add(Map.entry("row_status", Optional.of(INVALID_PARTIAL_ERROR.toString())));
    keyValueEntries2.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("1")));
    dataRecords.add(getDataRecord(keyValueEntries2).build());

    // Set up maps to be used in validating the output record proto for DataRecord 1 and 2.
    // Fields
    Map<String, String> fieldMap1 = new HashMap<>();
    fieldMap1.put("FAKE.1@google.com", "em");
    fieldMap1.put("FAKE.2@google.com", "em");
    fieldMap1.put("555-555-5551", "ph");
    fieldMap1.put("555-555-5552", "ph");
    // CompositeField address
    List<Map<String, String>> addressGroupMaps1 = new ArrayList<>();
    Map<String, String> firstAddressGroupMap1 = new HashMap<>();
    firstAddressGroupMap1.put("John", "fn");
    firstAddressGroupMap1.put("Doe", "ln");
    firstAddressGroupMap1.put("99999", "zc");
    firstAddressGroupMap1.put("US", "cc");
    addressGroupMaps1.add(firstAddressGroupMap1);
    Map<String, String> secondAddressGroupMap1 = new HashMap<>();
    secondAddressGroupMap1.put("Jane", "fn");
    secondAddressGroupMap1.put("Deer", "ln");
    secondAddressGroupMap1.put("V6Z 2H7", "zc");
    secondAddressGroupMap1.put("CA", "cc");
    addressGroupMaps1.add(secondAddressGroupMap1);
    // CompositeField socials
    List<Map<String, String>> socialGroupMaps1 = new ArrayList<>();
    Map<String, String> firstSocialsGroupMap1 = new HashMap<>();
    firstSocialsGroupMap1.put("@fake-instagram", "insta");
    firstSocialsGroupMap1.put("@fake-tiktok", "tik");
    socialGroupMaps1.add(firstSocialsGroupMap1);

    // This third data record will output as one output record proto due to the row marker.
    // DataRecord 3
    List<Entry<String, Optional<String>>> keyValueEntries3 = new ArrayList<>();
    keyValueEntries3.add(Map.entry("em", Optional.of("FAKE.3@google.com")));
    keyValueEntries3.add(Map.entry("ph", Optional.empty()));
    keyValueEntries3.add(Map.entry("fn", Optional.of("UNMATCHED_1")));
    keyValueEntries3.add(Map.entry("ln", Optional.of("UNMATCHED_2")));
    keyValueEntries3.add(Map.entry("zc", Optional.of("UNMATCHED_3")));
    keyValueEntries3.add(Map.entry("cc", Optional.empty()));
    keyValueEntries3.add(Map.entry("insta", Optional.empty()));
    keyValueEntries3.add(Map.entry("tik", Optional.empty()));
    keyValueEntries3.add(Map.entry("coord_key_id", Optional.of(encryptionKey2)));
    keyValueEntries3.add(Map.entry("metadata", Optional.empty()));
    keyValueEntries3.add(Map.entry("row_status", Optional.of(SUCCESS.toString())));
    keyValueEntries3.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("2")));
    dataRecords.add(getDataRecord(keyValueEntries3).build());

    // Set up maps to be used in validating the output record proto for third input DataRecord.
    Map<String, String> fieldMap2 = new HashMap<>();
    fieldMap2.put("FAKE.3@google.com", "em");
    List<Map<String, String>> addressGroupMaps2 = new ArrayList<>();
    Map<String, String> firstAddressGroupMap2 = new HashMap<>();
    firstAddressGroupMap2.put("UNMATCHED_1", "fn");
    firstAddressGroupMap2.put("UNMATCHED_2", "ln");
    firstAddressGroupMap2.put("UNMATCHED_3", "zc");
    addressGroupMaps2.add(firstAddressGroupMap2);
    List<Map<String, String>> socialGroupMaps2 = new ArrayList<>();

    // Run the write method
    DataChunk dataChunk = DataChunk.builder().setRecords(dataRecords).setSchema(schema).build();
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            1000, testDataDestination, "proto_writer_test.txt", schema, matchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out file
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("proto_writer_test_1.txt");
    assertThat(testDataDestination.fileLines).hasSize(2);

    // Validate the first record of the file
    ConfidentialMatchOutputDataRecord returnedRecord1 =
        base64Decode(testDataDestination.fileLines.get(0)).get();
    assertEquals(INVALID_PARTIAL_ERROR.toString(), returnedRecord1.getStatus());
    assertThat(returnedRecord1.getMetadataList()).hasSize(1);
    assertEquals("metadata", returnedRecord1.getMetadataList().get(0).getKey());
    assertEquals("metadata_value", returnedRecord1.getMetadataList().get(0).getStringValue());
    returnedRecord1
        .getMatchKeysList()
        .forEach(
            matchKey -> {
              assertEquals(
                  encryptionKey1, matchKey.getEncryptionKey().getCoordinatorKey().getKeyId());
              if (matchKey.hasField()) {
                validateField(matchKey.getField(), fieldMap1);
              } else {
                switch (matchKey.getCompositeField().getKey()) {
                  case "address":
                    assertTrue(
                        validateCompositeFields(
                            matchKey.getCompositeField().getChildFieldsList(), addressGroupMaps1));
                    break;
                  case "socials":
                    assertTrue(
                        validateCompositeFields(
                            matchKey.getCompositeField().getChildFieldsList(), socialGroupMaps1));
                    break;
                  default:
                    fail(
                        "Unknown CompositeField name found: "
                            + matchKey.getCompositeField().getKey());
                }
              }
            });
    assertThat(fieldMap1).isEmpty();

    // Validate the second record of the file
    ConfidentialMatchOutputDataRecord returnedRecord2 =
        base64Decode(testDataDestination.fileLines.get(1)).get();
    assertEquals(SUCCESS.toString(), returnedRecord2.getStatus());
    assertThat(returnedRecord2.getMetadataList()).hasSize(0);
    returnedRecord2
        .getMatchKeysList()
        .forEach(
            matchKey -> {
              assertEquals(
                  encryptionKey2, matchKey.getEncryptionKey().getCoordinatorKey().getKeyId());
              if (matchKey.hasField()) {
                validateField(matchKey.getField(), fieldMap2);
              } else {
                switch (matchKey.getCompositeField().getKey()) {
                  case "address":
                    assertTrue(
                        validateCompositeFields(
                            matchKey.getCompositeField().getChildFieldsList(), addressGroupMaps2));
                    break;
                  case "socials":
                    assertTrue(
                        validateCompositeFields(
                            matchKey.getCompositeField().getChildFieldsList(), socialGroupMaps2));
                    break;
                  default:
                    fail(
                        "Unknown CompositeField name found: "
                            + matchKey.getCompositeField().getKey());
                }
              }
            });
    assertThat(fieldMap2).isEmpty();
  }

  private void validateField(Field field, Map<String, String> fieldMap) {
    KeyValue keyValue = field.getKeyValue();
    assertThat(fieldMap).containsKey(keyValue.getStringValue());
    assertEquals(fieldMap.get(keyValue.getStringValue()), keyValue.getKey());
    fieldMap.remove(keyValue.getStringValue());
  }

  private boolean validateCompositeFields(
      List<CompositeChildField> fields, List<Map<String, String>> compositeFieldMaps) {
    for (var compositeFieldMap : compositeFieldMaps) {
      if (compositeFieldMap.containsKey(fields.get(0).getKeyValue().getStringValue())) {
        for (var field : fields) {
          KeyValue keyValue = field.getKeyValue();
          assertEquals(keyValue.getKey(), compositeFieldMap.get(keyValue.getStringValue()));
          compositeFieldMap.remove(keyValue.getStringValue());
        }
        return 0 == compositeFieldMap.size();
      }
    }
    return false;
  }

  private Schema getSchema(String path) throws Exception {
    return ProtoUtils.getProtoFromJson(
        Resources.toString(Objects.requireNonNull(getClass().getResource(path)), Charsets.UTF_8),
        Schema.class);
  }

  private Optional<ConfidentialMatchOutputDataRecord> base64Decode(String base64EncodedString) {
    try {
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
      return Optional.of(ConfidentialMatchOutputDataRecord.parseFrom(decodedBytes));
    } catch (Exception e) {
      fail("Failed to decode ConfidentialMatchOutputDataRecord: " + e.getMessage());
      return Optional.empty();
    }
  }

  private DataRecord.Builder getDataRecord(List<Entry<String, Optional<String>>> keyValueEntries) {
    return DataRecord.newBuilder()
        .addAllKeyValues(
            keyValueEntries.stream()
                .map(
                    entry -> {
                      DataRecord.KeyValue.Builder keyValue =
                          DataRecord.KeyValue.newBuilder().setKey(entry.getKey());
                      return entry.getValue().isPresent()
                          ? keyValue.setStringValue(entry.getValue().get()).build()
                          : keyValue.build();
                    })
                .collect(Collectors.toList()));
  }

  private static class TestDataDestination implements DataDestination {

    private List<String> fileLines;
    private File file;
    private String name;

    private final boolean throwException;

    TestDataDestination() {
      throwException = false;
    }

    TestDataDestination(boolean throwException) {
      this.throwException = throwException;
    }

    @Override
    public void write(File file, String name) throws IOException {
      this.file = file;
      this.name = name;
      if (throwException) {
        throw new RuntimeException("Test write exception");
      }
      fileLines = Files.readAllLines(file.toPath());
    }
  }
}
