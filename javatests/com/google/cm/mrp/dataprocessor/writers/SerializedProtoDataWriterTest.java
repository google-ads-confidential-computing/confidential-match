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
import static com.google.common.truth.Truth8.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.MatchConfigProvider;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchOutputDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchKey;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.FieldMatches;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProcessingMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.AwsWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.CompositeFieldMatchedOutput;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.MatchedOutputField;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.SingleFieldMatchedOutput;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.ModeProto.Mode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.models.JobParameters;
import com.google.cm.mrp.models.JobParameters.OutputDataLocation;
import com.google.cm.util.ProtoUtils;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

  private static final FeatureFlags DEFAULT_FEATURE_FLAGS =
      FeatureFlags.builder().setMaxRecordsPerProtoOutputFile(1000).build();

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final JobParameters DEFAULT_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .build();

  private static final JobParameters GCP_ENCRYPTION_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .setEncryptionMetadata(
              EncryptionMetadata.newBuilder()
                  .setEncryptionKeyInfo(
                      EncryptionKeyInfo.newBuilder()
                          .setWrappedKeyInfo(
                              WrappedKeyInfo.newBuilder()
                                  .setGcpWrappedKeyInfo(GcpWrappedKeyInfo.newBuilder())))
                  .build())
          .build();
  private static final JobParameters AWS_ENCRYPTION_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .setEncryptionMetadata(
              EncryptionMetadata.newBuilder()
                  .setEncryptionKeyInfo(
                      EncryptionKeyInfo.newBuilder()
                          .setWrappedKeyInfo(
                              WrappedKeyInfo.newBuilder()
                                  .setAwsWrappedKeyInfo(AwsWrappedKeyInfo.newBuilder())))
                  .build())
          .build();
  private static final JobParameters JOIN_MODE_PARAMS =
      JobParameters.builder()
          .setMode(Mode.JOIN)
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .build();

  private MatchConfig matchConfig;

  @Before
  public void setup() throws Exception {
    matchConfig = MatchConfigProvider.getMatchConfig("mic");
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
            DEFAULT_FEATURE_FLAGS,
            DEFAULT_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            matchConfig);
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
  public void write_multipleFiles() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_hashed.json");
    var dataChunkBuilder = DataChunk.builder();
    int limit = 1000;
    for (int i = 0; i < limit; i++) {
      // One MatchKey using row level wrapped key encryption
      List<Entry<String, Optional<String>>> keyValues = new ArrayList<>();
      keyValues.add(Map.entry("email", Optional.of("email@google.com" + i)));
      keyValues.add(Map.entry("phone", Optional.of("111-111-" + i)));
      keyValues.add(Map.entry("first_name", Optional.of("fn-" + i)));
      keyValues.add(Map.entry("last_name", Optional.of("ln-" + i)));
      keyValues.add(Map.entry("zip_code", Optional.of("9" + i)));
      keyValues.add(Map.entry("country_code", Optional.of("us")));
      keyValues.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of(i + "")));
      dataChunkBuilder.addRecord(getDataRecord(keyValues).build());
    }
    var dataChunk = dataChunkBuilder.setSchema(schema).build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            FeatureFlags.builder().setMaxRecordsPerProtoOutputFile(100).build(),
            DEFAULT_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            matchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out files
    assertThat(testDataDestination.fileList.stream().filter(File::exists).findAny())
        .isEmpty(); // All temp files should've been deleted
    Set<String> names = new HashSet<>(testDataDestination.fileNames);
    assertThat(names).hasSize(10);
    assertThat(testDataDestination.fileLines).hasSize(1000);
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
    keyValues.add(Map.entry("wip_provider", Optional.of("wip")));
    DataChunk dataChunk =
        DataChunk.builder().addRecord(getDataRecord(keyValues)).setSchema(schema).build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            DEFAULT_FEATURE_FLAGS,
            GCP_ENCRYPTION_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            matchConfig);
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
    keyValues.add(Map.entry("wip_provider", Optional.of("wip")));
    DataRecord dataRecord =
        getDataRecord(keyValues)
            .setProcessingMetadata(
                ProcessingMetadata.newBuilder().setProtoEncryptionLevel(ROW_LEVEL).build())
            .build();
    DataChunk dataChunk = DataChunk.builder().addRecord(dataRecord).setSchema(schema).build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            DEFAULT_FEATURE_FLAGS,
            GCP_ENCRYPTION_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            matchConfig);
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
  public void write_withAwsRowLevelEncryptionKey() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_aws_wrapped_key.json");
    // One MatchKey using row level wrapped key encryption
    List<Entry<String, Optional<String>>> keyValues = new ArrayList<>();
    keyValues.add(Map.entry("email", Optional.of("FAKE.1@google.com")));
    keyValues.add(Map.entry("encrypted_dek", Optional.of("dek")));
    keyValues.add(Map.entry("kek_uri", Optional.of("kek")));
    keyValues.add(Map.entry("role_arn", Optional.of("testRole")));
    DataRecord dataRecord =
        getDataRecord(keyValues)
            .setProcessingMetadata(
                ProcessingMetadata.newBuilder().setProtoEncryptionLevel(ROW_LEVEL).build())
            .build();
    DataChunk dataChunk = DataChunk.builder().addRecord(dataRecord).setSchema(schema).build();

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            DEFAULT_FEATURE_FLAGS,
            AWS_ENCRYPTION_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            matchConfig);
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
    var encryptionKey = returnedRecord.getEncryptionKey().getAwsWrappedKey();
    assertThat(encryptionKey.getEncryptedDek()).isEqualTo("dek");
    assertThat(encryptionKey.getKekUri()).isEqualTo("kek");
    assertThat(encryptionKey.getRoleArn()).isEqualTo("testRole");
  }

  @Test
  public void write_groupByRowMarker() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_multiple_column_groups.json");
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

    MatchConfig testMatchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/match_config_multiple_column_groups.json")),
                UTF_8),
            MatchConfig.class);

    // Run the write method
    DataChunk dataChunk = DataChunk.builder().setRecords(dataRecords).setSchema(schema).build();
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            DEFAULT_FEATURE_FLAGS,
            DEFAULT_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            testMatchConfig);
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

  @Test
  public void write_joinModeSingleAndMultiple_addsToOutput() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_multiple_column_groups.json");
    List<DataRecord> dataRecords = new ArrayList<>();

    Map<Integer, FieldMatch> singleFieldMatchesMap = new HashMap<>();
    Map<Integer, FieldMatch> compositeFieldMatchesMap = new HashMap<>();
    List<Entry<String, Optional<String>>> keyValues = new ArrayList<>();
    keyValues.add(Map.entry("em", Optional.of("FAKE.1@google.com")));
    singleFieldMatchesMap.put(0, getSingleFieldMatch("encrypted_gaia_id", "1-1"));
    keyValues.add(Map.entry("ph", Optional.of("999-999-9999")));
    singleFieldMatchesMap.put(
        1, getSingleFieldMatch("encrypted_gaia_id", "1-2", "encrypted_gaia_id", "1-3"));
    keyValues.add(Map.entry("fn", Optional.of("fakeName")));
    keyValues.add(Map.entry("ln", Optional.of("fakeLastName")));
    keyValues.add(Map.entry("zc", Optional.of("99999")));
    keyValues.add(Map.entry("cc", Optional.of("US")));
    compositeFieldMatchesMap.put(0, getCompositeFieldMatch("encrypted_gaia_id", "1-4"));
    keyValues.add(Map.entry("insta", Optional.of("test")));
    keyValues.add(Map.entry("tik", Optional.of("@test")));
    compositeFieldMatchesMap.put(
        1, getCompositeFieldMatch("encrypted_gaia_id", "1-5", "encrypted_gaia_id", "1-6"));
    keyValues.add(Map.entry("coord_key_id", Optional.of("testKey")));
    keyValues.add(Map.entry("metadata", Optional.empty()));
    keyValues.add(Map.entry("row_status", Optional.of(SUCCESS.toString())));
    keyValues.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("1")));
    dataRecords.add(
        getDataRecord(keyValues)
            .setJoinFields(
                FieldMatches.newBuilder()
                    .putAllSingleFieldRecordMatches(singleFieldMatchesMap)
                    .putAllCompositeFieldRecordMatches(compositeFieldMatchesMap))
            .build());
    DataChunk dataChunk = DataChunk.builder().setRecords(dataRecords).setSchema(schema).build();
    MatchConfig testMatchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/match_config_multiple_column_groups.json")),
                UTF_8),
            MatchConfig.class);

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            DEFAULT_FEATURE_FLAGS,
            JOIN_MODE_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            testMatchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out file
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("proto_writer_test_1.txt");
    assertThat(testDataDestination.fileLines).hasSize(1);
    // Validate the record
    ConfidentialMatchOutputDataRecord returnedRecord =
        base64Decode(testDataDestination.fileLines.get(0)).get();
    assertEquals("SUCCESS", returnedRecord.getStatus());
    assertThat(returnedRecord.getMatchKeysCount()).isEqualTo(4);
    // check email
    var matchKey = returnedRecord.getMatchKeys(0);
    var field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("em");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("FAKE.1@google.com");
    assertThat(field.getMatchedOutputFieldsCount()).isEqualTo(1);
    var matchedOutputFields = field.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    var keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(keyValue.getStringValue()).isEqualTo("1-1");
    // check phone
    matchKey = returnedRecord.getMatchKeys(1);
    field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("ph");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("999-999-9999");
    assertThat(field.getMatchedOutputFieldsCount()).isEqualTo(2);
    matchedOutputFields = matchKey.getField().getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(keyValue.getStringValue()).isEqualTo("1-2");
    matchedOutputFields = matchKey.getField().getMatchedOutputFields(1);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(keyValue.getStringValue()).isEqualTo("1-3");
    // check address
    matchKey = returnedRecord.getMatchKeys(2);
    var compositeField = matchKey.getCompositeField();
    assertThat(compositeField.getKey()).isEqualTo("address");
    assertThat(compositeField.getChildFieldsCount()).isEqualTo(4);
    var childField = compositeField.getChildFields(0);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("fn");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("fakeName");
    childField = compositeField.getChildFields(1);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("ln");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("fakeLastName");
    childField = compositeField.getChildFields(2);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("zc");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("99999");
    childField = compositeField.getChildFields(3);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("cc");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("US");
    // check address joinFields
    assertThat(compositeField.getMatchedOutputFieldsCount()).isEqualTo(1);
    matchedOutputFields = compositeField.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(keyValue.getStringValue()).isEqualTo("1-4");
    // check socials
    matchKey = returnedRecord.getMatchKeys(3);
    compositeField = matchKey.getCompositeField();
    assertThat(compositeField.getKey()).isEqualTo("socials");
    assertThat(compositeField.getChildFieldsCount()).isEqualTo(2);
    childField = compositeField.getChildFields(0);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("insta");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("test");
    childField = compositeField.getChildFields(1);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("tik");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("@test");
    // check socials joinFields
    assertThat(compositeField.getMatchedOutputFieldsCount()).isEqualTo(2);
    matchedOutputFields = compositeField.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(keyValue.getStringValue()).isEqualTo("1-5");
    matchedOutputFields = compositeField.getMatchedOutputFields(1);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(keyValue.getStringValue()).isEqualTo("1-6");
  }

  @Test
  public void write_joinModeGroupRecords_doesNotMixGroupedRecords() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    Schema schema = getSchema("testdata/proto_schema_hashed.json");
    List<DataRecord> dataRecords = new ArrayList<>();
    String joinFieldName = "encrypted_gaia_id";

    Map<Integer, FieldMatch> singleFieldMatchesMap0 = new HashMap<>();
    Map<Integer, FieldMatch> compositeFieldMatchesMap0 = new HashMap<>();
    List<Entry<String, Optional<String>>> keyValues0 = new ArrayList<>();
    keyValues0.add(Map.entry("email", Optional.of("fake0@google.com")));
    singleFieldMatchesMap0.put(0, getSingleFieldMatch(joinFieldName, "0-0"));
    keyValues0.add(Map.entry("phone", Optional.of("UNMATCHED")));
    keyValues0.add(Map.entry("first_name", Optional.of("fakeName0")));
    keyValues0.add(Map.entry("last_name", Optional.of("fakeLastName0")));
    keyValues0.add(Map.entry("zip_code", Optional.of("00000")));
    keyValues0.add(Map.entry("country_code", Optional.of("US")));
    compositeFieldMatchesMap0.put(0, getCompositeFieldMatch(joinFieldName, "0-2"));
    keyValues0.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("0")));
    dataRecords.add(
        getDataRecord(keyValues0)
            .setJoinFields(
                FieldMatches.newBuilder()
                    .putAllSingleFieldRecordMatches(singleFieldMatchesMap0)
                    .putAllCompositeFieldRecordMatches(compositeFieldMatchesMap0))
            .build());
    Map<Integer, FieldMatch> singleFieldMatchesMap1 = new HashMap<>();
    Map<Integer, FieldMatch> compositeFieldMatchesMap1 = new HashMap<>();
    List<Entry<String, Optional<String>>> keyValues1 = new ArrayList<>();
    keyValues1.add(Map.entry("email", Optional.of("UNMATCHED")));
    keyValues1.add(Map.entry("phone", Optional.of("111-111-1111")));
    singleFieldMatchesMap1.put(1, getSingleFieldMatch(joinFieldName, "1-1"));
    keyValues1.add(Map.entry("first_name", Optional.of("fakeName1")));
    keyValues1.add(Map.entry("last_name", Optional.of("fakeLastName1")));
    keyValues1.add(Map.entry("zip_code", Optional.of("11111")));
    keyValues1.add(Map.entry("country_code", Optional.of("US")));
    compositeFieldMatchesMap1.put(0, getCompositeFieldMatch(joinFieldName, "1-2"));
    keyValues1.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("0")));
    dataRecords.add(
        getDataRecord(keyValues1)
            .setJoinFields(
                FieldMatches.newBuilder()
                    .putAllSingleFieldRecordMatches(singleFieldMatchesMap1)
                    .putAllCompositeFieldRecordMatches(compositeFieldMatchesMap1))
            .build());
    Map<Integer, FieldMatch> singleFieldMatchesMap2 = new HashMap<>();
    List<Entry<String, Optional<String>>> keyValues2 = new ArrayList<>();
    keyValues2.add(Map.entry("email", Optional.of("fake2@google.com")));
    singleFieldMatchesMap2.put(0, getSingleFieldMatch(joinFieldName, "2-0"));
    keyValues2.add(Map.entry("phone", Optional.of("UNMATCHED")));
    keyValues2.add(Map.entry("first_name", Optional.of("UNMATCHED")));
    keyValues2.add(Map.entry("last_name", Optional.of("UNMATCHED")));
    keyValues2.add(Map.entry("zip_code", Optional.of("UNMATCHED")));
    keyValues2.add(Map.entry("country_code", Optional.of("UNMATCHED")));
    keyValues2.add(Map.entry(ROW_MARKER_COLUMN_NAME, Optional.of("1")));
    dataRecords.add(
        getDataRecord(keyValues2)
            .setJoinFields(
                FieldMatches.newBuilder().putAllSingleFieldRecordMatches(singleFieldMatchesMap2))
            .build());
    DataChunk dataChunk = DataChunk.builder().setRecords(dataRecords).setSchema(schema).build();

    MatchConfig testMatchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/match_config_multiple_column_groups.json")),
                UTF_8),
            MatchConfig.class);

    // Run the write method
    SerializedProtoDataWriter dataWriter =
        new SerializedProtoDataWriter(
            DEFAULT_FEATURE_FLAGS,
            JOIN_MODE_PARAMS,
            testDataDestination,
            "proto_writer_test.txt",
            schema,
            testMatchConfig);
    dataWriter.write(dataChunk);
    dataWriter.close();

    // Validate the written out file
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("proto_writer_test_1.txt");
    assertThat(testDataDestination.fileLines).hasSize(2);
    // Validate the first record
    ConfidentialMatchOutputDataRecord returnedRecord =
        base64Decode(testDataDestination.fileLines.get(0)).get();
    assertThat(returnedRecord.getMatchKeysCount()).isEqualTo(6);
    // sort matchKeys for deterministic testing
    var matchKeys = sortMatchKeys(returnedRecord.getMatchKeysList());
    // check matched email
    var matchKey = matchKeys.get(0);
    var field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("email");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("fake0@google.com");
    assertThat(field.getMatchedOutputFieldsCount()).isEqualTo(1);
    var matchedOutputFields = field.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    var keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo(joinFieldName);
    assertThat(keyValue.getStringValue()).isEqualTo("0-0");
    // check unmatched email
    matchKey = matchKeys.get(1);
    field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("email");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    assertThat(field.getMatchedOutputFieldsList()).isEmpty();
    // check matched phone
    matchKey = matchKeys.get(2);
    field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("phone");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("111-111-1111");
    assertThat(field.getMatchedOutputFieldsCount()).isEqualTo(1);
    matchedOutputFields = field.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo(joinFieldName);
    assertThat(keyValue.getStringValue()).isEqualTo("1-1");
    // check unmatched phone
    matchKey = matchKeys.get(3);
    field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("phone");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    assertThat(field.getMatchedOutputFieldsList()).isEmpty(); // no matches
    // check first address
    matchKey = matchKeys.get(4);
    var compositeField = matchKey.getCompositeField();
    assertThat(compositeField.getKey()).isEqualTo("address");
    assertThat(compositeField.getChildFieldsCount()).isEqualTo(4);
    var childField = compositeField.getChildFields(0);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("first_name");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("fakeName0");
    childField = compositeField.getChildFields(1);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("last_name");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("fakeLastName0");
    childField = compositeField.getChildFields(2);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("zip_code");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("00000");
    childField = compositeField.getChildFields(3);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("country_code");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("US");
    // check address joinFields
    assertThat(compositeField.getMatchedOutputFieldsCount()).isEqualTo(1);
    matchedOutputFields = compositeField.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo(joinFieldName);
    assertThat(keyValue.getStringValue()).isEqualTo("0-2");
    // check second address
    matchKey = matchKeys.get(5);
    compositeField = matchKey.getCompositeField();
    assertThat(compositeField.getKey()).isEqualTo("address");
    assertThat(compositeField.getChildFieldsCount()).isEqualTo(4);
    childField = compositeField.getChildFields(0);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("first_name");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("fakeName1");
    childField = compositeField.getChildFields(1);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("last_name");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("fakeLastName1");
    childField = compositeField.getChildFields(2);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("zip_code");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("11111");
    childField = compositeField.getChildFields(3);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("country_code");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("US");
    // check address joinFields
    assertThat(compositeField.getMatchedOutputFieldsCount()).isEqualTo(1);
    matchedOutputFields = compositeField.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo(joinFieldName);
    assertThat(keyValue.getStringValue()).isEqualTo("1-2");

    // Validate the second record
    returnedRecord = base64Decode(testDataDestination.fileLines.get(1)).get();
    assertThat(returnedRecord.getMatchKeysCount()).isEqualTo(3);
    // sort matchKeys for deterministic testing
    matchKeys = sortMatchKeys(returnedRecord.getMatchKeysList());
    // check matched email
    matchKey = matchKeys.get(0);
    field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("email");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("fake2@google.com");
    assertThat(field.getMatchedOutputFieldsCount()).isEqualTo(1);
    matchedOutputFields = field.getMatchedOutputFields(0);
    assertThat(matchedOutputFields.getKeyValueCount()).isEqualTo(1);
    keyValue = matchedOutputFields.getKeyValue(0);
    assertThat(keyValue.getKey()).isEqualTo(joinFieldName);
    assertThat(keyValue.getStringValue()).isEqualTo("2-0");
    // check unmatched phone
    matchKey = matchKeys.get(1);
    field = matchKey.getField();
    assertThat(field.getKeyValue().getKey()).isEqualTo("phone");
    assertThat(field.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    assertThat(field.getMatchedOutputFieldsList()).isEmpty(); // no matches
    // check unmatched address
    matchKey = matchKeys.get(2);
    compositeField = matchKey.getCompositeField();
    assertThat(compositeField.getKey()).isEqualTo("address");
    assertThat(compositeField.getChildFieldsCount()).isEqualTo(4);
    childField = compositeField.getChildFields(0);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("first_name");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    childField = compositeField.getChildFields(1);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("last_name");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    childField = compositeField.getChildFields(2);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("zip_code");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    childField = compositeField.getChildFields(3);
    assertThat(childField.getKeyValue().getKey()).isEqualTo("country_code");
    assertThat(childField.getKeyValue().getStringValue()).isEqualTo("UNMATCHED");
    // no matches
    assertThat(compositeField.getMatchedOutputFieldsList()).isEmpty();
  }

  private FieldMatch getSingleFieldMatch(String k0, String v0) {
    return getSingleFieldMatch(k0, v0, /* k1= */ "", /* v1= */ "");
  }

  private FieldMatch getSingleFieldMatch(String k0, String v0, String k1, String v1) {
    String[][] keyValueQuads = {{k0, v0}, {k1, v1}};
    var builder = SingleFieldMatchedOutput.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      if (!keyValue[0].isBlank() && !keyValue[1].isBlank()) {
        builder.addMatchedOutputFields(
            MatchedOutputField.newBuilder()
                .addIndividualFields(
                    MatchedOutputField.Field.newBuilder()
                        .setKey(keyValue[0])
                        .setValue(keyValue[1])));
      }
    }
    return FieldMatch.newBuilder().setSingleFieldMatchedOutput(builder).build();
  }

  private FieldMatch getCompositeFieldMatch(String k0, String v0) {
    return getCompositeFieldMatch(k0, v0, /* k1= */ "", /* v1= */ "");
  }

  private FieldMatch getCompositeFieldMatch(String k0, String v0, String k1, String v1) {
    String[][] keyValueQuads = {{k0, v0}, {k1, v1}};
    var builder = CompositeFieldMatchedOutput.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      if (!keyValue[0].isBlank() && !keyValue[1].isBlank()) {
        builder.addMatchedOutputFields(
            MatchedOutputField.newBuilder()
                .addIndividualFields(
                    MatchedOutputField.Field.newBuilder()
                        .setKey(keyValue[0])
                        .setValue(keyValue[1])));
      }
    }
    return FieldMatch.newBuilder().setCompositeFieldMatchedOutput(builder).build();
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
    return getDataRecord(keyValueEntries, FieldMatches.newBuilder().build());
  }

  private DataRecord.Builder getDataRecord(
      List<Entry<String, Optional<String>>> keyValueEntries, FieldMatches joinFields) {
    return DataRecord.newBuilder()
        .setJoinFields(joinFields)
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

  /**
   * Sort match keys, first by single vs composite then by key name, then by first int in the values
   * of the matches. For this reason, it is recommended that the test values correspond to the
   * desired order. Eg: email: test1@gmail.com , email-> test2@gmail.com . The `1` and `2` will be
   * used to sort. `UNMATCHED` will be sorted last
   */
  private List<MatchKey> sortMatchKeys(List<MatchKey> inputList) {
    List<MatchKey> mutableList = new ArrayList<>(inputList);
    mutableList.sort(
        (matchKey1, matchKey2) -> {
          int isComposite =
              Boolean.compare(matchKey1.hasCompositeField(), matchKey2.hasCompositeField());
          if (isComposite == 0) {
            if (matchKey1.hasField()) {
              // get key from KV and compare them
              KeyValue kv1 = matchKey1.getField().getKeyValue();
              KeyValue kv2 = matchKey2.getField().getKeyValue();
              int keyCompare = kv1.getKey().compareTo(kv2.getKey());
              // if it's the same key, then compare the actual value by taking
              // the first int found and comparing it.
              if (keyCompare == 0) {
                int numInValue1 = getFirstIntInStringValue(kv1);
                int numInValue2 = getFirstIntInStringValue(kv2);
                return Integer.compare(numInValue1, numInValue2);
              }
              return keyCompare;
            } else {
              // get key from compositeField and compare them
              CompositeField cf1 = matchKey1.getCompositeField();
              CompositeField cf2 = matchKey1.getCompositeField();
              int keyCompare = cf1.getKey().compareTo(cf2.getKey());
              // if it's the same key, then compare the values of the childFields by taking
              // the first int found and comparing it.
              if (keyCompare == 0) {
                int numInValue1 = getFirstIntegerInChildFields(cf1);
                int numInValue2 = getFirstIntegerInChildFields(cf2);
                return Integer.compare(numInValue1, numInValue2);
              }
              return keyCompare;
            }
          }
          return isComposite;
        });
    return mutableList;
  }

  private int getFirstIntegerInChildFields(CompositeField compositeField) {
    String intStr =
        compositeField.getChildFieldsList().stream()
            .map(field -> field.getKeyValue().getStringValue().replaceAll("[^0-9]", ""))
            .collect(Collectors.joining());
    return intStr.isBlank() ? Integer.MAX_VALUE : intStr.charAt(0);
  }

  private int getFirstIntInStringValue(KeyValue kv) {
    String intStr = kv.getStringValue().replaceAll("[^0-9]", "");
    return intStr.isBlank() ? Integer.MAX_VALUE : intStr.charAt(0);
  }

  private static class TestDataDestination implements DataDestination {

    private List<String> fileLines;
    private File file;
    private String name;
    private List<String> fileNames;
    private List<File> fileList;

    private final boolean throwException;

    TestDataDestination() {
      throwException = false;
      fileList = new ArrayList<>();
      fileLines = new ArrayList<>();
      fileNames = new ArrayList<>();
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
      fileNames.add(name);
      fileList.add(file);
      fileLines.addAll(Files.readAllLines(file.toPath()));
    }
  }
}
