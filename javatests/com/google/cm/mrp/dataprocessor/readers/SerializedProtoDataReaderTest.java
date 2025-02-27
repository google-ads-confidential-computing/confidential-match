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

package com.google.cm.mrp.dataprocessor.readers;

import static com.google.cm.mrp.MatchConfigProvider.getMatchConfig;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_ENCRYPTION_TYPE;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateAeadUri;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateEncryptedDek;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.getDefaultAeadSelector;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridDecrypt;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridEncrypt;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.CoordinatorKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.GcpWrappedKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchKey;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProtoEncryptionLevel;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClient;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.HybridCryptoClient;
import com.google.cm.mrp.dataprocessor.converters.SchemaConverter;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.testutils.AeadKeyGenerator;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class SerializedProtoDataReaderTest {

  private static final MatchConfig micMatchConfig = getMatchConfig("mic_proto");

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private HybridEncryptionKeyService mockHybridEncryptionKeyService;
  private HybridCryptoClient hybridCryptoClient;
  @Mock private AeadProvider mockAeadProvider;
  @Mock private ConfidentialMatchDataRecordParserFactory mockCfmDataRecordParserFactory;
  private static final String TEST_WIP = "TEST_WIP";

  @Test
  public void next_coordinatorKey_returnsDecryptedRecords() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_encrypted.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "txt");
    String testCoordinatorKey = "test123";
    var encrypter = getDefaultHybridEncrypt();
    String encryptedEmail = encryptString(encrypter, "fakeemail@fake.com");
    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setCoordinatorKey(
                        CoordinatorKey.newBuilder().setKeyId(testCoordinatorKey).build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue(encryptedEmail)
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.newLine();
      writer.write(base64().encode(testRecord.toByteArray()));
      writer.newLine();
    }
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);
    when(mockCfmDataRecordParserFactory.create(any(), any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig,
                generateInternalSchema(schema),
                SuccessMode.ALLOW_PARTIAL_SUCCESS,
                COORDINATOR_ENCRYPTION_METADATA));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            micMatchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            COORDINATOR_ENCRYPTION_METADATA,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(
              ImmutableList.copyOf(SchemaConverter.convertToColumnAliases(dataReader.getSchema())))
          .containsExactly(
              "metadata",
              "coordinator_key_id",
              "email",
              "phone",
              "first_name",
              "last_name",
              "country_code",
              "zip_code",
              ROW_MARKER_COLUMN_NAME);

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasCoordinatorKey()).isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(2, 3, 4, 5);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getCoordinatorKeyColumnIndices()
                  .getCoordinatorKeyColumnIndex())
          .isEqualTo(1);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("fake metadata");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(testCoordinatorKey);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo("fakeemail@fake.com");
      assertThat(record.getEncryptedKeyValuesMap().get(2)).isEqualTo(encryptedEmail);
      assertThat(record.getProcessingMetadata().getProtoEncryptionLevel())
          .isEqualTo(ProtoEncryptionLevel.MATCH_KEY_LEVEL);
    }
  }

  @Test
  public void next_wrappedKey_returnsDecryptedRecords() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_wrapped_encrypted.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "txt");
    String dek = generateEncryptedDek();
    String kekUri = generateAeadUri();
    String encryptedEmail = AeadKeyGenerator.encryptString(dek, "fakeemail@fake.com");
    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder().setEncryptedDek(dek).setKekUri(kekUri).build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue(encryptedEmail)
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.newLine();
      writer.write(base64().encode(testRecord.toByteArray()));
      writer.newLine();
    }
    when(mockCfmDataRecordParserFactory.create(any(), any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig,
                generateInternalSchema(schema),
                SuccessMode.ALLOW_PARTIAL_SUCCESS,
                WRAPPED_ENCRYPTION_METADATA));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            micMatchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            WRAPPED_ENCRYPTION_METADATA,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(
              ImmutableList.copyOf(SchemaConverter.convertToColumnAliases(dataReader.getSchema())))
          .containsExactly(
              "metadata",
              "encrypted_dek",
              "kek_uri",
              "email",
              "phone",
              "first_name",
              "last_name",
              "country_code",
              "zip_code",
              ROW_MARKER_COLUMN_NAME);

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(3, 4, 5, 6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(1);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(2);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("fake metadata");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(dek);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(kekUri);
      assertThat(record.getKeyValues(3).getStringValue()).isEqualTo("fakeemail@fake.com");
      assertThat(record.getEncryptedKeyValuesMap().get(3)).isEqualTo(encryptedEmail);
      assertThat(record.getProcessingMetadata().getProtoEncryptionLevel())
          .isEqualTo(ProtoEncryptionLevel.MATCH_KEY_LEVEL);
    }
  }

  @Test
  public void next_rowLevelWrappedKey_returnsDecryptedRecords() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_wrapped_encrypted.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "txt");
    String dek = generateEncryptedDek();
    String kekUri = generateAeadUri();
    String encryptedEmail = AeadKeyGenerator.encryptString(dek, "fakeemail@fake.com");
    MatchKey email =
        MatchKey.newBuilder()
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue(encryptedEmail)
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder().setEncryptedDek(dek).setKekUri(kekUri).build())
                    .build())
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.newLine();
      writer.write(base64().encode(testRecord.toByteArray()));
      writer.newLine();
    }
    when(mockCfmDataRecordParserFactory.create(any(), any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig,
                generateInternalSchema(schema),
                SuccessMode.ALLOW_PARTIAL_SUCCESS,
                WRAPPED_ENCRYPTION_METADATA));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            micMatchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            WRAPPED_ENCRYPTION_METADATA,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(
              ImmutableList.copyOf(SchemaConverter.convertToColumnAliases(dataReader.getSchema())))
          .containsExactly(
              "metadata",
              "encrypted_dek",
              "kek_uri",
              "email",
              "phone",
              "first_name",
              "last_name",
              "country_code",
              "zip_code",
              ROW_MARKER_COLUMN_NAME);

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(3, 4, 5, 6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(1);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(2);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("fake metadata");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(dek);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(kekUri);
      assertThat(record.getKeyValues(3).getStringValue()).isEqualTo("fakeemail@fake.com");
      assertThat(record.getEncryptedKeyValuesMap().get(3)).isEqualTo(encryptedEmail);
      assertThat(record.getProcessingMetadata().getProtoEncryptionLevel())
          .isEqualTo(ProtoEncryptionLevel.ROW_LEVEL);
    }
  }

  @Test
  public void next_wrappedKeyWithWip_returnsDecryptedRecords() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass()
                        .getResource("testdata/mic_proto_schema_wrapped_with_wip_encrypted.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "txt");
    String dek = generateEncryptedDek();
    String kekUri = generateAeadUri();
    String encryptedEmail = AeadKeyGenerator.encryptString(dek, "fakeemail@fake.com");
    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek(dek)
                            .setKekUri(kekUri)
                            .setWip(TEST_WIP)
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue(encryptedEmail)
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.newLine();
      writer.write(base64().encode(testRecord.toByteArray()));
      writer.newLine();
    }
    when(mockCfmDataRecordParserFactory.create(any(), any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig,
                generateInternalSchema(schema),
                SuccessMode.ALLOW_PARTIAL_SUCCESS,
                WRAPPED_ENCRYPTION_METADATA));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            micMatchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            NO_WIP_WRAPPED_ENCRYPTION_METADATA,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(
              ImmutableList.copyOf(SchemaConverter.convertToColumnAliases(dataReader.getSchema())))
          .containsExactly(
              "metadata",
              "encrypted_dek",
              "kek_uri",
              "wip_provider",
              "email",
              "phone",
              "first_name",
              "last_name",
              "country_code",
              "zip_code",
              ROW_MARKER_COLUMN_NAME);

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(4, 5, 6, 7);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(1);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(2);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getGcpColumnIndices()
                  .getWipProviderIndex())
          .isEqualTo(3);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("fake metadata");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(dek);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(kekUri);
      assertThat(record.getKeyValues(3).getStringValue()).isEqualTo(TEST_WIP);
      assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("fakeemail@fake.com");
      assertThat(record.getEncryptedKeyValuesMap().get(4)).isEqualTo(encryptedEmail);
      assertThat(record.getProcessingMetadata().getProtoEncryptionLevel())
          .isEqualTo(ProtoEncryptionLevel.MATCH_KEY_LEVEL);
    }
  }

  @Test
  public void next_unencrypted_returnsRecords() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_unencrypted.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "txt");
    String fakeEmail = "fakeemail@fake.com";
    MatchKey email =
        MatchKey.newBuilder()
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder().setKey("email").setStringValue(fakeEmail).build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.newLine();
      writer.write(base64().encode(testRecord.toByteArray()));
      writer.newLine();
    }
    when(mockCfmDataRecordParserFactory.create(any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig, generateInternalSchema(schema), SuccessMode.ALLOW_PARTIAL_SUCCESS));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            micMatchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(
              ImmutableList.copyOf(SchemaConverter.convertToColumnAliases(dataReader.getSchema())))
          .containsExactly(
              "metadata",
              "email",
              "phone",
              "first_name",
              "last_name",
              "country_code",
              "zip_code",
              ROW_MARKER_COLUMN_NAME);

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isFalse();
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("fake metadata");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(fakeEmail);
      assertThat(record.getProcessingMetadata().getProtoEncryptionLevel())
          .isEqualTo(ProtoEncryptionLevel.UNSPECIFIED_ENCRYPTION_LEVEL);
    }
  }

  @Test
  public void constructor_unsupportedKeyType_throwsException() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_wrapped_encrypted.json")),
                UTF_8),
            Schema.class);
    MatchConfig matchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass()
                        .getResource(
                            "/com/google/cm/mrp/dataprocessor/testdata/coordinator_encryption_match_config.json")),
                UTF_8),
            MatchConfig.class);

    var file = File.createTempFile("mrp_test", "txt");
    when(mockCfmDataRecordParserFactory.create(any(), any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig, generateInternalSchema(schema), SuccessMode.ALLOW_PARTIAL_SUCCESS));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            matchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            WRAPPED_ENCRYPTION_METADATA,
            aeadCryptoClient)) {
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof JobProcessorException);
      assertThat(((JobProcessorException) e).getErrorCode()).isEqualTo(UNSUPPORTED_ENCRYPTION_TYPE);
    }
  }

  @Test
  public void next_encryptionKeyAtRowAndMatchKey_returnsInvalidEncryptionException()
      throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_wrapped_encrypted.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "txt");
    String dek = generateEncryptedDek();
    String kekUri = generateAeadUri();
    String encryptedEmail = AeadKeyGenerator.encryptString(dek, "fakeemail@fake.com");
    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder().setEncryptedDek(dek).setKekUri(kekUri).build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue(encryptedEmail)
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder().setEncryptedDek(dek).setKekUri(kekUri).build())
                    .build())
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.newLine();
      writer.write(base64().encode(testRecord.toByteArray()));
      writer.newLine();
    }
    when(mockCfmDataRecordParserFactory.create(any(), any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                micMatchConfig,
                generateInternalSchema(schema),
                SuccessMode.ALLOW_PARTIAL_SUCCESS,
                WRAPPED_ENCRYPTION_METADATA));

    try (DataReader dataReader =
        new SerializedProtoDataReader(
            mockCfmDataRecordParserFactory,
            1000,
            new FileInputStream(file),
            schema,
            "test",
            micMatchConfig,
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            WRAPPED_ENCRYPTION_METADATA,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(
              ImmutableList.copyOf(SchemaConverter.convertToColumnAliases(dataReader.getSchema())))
          .containsExactly(
              "metadata",
              "encrypted_dek",
              "kek_uri",
              "email",
              "phone",
              "first_name",
              "last_name",
              "country_code",
              "zip_code",
              ROW_MARKER_COLUMN_NAME);

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(3, 4, 5, 6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(1);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(2);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
    }
  }

  private static final EncryptionMetadata COORDINATOR_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setCoordinatorKeyInfo(
                      CoordinatorKeyInfo.newBuilder()
                          .addCoordinatorInfo(
                              CoordinatorInfo.newBuilder()
                                  .setKeyServiceEndpoint("TEST_ENDPOINT")
                                  .setKmsIdentity("TEST_IDENTITY")
                                  .setKmsWipProvider("TEST_WIP"))
                          .addCoordinatorInfo(
                              CoordinatorInfo.newBuilder()
                                  .setKeyServiceEndpoint("TEST_ENDPOINT_B")
                                  .setKmsIdentity("TEST_IDENTITY_B")
                                  .setKmsWipProvider("TEST_WIP_B"))))
          .build();

  private static final EncryptionMetadata WRAPPED_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setWrappedKeyInfo(
                      WrappedKeyInfo.newBuilder()
                          .setKeyType(KeyType.XCHACHA20_POLY1305)
                          .setGcpWrappedKeyInfo(
                              GcpWrappedKeyInfo.newBuilder().setWipProvider(TEST_WIP))))
          .build();
  private static final EncryptionMetadata NO_WIP_WRAPPED_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setWrappedKeyInfo(
                      WrappedKeyInfo.newBuilder()
                          .setKeyType(KeyType.XCHACHA20_POLY1305)
                          .setGcpWrappedKeyInfo(GcpWrappedKeyInfo.newBuilder().setWipProvider(""))))
          .build();

  private AeadCryptoClient makeAeadCryptoClient() {
    try {
      return new AeadCryptoClient(
          mockAeadProvider, WRAPPED_ENCRYPTION_METADATA.getEncryptionKeyInfo());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private Schema generateInternalSchema(Schema inputSchema) {
    return inputSchema.toBuilder()
        .addColumns(
            Column.newBuilder()
                .setColumnAlias(ROW_MARKER_COLUMN_NAME)
                .setColumnName(ROW_MARKER_COLUMN_NAME)
                .setColumnType(ColumnType.STRING)
                .build())
        .build();
  }
}
