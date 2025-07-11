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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECODING_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.JOB_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_ENCRYPTION_TYPE;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_AUTH_FAILED;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_MISSING_IN_RECORD;
import static com.google.cm.mrp.clients.testutils.AeadProviderTestUtil.realKeysetHandleRead;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.decryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateAeadUri;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateEncryptedDek;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.getDefaultAeadSelector;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridDecrypt;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridEncrypt;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.io.BaseEncoding.base64Url;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.StorageException;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClient;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.clients.cryptoclient.HybridCryptoClient;
import com.google.cm.mrp.dataprocessor.converters.SchemaConverter;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.models.JobParameters;
import com.google.cm.mrp.models.JobParameters.OutputDataLocation;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class CsvDataReaderTest {
  private static final MatchConfig cmMatchConfig = getMatchConfig("customer_match");
  private static final MatchConfig matchConfigWithCoordKey = getCoordinatorEncryptionMatchConfig();
  private static final MatchConfig matchConfigWithWip = getMatchConfigWithWip();
  private static final String TEST_WIP = "TEST_WIP";
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

  private static final JobParameters WRAPPED_KEY_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .setEncodingType(EncodingType.BASE64)
          .setEncryptionMetadata(WRAPPED_ENCRYPTION_METADATA)
          .build();

  private static final JobParameters NO_WIP_WRAPPED_KEY_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .setEncodingType(EncodingType.BASE64)
          .setEncryptionMetadata(
              EncryptionMetadata.newBuilder()
                  .setEncryptionKeyInfo(
                      EncryptionKeyInfo.newBuilder()
                          .setWrappedKeyInfo(
                              WrappedKeyInfo.newBuilder()
                                  .setKeyType(KeyType.XCHACHA20_POLY1305)
                                  .setGcpWrappedKeyInfo(
                                      GcpWrappedKeyInfo.newBuilder().setWipProvider(""))))
                  .build())
          .build();

  private static final JobParameters COORDINATOR_KEY_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .setEncodingType(EncodingType.BASE64)
          .setEncryptionMetadata(
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
                  .build())
          .build();

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private HybridEncryptionKeyService mockHybridEncryptionKeyService;
  @Mock private AeadProvider mockAeadProvider;

  @Mock private CryptoClient mockCryptoClient;
  @Mock private FileInputStream mockFileInputStream;
  private HybridCryptoClient hybridCryptoClient;

  @Test
  public void next_returnsRecords() throws Exception {
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            getClass().getResourceAsStream("testdata/input_data.csv"),
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass()
                            .getResource("/com/google/cm/mrp/dataprocessor/testdata/schema.json")),
                    UTF_8),
                Schema.class),
            "test",
            SuccessMode.ONLY_COMPLETE_SUCCESS)) {

      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("email", "phone", "first_name", "last_name", "zip_code", "country_code");
      DataChunk dataChunk = dataReader.next();

      // with default data chunk size, there should be one data chunk with 7
      // records
      assertEquals(7, dataChunk.records().size());

      assertTrue(
          dataChunk.records().stream()
              .allMatch(
                  dataRecord ->
                      dataRecord.getKeyValuesCount() == 6
                          && dataRecord.getKeyValues(0).hasStringValue()
                          && "email".equals(dataRecord.getKeyValues(0).getKey())
                          && dataRecord.getKeyValues(1).hasStringValue()
                          && "phone".equals(dataRecord.getKeyValues(1).getKey())
                          && dataRecord.getKeyValues(2).hasStringValue()
                          && "first_name".equals(dataRecord.getKeyValues(2).getKey())
                          && dataRecord.getKeyValues(3).hasStringValue()
                          && "last_name".equals(dataRecord.getKeyValues(3).getKey())
                          && dataRecord.getKeyValues(4).hasStringValue()
                          && "zip_code".equals(dataRecord.getKeyValues(4).getKey())
                          && dataRecord.getKeyValues(5).hasStringValue()
                          && "country_code".equals(dataRecord.getKeyValues(5).getKey())));
    }
  }

  @Test
  public void next_returnsCorrectChunkSize() throws Exception {
    try (DataReader dataReader =
        new CsvDataReader(
            5,
            getClass().getResourceAsStream("testdata/input_data.csv"),
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass()
                            .getResource("/com/google/cm/mrp/dataprocessor/testdata/schema.json")),
                    UTF_8),
                Schema.class),
            "test",
            SuccessMode.ONLY_COMPLETE_SUCCESS)) {

      DataChunk dataChunk = dataReader.next();

      // with data chunk size of 5, the first data chunk should have 5 records
      assertEquals(5, dataChunk.records().size());

      assertTrue(
          dataChunk.records().stream()
              .allMatch(
                  dataRecord ->
                      dataRecord.getKeyValuesCount() == 6
                          && dataRecord.getKeyValues(0).hasStringValue()
                          && "email".equals(dataRecord.getKeyValues(0).getKey())
                          && dataRecord.getKeyValues(1).hasStringValue()
                          && "phone".equals(dataRecord.getKeyValues(1).getKey())
                          && dataRecord.getKeyValues(2).hasStringValue()
                          && "first_name".equals(dataRecord.getKeyValues(2).getKey())
                          && dataRecord.getKeyValues(3).hasStringValue()
                          && "last_name".equals(dataRecord.getKeyValues(3).getKey())
                          && dataRecord.getKeyValues(4).hasStringValue()
                          && "zip_code".equals(dataRecord.getKeyValues(4).getKey())
                          && dataRecord.getKeyValues(5).hasStringValue()
                          && "country_code".equals(dataRecord.getKeyValues(5).getKey())));

      dataChunk = dataReader.next();

      // the second data chunk should have 7 - 5 = 2 records
      assertEquals(2, dataChunk.records().size());

      assertTrue(
          dataChunk.records().stream()
              .allMatch(
                  dataRecord ->
                      dataRecord.getKeyValuesCount() == 6
                          && dataRecord.getKeyValues(0).hasStringValue()
                          && "email".equals(dataRecord.getKeyValues(0).getKey())
                          && dataRecord.getKeyValues(1).hasStringValue()
                          && "phone".equals(dataRecord.getKeyValues(1).getKey())
                          && dataRecord.getKeyValues(2).hasStringValue()
                          && "first_name".equals(dataRecord.getKeyValues(2).getKey())
                          && dataRecord.getKeyValues(3).hasStringValue()
                          && "last_name".equals(dataRecord.getKeyValues(3).getKey())
                          && dataRecord.getKeyValues(4).hasStringValue()
                          && "zip_code".equals(dataRecord.getKeyValues(4).getKey())
                          && dataRecord.getKeyValues(5).hasStringValue()
                          && "country_code".equals(dataRecord.getKeyValues(5).getKey())));
    }
  }

  @Test
  public void next_whenTooFewColumnsInRowThenThrows() throws Exception {
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            Objects.requireNonNull(
                getClass().getResourceAsStream("testdata/too_few_columns_in_record_row.csv")),
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass()
                            .getResource("/com/google/cm/mrp/dataprocessor/testdata/schema.json")),
                    UTF_8),
                Schema.class),
            "test",
            SuccessMode.ONLY_COMPLETE_SUCCESS)) {

      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, dataReader::next);

      assertThat(ex)
          .hasMessageThat()
          .isEqualTo("Index for header 'phone' is 1 but CSVRecord only has 1 values!");
    }
  }

  @Test
  public void next_whenInvalidDataInRowThenThrows() throws Exception {
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            Objects.requireNonNull(
                getClass().getResourceAsStream("testdata/invalid_quotes_in_records.csv")),
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass()
                            .getResource("/com/google/cm/mrp/dataprocessor/testdata/schema.json")),
                    UTF_8),
                Schema.class),
            "test",
            SuccessMode.ONLY_COMPLETE_SUCCESS)) {

      JobProcessorException ex = assertThrows(JobProcessorException.class, dataReader::next);

      assertThat(ex)
          .hasMessageThat()
          .isEqualTo("Could not read CSV file, make sure input data is correct.");
    }
  }

  @Test
  public void next_whenInvalidDataInFirstRowThenThrows() throws Exception {
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            Objects.requireNonNull(
                getClass().getResourceAsStream("testdata/invalid_quotes_first_record.csv")),
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass()
                            .getResource("/com/google/cm/mrp/dataprocessor/testdata/schema.json")),
                    UTF_8),
                Schema.class),
            "test",
            SuccessMode.ONLY_COMPLETE_SUCCESS)) {

      JobProcessorException ex = assertThrows(JobProcessorException.class, dataReader::hasNext);

      assertThat(ex)
          .hasMessageThat()
          .isEqualTo("Could not read CSV file, make sure input data is correct.");
    }
  }

  @Test
  public void next_whenGcsReadErrorThenRetry() throws Exception {
    when(mockFileInputStream.read(any(), anyInt(), anyInt()))
        .thenThrow(new IOException("GCS error", new StorageException(1, "Test Error")));
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            mockFileInputStream,
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass()
                            .getResource(
                                "/com/google/cm/mrp/dataprocessor/testdata/schema_no_header.json")),
                    UTF_8),
                Schema.class),
            "test",
            SuccessMode.ONLY_COMPLETE_SUCCESS)) {

      JobProcessorException ex = assertThrows(JobProcessorException.class, dataReader::next);

      assertThat(ex).hasMessageThat().isEqualTo("Could not read CSV file from input stream.");
      assertThat(ex.isRetriable()).isTrue();
    }
  }

  @Test
  public void next_encryptedReturnsDecryptedRecords() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockAeadProvider.readKeysetHandle(any(), any())).thenAnswer(realKeysetHandleRead());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i);
        writer.write(String.join(",", values));
        writer.newLine();
      }
      String[] values = {"", "", " ", " ", "", "", generateEncryptedDek(), generateAeadUri()};
      writer.write(String.join(",", values));
      writer.newLine();
    }
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0, 1, 2, 3);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(7);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(11);
      for (int i = 0; i < dataRecords.size() - 1; ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(i + "@google.com");
        assertThat(record.getKeyValues(1).getStringValue()).isEqualTo("999-999-999" + i);
        assertThat(record.getKeyValues(2).getStringValue()).isEqualTo("first_name" + i);
        assertThat(record.getKeyValues(3).getStringValue()).isEqualTo("last_name" + i);
        assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("9999" + i);
        assertThat(record.getKeyValues(5).getStringValue()).isEqualTo("US");
        // verify KEK
        assertThat(record.getKeyValues(7).getStringValue()).startsWith("gcp-kms://");
        // verify DEK exists
        assertThat(record.getKeyValues(6).hasStringValue()).isTrue();
        var dek = record.getKeyValues(6).getStringValue();
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(4);
        // verify encryptedRecords decrypt correctly using DEK
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(0)))
            .isEqualTo(i + "@google.com");
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(1)))
            .isEqualTo("999-999-999" + i);
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(2)))
            .isEqualTo("first_name" + i);
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(3)))
            .isEqualTo("last_name" + i);
      }
      var record = dataRecords.get(10);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(" ");
      assertThat(record.getKeyValues(3).getStringValue()).isEqualTo(" ");
      assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(5).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(7).getStringValue()).startsWith("gcp-kms://");
      assertThat(record.getKeyValues(6).hasStringValue()).isTrue();
      var dek = record.getKeyValues(6).getStringValue();
      assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
    }
  }

  @Test
  public void next_encryptedHexEncoded_returnsDecryptedRecords() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockAeadProvider.readKeysetHandle(any(), any())).thenAnswer(realKeysetHandleRead());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i, EncodingType.HEX);
        writer.write(String.join(",", values));
        writer.newLine();
      }
      String[] values = {"", "", " ", " ", "", "", generateEncryptedDek(), generateAeadUri()};
      writer.write(String.join(",", values));
      writer.newLine();
    }
    JobParameters testParams =
        JobParameters.builder()
            .setJobId("test")
            .setDataLocation(DataLocation.getDefaultInstance())
            .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
            .setEncodingType(EncodingType.HEX)
            .setEncryptionMetadata(WRAPPED_ENCRYPTION_METADATA)
            .build();
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            testParams,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0, 1, 2, 3);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(7);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(11);
      for (int i = 0; i < dataRecords.size() - 1; ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(i + "@google.com");
        assertThat(record.getKeyValues(1).getStringValue()).isEqualTo("999-999-999" + i);
        assertThat(record.getKeyValues(2).getStringValue()).isEqualTo("first_name" + i);
        assertThat(record.getKeyValues(3).getStringValue()).isEqualTo("last_name" + i);
        assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("9999" + i);
        assertThat(record.getKeyValues(5).getStringValue()).isEqualTo("US");
        // verify KEK
        assertThat(record.getKeyValues(7).getStringValue()).startsWith("gcp-kms://");
        // verify DEK exists
        assertThat(record.getKeyValues(6).hasStringValue()).isTrue();
        var dek = record.getKeyValues(6).getStringValue();
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(4);
        // verify encryptedRecords decrypt correctly using DEK
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(0)))
            .isEqualTo(i + "@google.com");
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(1)))
            .isEqualTo("999-999-999" + i);
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(2)))
            .isEqualTo("first_name" + i);
        assertThat(decryptString(dek, record.getEncryptedKeyValuesOrThrow(3)))
            .isEqualTo("last_name" + i);
      }
      var record = dataRecords.get(10);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(" ");
      assertThat(record.getKeyValues(3).getStringValue()).isEqualTo(" ");
      assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(5).getStringValue()).isEqualTo("");
      assertThat(record.getKeyValues(7).getStringValue()).startsWith("gcp-kms://");
      assertThat(record.getKeyValues(6).hasStringValue()).isTrue();
      var dek = record.getKeyValues(6).getStringValue();
      assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
    }
  }

  @Test
  public void next_encryptedMissingKek_throws() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i);
        values[7] = " "; // KEK index
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      JobProcessorException ex = assertThrows(JobProcessorException.class, dataReader::next);

      assertThat(ex.getErrorCode()).isEqualTo(KEK_MISSING_IN_RECORD);
      assertThat(ex.getMessage()).isEqualTo("Error parsing encryption keys.");
      assertThat(ex.isRetriable()).isFalse();
    }
  }

  @Test
  public void next_encryptedParamNotInMatchConfig_throws() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i);
        values[7] = " "; // KEK index
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }

    JobProcessorException ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new CsvDataReader(
                        1000,
                        new FileInputStream(file),
                        schema,
                        "test",
                        COORDINATOR_KEY_PARAMS,
                        cmMatchConfig.getEncryptionKeyColumns(),
                        SuccessMode.ONLY_COMPLETE_SUCCESS,
                        mockCryptoClient)
                    .next());
    assertThat(ex.getErrorCode()).isEqualTo(UNSUPPORTED_ENCRYPTION_TYPE);
    assertThat(ex.getMessage())
        .isEqualTo("Match config does not support coordinator key parameter from schema.");
    assertThat(ex.isRetriable()).isFalse();
  }

  @Test
  public void next_encryptedWithRowWip_returnsDecryptedRecords() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockAeadProvider.readKeysetHandle(any(), any())).thenAnswer(realKeysetHandleRead());
    AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_with_wip_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i, TEST_WIP);
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            NO_WIP_WRAPPED_KEY_PARAMS,
            matchConfigWithWip.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            aeadCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek", "Wip");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0, 1, 2, 3);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(7);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getGcpColumnIndices()
                  .getWipProviderIndex())
          .isEqualTo(8);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(10);
      for (int i = 0; i < dataRecords.size(); ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(i + "@google.com");
        assertThat(record.getKeyValues(1).getStringValue()).isEqualTo("999-999-999" + i);
        assertThat(record.getKeyValues(2).getStringValue()).isEqualTo("first_name" + i);
        assertThat(record.getKeyValues(3).getStringValue()).isEqualTo("last_name" + i);
        assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("9999" + i);
        assertThat(record.getKeyValues(5).getStringValue()).isEqualTo("US");
        // verify DEK exists
        assertThat(record.getKeyValues(6).hasStringValue()).isTrue();
        // verify KEK
        assertThat(record.getKeyValues(7).getStringValue()).startsWith("gcp-kms://");
        // verify WIP
        assertThat(record.getKeyValues(8).getStringValue()).isEqualTo(TEST_WIP);
      }
    }
  }

  @Test
  public void next_encryptedWithoutWipInRequestOrInMatchConfig_throws() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_with_wip_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i, TEST_WIP);
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }

    JobProcessorException ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new CsvDataReader(
                        1000,
                        new FileInputStream(file),
                        schema,
                        "test",
                        NO_WIP_WRAPPED_KEY_PARAMS,
                        cmMatchConfig.getEncryptionKeyColumns(),
                        SuccessMode.ONLY_COMPLETE_SUCCESS,
                        mockCryptoClient)
                    .next());

    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_COLUMNS_CONFIG_ERROR);
    assertThat(ex.getMessage())
        .isEqualTo("WIP missing in request and no WIP column in match config.");
  }

  @Test
  public void next_encryptedMissingWipInRequestOrInData_throws() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_with_wip_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i, "");
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }

    JobProcessorException ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new CsvDataReader(
                        1000,
                        new FileInputStream(file),
                        schema,
                        "test",
                        NO_WIP_WRAPPED_KEY_PARAMS,
                        matchConfigWithWip.getEncryptionKeyColumns(),
                        SuccessMode.ONLY_COMPLETE_SUCCESS,
                        mockCryptoClient)
                    .next());

    assertThat(ex.getErrorCode()).isEqualTo(WIP_MISSING_IN_RECORD);
    assertThat(ex.getMessage()).isEqualTo("Error parsing encryption keys.");
  }

  @Test
  public void next_encryptedBadWipAuth_throws() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i);
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
    when(mockCryptoClient.decrypt(any(), anyString(), any(EncodingType.class)))
        .thenThrow(new CryptoClientException(WIP_AUTH_FAILED));
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      JobProcessorException ex = assertThrows(JobProcessorException.class, dataReader::next);

      assertThat(ex.getErrorCode()).isEqualTo(WIP_AUTH_FAILED);
      assertThat(ex.isRetriable()).isFalse();
    }
  }

  @Test
  public void next_partialSuccessEnabledEncryptedReturnsDekDecryptionRowLevelError()
      throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        var dek = generateEncryptedDek();
        String testKek = generateAeadUri();
        String encryptedEmail = i + "@google.com";
        String encryptedPhone = "999-999-999" + i;
        String encryptedFirstName = "first_name" + i;
        String encryptedLastName = "last_name" + i;
        String zip = "9999" + i;
        String country = "US";
        String[] values = {
          encryptedEmail,
          encryptedPhone,
          encryptedFirstName,
          encryptedLastName,
          zip,
          country,
          dek,
          testKek
        };
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
    when(mockCryptoClient.decrypt(any(), anyString(), any(EncodingType.class)))
        .thenThrow(new CryptoClientException(DEK_DECRYPTION_ERROR));
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0, 1, 2, 3);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getEncryptedDekColumnIndex())
          .isEqualTo(6);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getWrappedKeyColumnIndices()
                  .getKekUriColumnIndex())
          .isEqualTo(7);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(10);
      for (int i = 0; i < dataRecords.size(); ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(i + "@google.com");
        assertThat(record.getKeyValues(1).getStringValue()).isEqualTo("999-999-999" + i);
        assertThat(record.getKeyValues(2).getStringValue()).isEqualTo("first_name" + i);
        assertThat(record.getKeyValues(3).getStringValue()).isEqualTo("last_name" + i);
        assertThat(record.getKeyValues(4).getStringValue()).isEqualTo("9999" + i);
        assertThat(record.getKeyValues(5).getStringValue()).isEqualTo("US");
        // verify KEK
        assertThat(record.getKeyValues(7).getStringValue()).startsWith("gcp-kms://");
        // verify DEK exists
        assertThat(record.getKeyValues(6).hasStringValue()).isTrue();
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
        assertThat(record.hasErrorCode()).isTrue();
        assertThat(record.getErrorCode()).isEqualTo(DEK_DECRYPTION_ERROR);
      }
    }
  }

  @Test
  public void next_partialSuccessEnabledMissingKek_returnsRowLevelError() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i);
        values[7] = " "; // KEK index
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(10);
      for (int i = 0; i < dataRecords.size(); ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValuesCount()).isEqualTo(8);
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
        assertThat(record.hasErrorCode()).isTrue();
        assertThat(record.getErrorCode()).isEqualTo(JobResultCode.KEK_MISSING_IN_RECORD);
      }
    }
  }

  @Test
  public void next_partialSuccessEnabledMissingDek_returnsRowLevelError() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i);
        values[6] = " "; // DEK index
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(10);
      for (int i = 0; i < dataRecords.size(); ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValuesCount()).isEqualTo(8);
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
        assertThat(record.hasErrorCode()).isTrue();
        assertThat(record.getErrorCode()).isEqualTo(JobResultCode.DEK_MISSING_IN_RECORD);
      }
    }
  }

  @Test
  public void next_partialSuccessEnabledMissingWip_returnsRowLevelError() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_with_wip_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        // empty wip
        String[] values = generateEncryptedFields(i, /* wip= */ " ");
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            NO_WIP_WRAPPED_KEY_PARAMS,
            matchConfigWithWip.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek", "Wip");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(10);
      for (int i = 0; i < dataRecords.size(); ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValuesCount()).isEqualTo(9);
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
        assertThat(record.hasErrorCode()).isTrue();
        assertThat(record.getErrorCode()).isEqualTo(WIP_MISSING_IN_RECORD);
      }
    }
  }

  @Test
  public void next_partialSuccessEnabledWrongWip_returnsRowLevelError() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_with_wip_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        String[] values = generateEncryptedFields(i, TEST_WIP);
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
    when(mockCryptoClient.decrypt(any(), anyString(), any(EncodingType.class)))
        .thenThrow(new CryptoClientException(WIP_AUTH_FAILED));
    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS, // jobLevel WIP
            matchConfigWithWip.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek", "Wip");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasWrappedEncryptionKeys())
          .isFalse();
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(10);
      for (int i = 0; i < dataRecords.size(); ++i) {
        var record = dataRecords.get(i);
        assertThat(record.getKeyValuesCount()).isEqualTo(9);
        assertThat(record.getEncryptedKeyValuesCount()).isEqualTo(0);
        assertThat(record.hasErrorCode()).isTrue();
        assertThat(record.getErrorCode()).isEqualTo(JobResultCode.WIP_AUTH_FAILED);
      }
    }
  }

  @Test
  public void next_partialSuccessEnabledEncryptedReturnsExceptionPassesThroughErrorCode()
      throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/wrapped_key_encrypted_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test_row_error", "csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      for (int i = 0; i < 10; ++i) {
        var dek = generateEncryptedDek();
        String testKek = generateAeadUri();
        String encryptedEmail = i + "@google.com";
        String encryptedPhone = "999-999-999" + i;
        String encryptedFirstName = "first_name" + i;
        String encryptedLastName = "last_name" + i;
        String zip = "9999" + i;
        String country = "US";
        String[] values = {
          encryptedEmail,
          encryptedPhone,
          encryptedFirstName,
          encryptedLastName,
          zip,
          country,
          dek,
          testKek
        };
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
    when(mockCryptoClient.decrypt(any(), anyString(), any(EncodingType.class)))
        .thenThrow(new CryptoClientException(JOB_DECRYPTION_ERROR));

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            WRAPPED_KEY_PARAMS,
            cmMatchConfig.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            mockCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly(
              "Email", "Phone", "FirstName", "LastName", "Zip", "Country", "Dek", "Kek");

      var ex = assertThrows(JobProcessorException.class, dataReader::next);

      assertThat(ex.getErrorCode()).isEqualTo(JOB_DECRYPTION_ERROR);
    }
  }

  @Test
  public void next_gtagEncrypted_returnsDecryptedRecords() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/gtag_coordinator_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    String testCoordinatorKey = "test123";
    String metadataString = "metadataString";
    String groupPiiString = "<em>.<hashed(email1)>";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      var encrypter = getDefaultHybridEncrypt();
      String encryptedGroupPii = encryptString(encrypter, groupPiiString);
      String[] values = {
        encryptedGroupPii, testCoordinatorKey, metadataString,
      };
      writer.write(String.join(",", values));
      writer.newLine();
    }
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            COORDINATOR_KEY_PARAMS,
            matchConfigWithCoordKey.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("GroupPiiString", "KeyID", "Metadata");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasCoordinatorKey()).isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getCoordinatorKeyColumnIndices()
                  .getCoordinatorKeyColumnIndex())
          .isEqualTo(1);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(groupPiiString);
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(testCoordinatorKey);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(metadataString);
    }
  }

  @Test
  public void next_hybridEncryptedPartialSuccessMissingCoordKey_returnsError() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/gtag_coordinator_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    String metadataString = "metadataString";
    String groupPiiString = "<em>.<hashed(email1)>";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      var encrypter = getDefaultHybridEncrypt();
      String encryptedGroupPii = encryptString(encrypter, groupPiiString);
      String[] values = {
        encryptedGroupPii, "", metadataString,
      };
      writer.write(String.join(",", values));
      writer.newLine();
    }
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            COORDINATOR_KEY_PARAMS,
            matchConfigWithCoordKey.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("GroupPiiString", "KeyID", "Metadata");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasCoordinatorKey()).isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValuesCount()).isEqualTo(3);
      assertThat(record.hasErrorCode()).isTrue();
      assertThat(record.getErrorCode()).isEqualTo(JobResultCode.COORDINATOR_KEY_MISSING_IN_RECORD);
    }
  }

  @Test
  public void next_encodedWithPaddingEncryptedReturnsExpectedRecords() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_encoded_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    String testCoordinatorKey = "test123";
    String rowMetadata = "metadata";
    String groupPiiString = "<em>.<hashed(email1)>";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      var encrypter = getDefaultHybridEncrypt();
      String encryptedGroupPii = encryptString(encrypter, groupPiiString);
      String[] values = {
        base64Url().encode(base64().decode(encryptedGroupPii)), testCoordinatorKey, rowMetadata
      };
      writer.write(String.join(",", values));
      writer.newLine();
    }
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            COORDINATOR_KEY_PARAMS,
            matchConfigWithCoordKey.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("gtag_grouped_pii", "coordinator_key_id", "row_metadata");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasCoordinatorKey()).isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getCoordinatorKeyColumnIndices()
                  .getCoordinatorKeyColumnIndex())
          .isEqualTo(1);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(groupPiiString);
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(testCoordinatorKey);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(rowMetadata);
    }
  }

  @Test
  public void next_encodedWithoutPaddingEncryptedReturnsExpectedRecords() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_encoded_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    String testCoordinatorKey = "test123";
    String rowMetadata = "metadata";
    String groupPiiString = "<em>.<hashed(email1)>";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      var encrypter = getDefaultHybridEncrypt();
      String encryptedGroupPii = encryptString(encrypter, groupPiiString);
      String[] values = {
        base64Url().omitPadding().encode(base64().decode(encryptedGroupPii)),
        testCoordinatorKey,
        rowMetadata
      };
      writer.write(String.join(",", values));
      writer.newLine();
    }
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            COORDINATOR_KEY_PARAMS,
            matchConfigWithCoordKey.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("gtag_grouped_pii", "coordinator_key_id", "row_metadata");

      DataChunk dataChunk = dataReader.next();

      assertThat(dataChunk.encryptionColumns().isPresent()).isTrue();
      var encryptionColumns = dataChunk.encryptionColumns().get();
      assertThat(encryptionColumns.getDataRecordEncryptionKeys().hasCoordinatorKey()).isFalse();
      assertThat(encryptionColumns.getEncryptedColumnIndicesList()).containsExactly(0);
      assertThat(
              encryptionColumns
                  .getEncryptionKeyColumnIndices()
                  .getCoordinatorKeyColumnIndices()
                  .getCoordinatorKeyColumnIndex())
          .isEqualTo(1);
      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.getKeyValues(0).getStringValue()).isEqualTo(groupPiiString);
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(testCoordinatorKey);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(rowMetadata);
    }
  }

  @Test
  public void next_oneEncryptedEncodedWithWrongEncoding_allowPartialSuccess_setsErrorCode()
      throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_encoded_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    String testCoordinatorKey = "test123";
    String rowMetadata = "metadata";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      String[] values = {"invalid_base_64url/string", testCoordinatorKey, rowMetadata};
      writer.write(String.join(",", values));
      writer.newLine();
    }
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            COORDINATOR_KEY_PARAMS,
            matchConfigWithCoordKey.getEncryptionKeyColumns(),
            SuccessMode.ALLOW_PARTIAL_SUCCESS,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("gtag_grouped_pii", "coordinator_key_id", "row_metadata");

      DataChunk dataChunk = dataReader.next();

      var dataRecords = dataChunk.records();
      assertThat(dataRecords).hasSize(1);
      var record = dataRecords.get(0);
      assertThat(record.hasErrorCode()).isTrue();
      assertThat(record.getErrorCode()).isEqualTo(DECODING_ERROR);
      assertThat(record.getKeyValues(1).getStringValue()).isEqualTo(testCoordinatorKey);
      assertThat(record.getKeyValues(2).getStringValue()).isEqualTo(rowMetadata);
    }
  }

  @Test
  public void next_oneEncryptedEncodedWithWrongEncoding_completeSuccess_throwsException()
      throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_ecw_encoded_schema.json")),
                UTF_8),
            Schema.class);
    var file = File.createTempFile("mrp_test", "csv");
    String testCoordinatorKey = "test123";
    String rowMetadata = "metadata";
    String groupPiiString = "<em>.<hashed(email1)>";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.join(",", SchemaConverter.convertToColumnNames(schema)));
      writer.newLine();
      var encrypter = getDefaultHybridEncrypt();
      String encryptedGroupPii = encryptString(encrypter, groupPiiString);
      String[] values = {"invalid_base_64url/string", testCoordinatorKey, rowMetadata};
      writer.write(String.join(",", values));
      writer.newLine();
    }
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    hybridCryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);

    try (DataReader dataReader =
        new CsvDataReader(
            1000,
            new FileInputStream(file),
            schema,
            "test",
            COORDINATOR_KEY_PARAMS,
            matchConfigWithCoordKey.getEncryptionKeyColumns(),
            SuccessMode.ONLY_COMPLETE_SUCCESS,
            hybridCryptoClient)) {
      assertThat(dataReader.getSchema()).isNotNull();
      assertThat(ImmutableList.copyOf(SchemaConverter.convertToColumnNames(dataReader.getSchema())))
          .containsExactly("gtag_grouped_pii", "coordinator_key_id", "row_metadata");

      var ex = assertThrows(JobProcessorException.class, dataReader::next);
      assertThat(ex.getMessage()).contains("Failed to decode column \"gtag_grouped_pii\"");
    }
  }

  private AeadCryptoClient makeAeadCryptoClient() {
    try {
      return new AeadCryptoClient(
          mockAeadProvider, WRAPPED_ENCRYPTION_METADATA.getEncryptionKeyInfo());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static MatchConfig getCoordinatorEncryptionMatchConfig() {
    try {
      return ProtoUtils.getProtoFromJson(
          Resources.toString(
              Objects.requireNonNull(
                  CsvDataReaderTest.class.getResource(
                      "/com/google/cm/mrp/dataprocessor/testdata/coordinator_encryption_match_config.json")),
              UTF_8),
          MatchConfig.class);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static MatchConfig getMatchConfigWithWip() {
    try {
      return ProtoUtils.getProtoFromJson(
          Resources.toString(
              Objects.requireNonNull(
                  CsvDataReaderTest.class.getResource(
                      "/com/google/cm/mrp/dataprocessor/testdata/wrapped_key_with_wip_config.json")),
              UTF_8),
          MatchConfig.class);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String[] generateEncryptedFields(int idx) throws Exception {
    return generateEncryptedFields(idx, EncodingType.BASE64);
  }

  private static String[] generateEncryptedFields(int idx, EncodingType encodingType)
      throws Exception {
    var dek = generateEncryptedDek();
    String testKek = generateAeadUri();
    String encryptedEmail = encryptString(dek, idx + "@google.com", encodingType);
    String encryptedPhone = encryptString(dek, "999-999-999" + idx, encodingType);
    String encryptedFirstName = encryptString(dek, "first_name" + idx, encodingType);
    String encryptedLastName = encryptString(dek, "last_name" + idx, encodingType);
    String zip = "9999" + idx;
    String country = "US";
    return new String[] {
      encryptedEmail,
      encryptedPhone,
      encryptedFirstName,
      encryptedLastName,
      zip,
      country,
      dek,
      testKek
    };
  }

  private static String[] generateEncryptedFields(int i, String wip) throws Exception {
    String[] fields = generateEncryptedFields(i);
    String[] newFields = Arrays.copyOf(fields, fields.length + 1);
    newFields[fields.length] = wip;
    return newFields;
  }
}
