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

package com.google.cm.mrp.dataprocessor;

import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
import static com.google.cm.mrp.dataprocessor.BlobStoreStreamDataSource.FOLDER_DELIMITER;
import static com.google.cm.mrp.dataprocessor.BlobStoreStreamDataSource.SCHEMA_DEFINITION_FILE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.storage.StorageException;
import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.MatchConfigProvider;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.DataFormat;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClient;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserFactory;
import com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl;
import com.google.cm.mrp.dataprocessor.readers.CsvDataReader;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.readers.SerializedProtoDataReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class BlobStoreStreamDataSourceTest {

  private static final String INPUT_BUCKET = "input_bucket";
  private static final String INPUT_PREFIX = "input_prefix";
  private static final String INPUT_FILE = "input.csv";
  private static final DataOwner.DataLocation FAKE_DATA_LOCATION =
      DataOwner.DataLocation.newBuilder()
          .setInputDataBucketName(INPUT_BUCKET)
          .setInputDataBlobPrefix(INPUT_PREFIX)
          .setIsStreamed(true)
          .build();
  private static final DataLocation BLOB_STORAGE_DATA_LOCATION =
      DataLocation.ofBlobStoreDataLocation(
          DataLocation.BlobStoreDataLocation.create(INPUT_BUCKET, INPUT_PREFIX + FOLDER_DELIMITER));
  private static final DataLocation SCHEMA_DATA_LOCATION =
      DataLocation.ofBlobStoreDataLocation(
          DataLocation.BlobStoreDataLocation.create(
              INPUT_BUCKET, INPUT_PREFIX + FOLDER_DELIMITER + SCHEMA_DEFINITION_FILE));
  private static final DataLocation INPUT_DATA_LOCATION =
      DataLocation.ofBlobStoreDataLocation(
          DataLocation.BlobStoreDataLocation.create(
              INPUT_BUCKET, INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE));
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private BlobStorageClient mockBlobStorageClient;
  @Mock private DataReaderFactory dataReaderFactory;
  @Mock private InputStream mockInputStream;
  private CsvDataReader fakeCsvDataReader;
  private SerializedProtoDataReader fakeSerializedProtoDataReader;
  @Mock private MetricClient mockMetricClient;
  @Mock private AeadProvider mockAeadProvider;

  @Captor private ArgumentCaptor<DataLocation> dataLocationArgumentCaptor;
  private final AeadCryptoClient aeadCryptoClient = makeAeadCryptoClient();

  private BlobStoreStreamDataSource blobStoreStreamDataSource;
  private MatchConfig matchConfig;
  @Mock private ConfidentialMatchDataRecordParserFactory mockCfmDataRecordParserFactory;

  @Before
  public void setupDataReader() throws Exception {
    fakeCsvDataReader = getFakeCsvReader("fake");
    fakeSerializedProtoDataReader = getFakeSerializedProtoReader("fake");
  }

  @Test
  public void next_whenBlobsFoundThenReturnsReaders() throws Exception {
    setUp(DataFormat.CSV);
    when(dataReaderFactory.createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            startsWith(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            any(SuccessMode.class)))
        .thenReturn(fakeCsvDataReader);

    DataReader result = blobStoreStreamDataSource.next();

    assertEquals(fakeCsvDataReader, result);
    verify(mockBlobStorageClient, times(2)).getBlob(any(), any());
    verify(mockBlobStorageClient).listBlobs(any(), any());
    verify(dataReaderFactory)
        .createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            startsWith(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            any(SuccessMode.class));
    verifyNoMoreInteractions(mockBlobStorageClient, dataReaderFactory, mockInputStream);
  }

  @Test
  public void next_whenProtoSchemaBlobsFoundThenReturnsProtoReaders() throws Exception {
    setUp(DataFormat.SERIALIZED_PROTO);
    when(dataReaderFactory.createProtoDataReader(
            eq(mockInputStream),
            any(Schema.class),
            startsWith(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            eq(matchConfig),
            any(SuccessMode.class)))
        .thenReturn(fakeSerializedProtoDataReader);

    DataReader result = blobStoreStreamDataSource.next();

    assertEquals(fakeSerializedProtoDataReader, result);
    verify(mockBlobStorageClient, times(2)).getBlob(any(), any());
    verify(mockBlobStorageClient).listBlobs(any(), any());
    verify(dataReaderFactory)
        .createProtoDataReader(
            eq(mockInputStream),
            any(Schema.class),
            startsWith(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            eq(matchConfig),
            any(SuccessMode.class));
    verifyNoMoreInteractions(mockBlobStorageClient, dataReaderFactory, mockInputStream);
  }

  @Test
  public void next_whenLargeCountOfBlobsFoundReturnDifferentBlobs() throws Exception {
    int numFiles = 100_000;
    setUp(FAKE_DATA_LOCATION, numFiles, DataFormat.CSV);
    when(dataReaderFactory.createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            startsWith(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            any(SuccessMode.class)))
        .thenAnswer(
            invocation -> {
              String name = invocation.getArgument(2);
              return getFakeCsvReader(name);
            });

    List<CompletableFuture<DataReader>> futures = new ArrayList<>();

    for (int i = 0; i < numFiles; i++) {
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                if (blobStoreStreamDataSource.hasNext()) {
                  return blobStoreStreamDataSource.next();
                }
                return null;
              }));
    }

    List<DataReader> results =
        futures.stream().map(CompletableFuture::join).collect(toImmutableList());

    // verify blobs
    Set<String> blobNames =
        results.stream().map(DataReader::getName).collect(ImmutableSet.toImmutableSet());
    assertThat(blobNames.size()).isEqualTo(numFiles);

    verify(mockBlobStorageClient, times(numFiles + 1)).getBlob(any(), any());
    verify(mockBlobStorageClient).listBlobs(any(), any());
    verify(dataReaderFactory, times(numFiles))
        .createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            startsWith(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            any(SuccessMode.class));
    verifyNoMoreInteractions(mockBlobStorageClient, dataReaderFactory, mockInputStream);
  }

  @Test
  public void next_whenListedBlobsFoundThenReturnsReaders() throws Exception {
    DataOwner.DataLocation dataLocation =
        DataOwner.DataLocation.newBuilder()
            .setInputDataBucketName(INPUT_BUCKET)
            .addAllInputDataBlobPaths(List.of(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE))
            .setInputSchemaPath(INPUT_PREFIX + FOLDER_DELIMITER + SCHEMA_DEFINITION_FILE)
            .setIsStreamed(true)
            .build();
    setUp(dataLocation, DataFormat.CSV);
    when(dataReaderFactory.createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            eq(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            any(SuccessMode.class)))
        .thenReturn(fakeCsvDataReader);

    DataReader result = blobStoreStreamDataSource.next();

    assertEquals(fakeCsvDataReader, result);
    verify(mockBlobStorageClient, times(2)).getBlob(any(), any());
    verify(mockBlobStorageClient, times(0)).listBlobs(any(), any());
    verify(dataReaderFactory)
        .createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            eq(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            any(SuccessMode.class));
    verifyNoMoreInteractions(mockBlobStorageClient, dataReaderFactory, mockInputStream);
  }

  @Test
  public void next_whenBlobsFoundWithCryptoClientThenReturnsReaderWithCryptoClient()
      throws Exception {
    matchConfig = MatchConfigProvider.getMatchConfig("customer_match");
    when(mockBlobStorageClient.listBlobs(BLOB_STORAGE_DATA_LOCATION, Optional.empty()))
        .thenReturn(
            ImmutableList.of(
                INPUT_PREFIX + FOLDER_DELIMITER + SCHEMA_DEFINITION_FILE,
                INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE));
    when(mockBlobStorageClient.getBlob(INPUT_DATA_LOCATION, Optional.empty()))
        .thenReturn(mockInputStream);
    InputStream schemaInputStream =
        Objects.requireNonNull(
            getClass()
                .getResourceAsStream("/com/google/cm/mrp/dataprocessor/testdata/schema.json"));
    when(mockBlobStorageClient.getBlob(SCHEMA_DATA_LOCATION, Optional.empty()))
        .thenReturn(schemaInputStream);
    when(dataReaderFactory.createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            eq(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            eq(matchConfig.getEncryptionKeyColumns()),
            any(SuccessMode.class),
            any(EncryptionMetadata.class),
            eq(aeadCryptoClient)))
        .thenReturn(fakeCsvDataReader);
    BlobStoreStreamDataSource blobStoreStreamDataSource =
        new BlobStoreStreamDataSource(
            mockBlobStorageClient,
            mockMetricClient,
            dataReaderFactory,
            FAKE_DATA_LOCATION,
            matchConfig,
            Optional.empty(),
            FeatureFlags.builder().build(),
            EncryptionMetadata.getDefaultInstance(),
            aeadCryptoClient);

    DataReader result = blobStoreStreamDataSource.next();

    assertEquals(fakeCsvDataReader, result);
    verify(mockBlobStorageClient, times(2)).getBlob(any(), any());
    verify(mockBlobStorageClient).listBlobs(any(), any());
    verify(dataReaderFactory)
        .createCsvDataReader(
            eq(mockInputStream),
            any(Schema.class),
            eq(INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE),
            eq(matchConfig.getEncryptionKeyColumns()),
            any(SuccessMode.class),
            eq(EncryptionMetadata.getDefaultInstance()),
            eq(aeadCryptoClient));
    verifyNoMoreInteractions(mockBlobStorageClient, dataReaderFactory, mockInputStream);
  }

  @Test
  public void getSchema_whenRuntimeExceptionThrowsNonRetryableError() throws Exception {
    setUp(DataFormat.CSV);
    when(mockBlobStorageClient.getBlob(SCHEMA_DATA_LOCATION, Optional.empty()))
        .thenThrow(NullPointerException.class);

    JobProcessorException e =
        assertThrows(
            JobProcessorException.class,
            () ->
                new BlobStoreStreamDataSource(
                    mockBlobStorageClient,
                    mockMetricClient,
                    dataReaderFactory,
                    FAKE_DATA_LOCATION,
                    matchConfig,
                    Optional.empty(),
                    FeatureFlags.builder().build()));

    assertEquals(JobResultCode.MISSING_SCHEMA_ERROR, e.getErrorCode());
    assertFalse(e.isRetriable());
    assertEquals("Unable to open the schema file", e.getMessage());
    assertEquals(e.getCause().getClass(), NullPointerException.class);
    verify(mockBlobStorageClient, times(2)).getBlob(dataLocationArgumentCaptor.capture(), any());
    assertThat(dataLocationArgumentCaptor.getAllValues())
        .containsExactly(SCHEMA_DATA_LOCATION, SCHEMA_DATA_LOCATION);
    verifyNoInteractions(dataReaderFactory);
  }

  @Test
  public void getSchema_whenStoragePermissionExThrowsNonRetryableError() throws Exception {
    setUp(DataFormat.CSV);
    var details = new GoogleJsonError();
    details.setCode(403);
    var exception =
        new StorageException(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(403, "error", new HttpHeaders()), details));
    when(mockBlobStorageClient.getBlob(SCHEMA_DATA_LOCATION, Optional.empty()))
        .thenThrow(exception);

    JobProcessorException e =
        assertThrows(
            JobProcessorException.class,
            () ->
                new BlobStoreStreamDataSource(
                    mockBlobStorageClient,
                    mockMetricClient,
                    dataReaderFactory,
                    FAKE_DATA_LOCATION,
                    matchConfig,
                    Optional.empty(),
                    FeatureFlags.builder().build()));

    assertEquals(JobResultCode.SCHEMA_PERMISSIONS_ERROR, e.getErrorCode());
    assertFalse(e.isRetriable());
    assertEquals("Missing permission to open schema file.", e.getMessage());
    assertEquals(e.getCause().getClass(), StorageException.class);
    verify(mockBlobStorageClient, times(2)).getBlob(dataLocationArgumentCaptor.capture(), any());
    assertThat(dataLocationArgumentCaptor.getAllValues())
        .containsExactly(SCHEMA_DATA_LOCATION, SCHEMA_DATA_LOCATION);
    verifyNoInteractions(dataReaderFactory);
  }

  @Test
  public void getSchema_whenOtherStorageExceptionThrowsRetryableError() throws Exception {
    setUp(DataFormat.CSV);
    var exception = new StorageException(new IOException(new SocketTimeoutException()));
    when(mockBlobStorageClient.getBlob(SCHEMA_DATA_LOCATION, Optional.empty()))
        .thenThrow(exception);

    JobProcessorException e =
        assertThrows(
            JobProcessorException.class,
            () ->
                new BlobStoreStreamDataSource(
                    mockBlobStorageClient,
                    mockMetricClient,
                    dataReaderFactory,
                    FAKE_DATA_LOCATION,
                    matchConfig,
                    Optional.empty(),
                    FeatureFlags.builder().build()));

    assertEquals(JobResultCode.SCHEMA_FILE_READ_ERROR, e.getErrorCode());
    assertTrue(e.isRetriable());
    assertEquals("Unable to read the schema file. Will retry.", e.getMessage());
    assertEquals(e.getCause().getClass(), StorageException.class);
    verify(mockBlobStorageClient, times(2)).getBlob(dataLocationArgumentCaptor.capture(), any());
    assertThat(dataLocationArgumentCaptor.getAllValues())
        .containsExactly(SCHEMA_DATA_LOCATION, SCHEMA_DATA_LOCATION);
    verifyNoInteractions(dataReaderFactory);
  }

  @Test
  public void getReader_whenExtraColumnInGroupThenThrowsException() throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_extra_column_in_group.json");
  }

  @Test
  public void getReader_whenIncompleteGroupInSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_incomplete_group.json");
  }

  @Test
  public void getReader_whenMissingColumnInSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_missing_column_group.json");
  }

  @Test
  public void getReader_whenNoMatchColumnsInSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_no_match_columns.json");
  }

  @Test
  public void getReader_whenNonUniqueColumnsInSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_non_unique.json");
  }

  @Test
  public void getReader_whenNonUniqueColumnsInNestedSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_nested_schema_non_unique.json");
  }

  @Test
  public void getReader_whenUnencryptedColumnHasColumnEncodingInSchemaThenThrowsException()
      throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_unencrypted_encoded.json");
  }

  @Test
  public void getReader_whenUnencryptedNestedColumnHasColumnEncodingInSchemaThenThrowsException()
      throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_unencrypted_encoded_nested_column.json");
  }

  @Test
  public void getReader_whenUnmatchColumnMarkedEncryptedInSchemaThenThrowsException()
      throws Exception {
    setUp(DataFormat.CSV);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_schema_unmatch_column_encrypted.json");
  }

  @Test
  public void getReader_whenNonUniqueColumnAliasInProtoSchemaThenThrowsException()
      throws Exception {
    setUp(DataFormat.SERIALIZED_PROTO);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_proto_schema_non_unique.json");
  }

  @Test
  public void getReader_whenNestedColumnInProtoSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.SERIALIZED_PROTO);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_proto_schema_with_nested_column.json");
  }

  @Test
  public void getReader_whenOutputColumnInProtoSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.SERIALIZED_PROTO);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_proto_schema_with_output_columns.json");
  }

  @Test
  public void getReader_whenRestrictedColumninProtoSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.SERIALIZED_PROTO);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_proto_schema_restricted_column.json");
  }

  @Test
  public void getReader_whenMissingColumnAliasinProtoSchemaThenThrowsException() throws Exception {
    setUp(DataFormat.SERIALIZED_PROTO);
    validateInvalidSchema(
        "/com/google/cm/mrp/dataprocessor/testdata/invalid_proto_schema_missing_column_alias.json");
  }

  private void validateInvalidSchema(String schemaPath) throws Exception {
    InputStream schemaInputStream =
        Objects.requireNonNull(getClass().getResourceAsStream(schemaPath));
    when(mockBlobStorageClient.getBlob(SCHEMA_DATA_LOCATION, Optional.empty()))
        .thenReturn(schemaInputStream);

    JobProcessorException e =
        assertThrows(
            JobProcessorException.class,
            () ->
                new BlobStoreStreamDataSource(
                    mockBlobStorageClient,
                    mockMetricClient,
                    dataReaderFactory,
                    FAKE_DATA_LOCATION,
                    matchConfig,
                    Optional.empty(),
                    FeatureFlags.builder().setEnableSerializedProto(true).build()));

    assertEquals(JobResultCode.INVALID_SCHEMA_FILE_ERROR, e.getErrorCode());
    assertFalse(e.isRetriable());
    assertEquals("Invalid schema file", e.getMessage());
    assertNull(e.getCause());
    verify(mockBlobStorageClient, times(2)).getBlob(dataLocationArgumentCaptor.capture(), any());
    assertThat(dataLocationArgumentCaptor.getAllValues())
        .containsExactly(SCHEMA_DATA_LOCATION, SCHEMA_DATA_LOCATION);
    verifyNoInteractions(dataReaderFactory);
  }

  private AeadCryptoClient makeAeadCryptoClient() {
    try {
      return new AeadCryptoClient(
          mockAeadProvider,
          EncryptionKeyInfo.newBuilder()
              .setWrappedKeyInfo(
                  WrappedKeyInfo.newBuilder()
                      .setKeyType(XCHACHA20_POLY1305)
                      .setGcpWrappedKeyInfo(
                          GcpWrappedKeyInfo.newBuilder().setWipProvider("defaultWip")))
              .build());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void setUp(DataFormat dataFormat) throws Exception {
    setUp(FAKE_DATA_LOCATION, dataFormat);
  }

  private void setUp(DataOwner.DataLocation dataLocation, DataFormat dataFormat) throws Exception {
    setUp(dataLocation, 1, dataFormat);
  }

  private void setUp(DataOwner.DataLocation dataLocation, int numFiles, DataFormat dataFormat)
      throws Exception {
    matchConfig = MatchConfigProvider.getMatchConfig("customer_match");
    String filePrefix = INPUT_PREFIX + FOLDER_DELIMITER + INPUT_FILE;
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.add(INPUT_PREFIX + FOLDER_DELIMITER + SCHEMA_DEFINITION_FILE);
    for (int i = 0; i < numFiles; ++i) {
      listBuilder.add(filePrefix + "-" + i + "-of-" + numFiles);
    }
    when(mockBlobStorageClient.listBlobs(BLOB_STORAGE_DATA_LOCATION, Optional.empty()))
        .thenReturn(listBuilder.build());
    when(mockBlobStorageClient.getBlob(
            argThat(
                locationArg ->
                    locationArg.blobStoreDataLocation().bucket().equals(INPUT_BUCKET)
                        && locationArg.blobStoreDataLocation().key().startsWith(filePrefix)),
            eq(Optional.empty())))
        .thenReturn(mockInputStream);

    InputStream schemaInputStream = getSchemaStream(dataFormat);

    when(mockBlobStorageClient.getBlob(SCHEMA_DATA_LOCATION, Optional.empty()))
        .thenReturn(schemaInputStream);
    blobStoreStreamDataSource =
        new BlobStoreStreamDataSource(
            mockBlobStorageClient,
            mockMetricClient,
            dataReaderFactory,
            dataLocation,
            matchConfig,
            Optional.empty(),
            FeatureFlags.builder().setEnableSerializedProto(true).build());
  }

  private InputStream getSchemaStream(DataFormat dataFormat) {
    switch (dataFormat) {
      case CSV:
        return Objects.requireNonNull(
            getClass()
                .getResourceAsStream("/com/google/cm/mrp/dataprocessor/testdata/schema.json"));
      case SERIALIZED_PROTO:
        return Objects.requireNonNull(
            getClass()
                .getResourceAsStream(
                    "/com/google/cm/mrp/dataprocessor/testdata/serialized_proto_schema.json"));
    }
    throw new RuntimeException("Invalid schema format: " + dataFormat);
  }

  private CsvDataReader getFakeCsvReader(String name) throws IOException {
    return new CsvDataReader(
        1000,
        mockInputStream,
        Schema.getDefaultInstance(),
        name,
        SuccessMode.ONLY_COMPLETE_SUCCESS);
  }

  private SerializedProtoDataReader getFakeSerializedProtoReader(String name) throws IOException {
    matchConfig = MatchConfigProvider.getMatchConfig("customer_match");
    when(mockCfmDataRecordParserFactory.create(any(), any(), any()))
        .thenReturn(
            new ConfidentialMatchDataRecordParserImpl(
                matchConfig, Schema.getDefaultInstance(), SuccessMode.ONLY_COMPLETE_SUCCESS));
    return new SerializedProtoDataReader(
        mockCfmDataRecordParserFactory,
        1000,
        mockInputStream,
        Schema.getDefaultInstance(),
        name,
        matchConfig,
        SuccessMode.ONLY_COMPLETE_SUCCESS);
  }
}
