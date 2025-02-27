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

import static com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo.KeyType.KEY_TYPE_XCHACHA20_POLY1305;
import static com.google.cm.lookupserver.api.LookupProto.LookupResult.Status.STATUS_FAILED;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_PROCESSING_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_FAILURE;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_INVALID_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.dataprocessor.LookupServerDataSource.PII_VALUE;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.decryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateEncryptedDek;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.getDefaultAeadSelector;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridDecrypt;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridEncrypt;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.lookupserver.api.LookupProto;
import com.google.cm.lookupserver.api.LookupProto.LookupKey;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.KeyFormat;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.cm.lookupserver.api.LookupProto.LookupResult.Status;
import com.google.cm.lookupserver.api.LookupProto.MatchedDataRecord;
import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.MatchConfigProvider;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.GcpWrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordProto;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.AwsWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClient;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientRequest;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientResponse;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.LookupDataSourceResult;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerFactory;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerImpl;
import com.google.cm.mrp.testutils.HybridKeyGenerator;
import com.google.cm.shared.api.errors.ErrorResponseProto.Details;
import com.google.cm.shared.api.errors.ErrorResponseProto.ErrorResponse;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
public final class LookupServerDataSourceTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String TEST_WIP = "testWip";
  private static final String TEST_IDENTITY = "testIdentity";
  private static final String TEST_ENDPOINT = "testEndpoint";
  private static final String TEST_WIP_B = "testWipB";
  private static final String TEST_IDENTITY_B = "testIdentityB";
  private static final String TEST_ENDPOINT_B = "testEndpointB";
  private static final String TEST_AUDIENCE = "testAudience";
  private static final String TEST_ROLE = "testRole";
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

  private static final EncryptionMetadata AWS_WRAPPED_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setWrappedKeyInfo(
                      WrappedKeyInfo.newBuilder()
                          .setKeyType(KeyType.XCHACHA20_POLY1305)
                          .setAwsWrappedKeyInfo(
                              AwsWrappedKeyInfo.newBuilder()
                                  .setAudience(TEST_AUDIENCE)
                                  .setRoleArn(TEST_ROLE))))
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
  private static final EncryptionMetadata COORDINATOR_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setCoordinatorKeyInfo(
                      CoordinatorKeyInfo.newBuilder()
                          .addCoordinatorInfo(
                              CoordinatorInfo.newBuilder()
                                  .setKeyServiceEndpoint(TEST_ENDPOINT)
                                  .setKmsIdentity(TEST_IDENTITY)
                                  .setKmsWipProvider(TEST_WIP))
                          .addCoordinatorInfo(
                              CoordinatorInfo.newBuilder()
                                  .setKeyServiceEndpoint(TEST_ENDPOINT_B)
                                  .setKmsIdentity(TEST_IDENTITY_B)
                                  .setKmsWipProvider(TEST_WIP_B))))
          .build();
  private static final Set<String> ENCRYPTED_COLUMNS =
      ImmutableSet.of("email", "phone", "first_name", "last_name");
  @Mock private AeadProvider mockAeadProvider;
  @Mock private CryptoClient mockCryptoClient;
  @Mock private LookupServiceClient mockLookupServiceClient;
  @Mock private DataRecordTransformerFactory dataRecordTransformerFactory;
  private LookupServerDataSource lookupServerDataSource;
  private LookupServerDataSource lookupServerDataSourceForEncryption;
  private LookupServerDataSource lookupServerDataSourceForV2;
  @Captor private ArgumentCaptor<LookupServiceClientRequest> lookupServiceClientRequestCaptor;

  @Before
  public void setUp() throws Exception {
    lookupServerDataSource =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            MatchConfigProvider.getMatchConfig("customer_match"),
            FeatureFlags.builder().build());
    var cryptoClient =
        new AeadCryptoClient(mockAeadProvider, WRAPPED_ENCRYPTION_METADATA.getEncryptionKeyInfo());
    lookupServerDataSourceForEncryption =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            MatchConfigProvider.getMatchConfig("customer_match"),
            cryptoClient,
            FeatureFlags.builder().build());
    lookupServerDataSourceForV2 =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            MatchConfigProvider.getMatchConfig("copla"),
            cryptoClient,
            FeatureFlags.builder().build());
    when(dataRecordTransformerFactory.create(any(), any()))
        .thenReturn(
            new DataRecordTransformerImpl(
                MatchConfig.getDefaultInstance(), Schema.getDefaultInstance()));
  }

  @Test
  public void lookup_whenAllColumnsMatchThenReturnsCompleteRecord() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(
                                            hashString("fake_first_namefake_last_nameUS99999")))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(3, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(2).getKeyValues(0).getKey());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        result.records().get(2).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(3, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenEncryptedMetadataSetsCorrectRequest() throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            MatchedDataRecord.newBuilder()
                                .setLookupKey(LookupKey.newBuilder().setKey(encryptedEmail))))
                .build());
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedFirstName = encryptString(dek, "fake_first_name");
    String encryptedLastName = encryptString(dek, "fake_last_name");
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    lookupServerDataSourceForEncryption.lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA));

    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify KeyMetadata
    assertThat(lookupRequest.encryptionKeyInfo()).isPresent();
    var lookupServiceWrappedKey =
        lookupRequest.encryptionKeyInfo().orElseThrow().getWrappedKeyInfo();
    assertEquals(dek, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_WIP, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertTrue(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(encryptedPhone, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertTrue(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals(
        "fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(encryptedEmail, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    // Check Address
    assertTrue(lookupRequest.records().get(2).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(2).getLookupKey().getDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenEncryptedMetadataDataChunkMissingDataEncryptionColumnsThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("DataChunk missing encryption columns");
    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_COLUMNS_PROCESSING_ERROR);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndDekColumnIndexSameAsEncryptedColumnsThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(2), Optional.empty()))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage())
        .isEqualTo("DEK or KEK indices cannot be part of columns-to-encrypt");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndKekColumnIndexSameAsEncryptedColumnsThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(2)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage())
        .isEqualTo("DEK or KEK indices cannot be part of columns-to-encrypt");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndWipColumnIndexSameAsEncryptedColumnsThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
      {"wip_provider", "wip_provider", "wip"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7), Optional.of(2)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(NO_WIP_WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("WIP index cannot be part of columns-to-encrypt");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndDekColumnIndexMissingThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.empty()))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("DEK or KEK indices out of bounds");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndKekColumnIndexMissingThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));
    assertThat(ex.getMessage()).isEqualTo("DEK or KEK indices out of bounds");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndWipColumnIndexMissingThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7), Optional.of(8)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(NO_WIP_WRAPPED_ENCRYPTION_METADATA)));
    assertThat(ex.getMessage()).isEqualTo("WIP index out of bounds");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndDekColumnBlankThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", ""},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("DEK or KEK key values missing in input data row");
    assertThat(ex.getErrorCode()).isEqualTo(MISSING_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndKekColumnBlankThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test_dek"},
      {"kek_uri", "kek_uri", ""},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("DEK or KEK key values missing in input data row");
    assertThat(ex.getErrorCode()).isEqualTo(MISSING_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndWipColumnBlankThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test_dek"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
      {"wip_provider", "wip_provider", ""},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7), Optional.of(8)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(NO_WIP_WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("WIP key value missing in input data row");
    assertThat(ex.getErrorCode()).isEqualTo(MISSING_ENCRYPTION_COLUMN);
  }

  @Test
  public void lookup_whenEncryptedMetadataNoWipAndWipIndicesMissingThrowsException() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test_dek"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
      {"wip_provider", "wip_provider", "testWip"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(NO_WIP_WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("No WIP in request and no WIP column index found.");
    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_COLUMNS_PROCESSING_ERROR);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndMissingCryptoClientThrowsException() {
    LookupDataSource lookupDataSource =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            MatchConfigProvider.getMatchConfig("customer_match"),
            FeatureFlags.builder().build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test_dek"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedTestData = {{"email", "email", "unneeded"}};
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedTestData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () -> lookupDataSource.lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));
    assertThat(ex.getMessage()).isEqualTo("CryptoClient missing in LookupServerDataSource");
    assertThat(ex.getErrorCode()).isEqualTo(CRYPTO_CLIENT_CONFIGURATION_ERROR);
  }

  @Test
  public void lookup_whenEncryptedMetadataAndMissingEncryptedKeyValuesThrowsException() {
    LookupDataSource lookupDataSource =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            MatchConfigProvider.getMatchConfig("customer_match"),
            FeatureFlags.builder().build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", "test_dek"},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () -> lookupDataSource.lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage())
        .isEqualTo(
            "Column index in set of columns-to-decrypt, but dataRecord missing encryptedKeyValue");
    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_COLUMNS_PROCESSING_ERROR);
  }

  @Test
  public void lookup_whenMultiColumnsMatchThenReturnsCompleteRecord() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email1@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email2@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("888-888-8888"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(
                                            hashString("fake_first_name1fake_last_name1US99999")))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(
                                            hashString("fake_first_name2fake_last_name2MX88888")))))
                .build());
    String[][] testData = {
      {"Email1", "email", "fake.email1@google.com", "1"},
      {"Phone1", "phone", "999-999-9999", "1"},
      {"FirstName1", "first_name", "fake_first_name1", "1"},
      {"LastName1", "last_name", "fake_last_name1", "1"},
      {"ZipCode1", "zip_code", "99999", "1"},
      {"CountryCode1", "country_code", "US", "1"},
      {"Email2", "email", "fake.email2@google.com", "2"},
      {"Phone2", "phone", "888-888-8888", "2"},
      {"FirstName2", "first_name", "fake_first_name2", "2"},
      {"LastName2", "last_name", "fake_last_name2", "2"},
      {"ZipCode2", "zip_code", "88888", "2"},
      {"CountryCode2", "country_code", "MX", "2"}
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(6, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals(
        "fake.email1@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals(
        "fake.email2@google.com", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(2).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(2).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(3).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(3).getKeyValues(0).getKey());
    assertEquals("888-888-8888", result.records().get(3).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(4).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(4).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        result.records().get(4).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(5).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(5).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_name2fake_last_name2MX88888"),
        result.records().get(5).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(6, lookupRequestRecords.size());
    assertEquals("fake.email1@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("fake.email2@google.com", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
    assertEquals("888-888-8888", lookupRequestRecords.get(3).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(3).getMetadataCount());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        lookupRequestRecords.get(4).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(4).getMetadataCount());
    assertEquals(
        hashString("fake_first_name2fake_last_name2MX88888"),
        lookupRequestRecords.get(5).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(5).getMetadataCount());
  }

  @Test
  public void lookup_whenPartialColumnsMatchThenReturnsPartialRecord() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(2, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(3, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenNoMatchThenReturnsEmptyResult() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(LookupResult.newBuilder().setStatus(Status.STATUS_SUCCESS))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(0, result.records().size());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(3, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenAssociatedDataThenReturnsMultipleRecords() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))
                                .addAssociatedData(
                                    LookupProto.KeyValue.newBuilder()
                                        .setKey("pii_type")
                                        .setStringValue("email"))
                                .addAssociatedData(
                                    LookupProto.KeyValue.newBuilder()
                                        .setKey("encrypted_gaia_id")
                                        .setBytesValue(ByteString.copyFromUtf8("abcd1234")))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))
                                .addAssociatedData(
                                    LookupProto.KeyValue.newBuilder()
                                        .setKey("pii_type")
                                        .setStringValue("phone"))
                                .addAssociatedData(
                                    LookupProto.KeyValue.newBuilder()
                                        .setKey("encrypted_gaia_id")
                                        .setBytesValue(ByteString.copyFromUtf8("abcd1234")))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(hashString("fake_first_namefake_last_nameUS99999")))
                                .addAssociatedData(
                                    LookupProto.KeyValue.newBuilder()
                                        .setKey("pii_type")
                                        .setStringValue("address"))
                                .addAssociatedData(
                                    LookupProto.KeyValue.newBuilder()
                                        .setKey("encrypted_gaia_id")
                                        .setBytesValue(ByteString.copyFromUtf8("abcd1234")))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(3, result.records().size());
    assertEquals(3, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("pii_type", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("email", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("encrypted_gaia_id", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("abcd1234", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals(3, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals("pii_type", result.records().get(1).getKeyValues(1).getKey());
    assertEquals("phone", result.records().get(1).getKeyValues(1).getStringValue());
    assertEquals("encrypted_gaia_id", result.records().get(1).getKeyValues(2).getKey());
    assertEquals("abcd1234", result.records().get(1).getKeyValues(2).getStringValue());
    assertEquals(3, result.records().get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(2).getKeyValues(0).getKey());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        result.records().get(2).getKeyValues(0).getStringValue());
    assertEquals("pii_type", result.records().get(2).getKeyValues(1).getKey());
    assertEquals("address", result.records().get(2).getKeyValues(1).getStringValue());
    assertEquals("encrypted_gaia_id", result.records().get(2).getKeyValues(2).getKey());
    assertEquals("abcd1234", result.records().get(2).getKeyValues(2).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(3, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenMatchTransformationsInConfigThenReturnsCompleteRecord() throws Exception {
    MatchConfig testConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass()
                        .getResource(
                            "/com/google/cm/mrp/dataprocessor/testdata/transformation_match_config.json")),
                UTF_8),
            MatchConfig.class);
    LookupServerDataSource testDataSource =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            testConfig,
            FeatureFlags.builder().build());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(
                                            hashString("fake_first_namefake_last_nameus09999")))))
                .build());
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com"}, // uppercase
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "9999"}, // not five digits
      {"country_code", "country_code", "us"},
    };
    Schema schema = getSchema(testData);
    DataChunk dataChunk =
        DataChunk.builder().setSchema(schema).addRecord(getDataRecord(testData)).build();
    when(dataRecordTransformerFactory.create(testConfig, schema))
        .thenReturn(new DataRecordTransformerImpl(testConfig, schema));

    DataChunk result = testDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(3, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(2).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameus09999"),
        result.records().get(2).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(3, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        hashString("fake_first_namefake_last_nameus09999"),
        lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenBlankEmailThenDoesNotLookupEmail() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(
                                            hashString("fake_first_namefake_last_nameUS99999")))))
                .build());
    String[][] testData = {
      {"email", "email", ""},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(2, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        result.records().get(1).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(2, lookupRequestRecords.size());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
  }

  @Test
  public void lookup_whenBlankPhoneThenDoesNotLookupPhone() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(
                                            hashString("fake_first_namefake_last_nameUS99999")))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", ""},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(2, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        result.records().get(1).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(2, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals(
        "IEUw/X1oeAwDwxvBs8+aS2bfk781XbfMOyEenBunDSU=",
        lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
  }

  @Test
  public void lookup_whenBlankAddressThenDoesNotLookupAddress() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", ""},
      {"last_name", "last_name", ""},
      {"zip_code", "zip_code", ""},
      {"country_code", "country_code", ""},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(2, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    assertEquals(2, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
  }

  @Test
  public void lookup_whenBlankColumnsThenDoesNotSendToLookup() throws Exception {
    String[][] testData = {
      {"email", "email", ""},
      {"phone", "phone", ""},
      {"first_name", "first_name", ""},
      {"last_name", "last_name", ""},
      {"zip_code", "zip_code", ""},
      {"country_code", "country_code", ""},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertTrue(result.records().isEmpty());
    verifyNoInteractions(mockLookupServiceClient);
  }

  @Test
  public void lookup_whenHashedDataRecordsWithErrorCodeSkipped() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey("fake.email@google.com"))))
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("999-999-9999"))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
    };
    String[][] testData2 = {
      {"email", "email", "fake.email2@google.com"},
      {"phone", "phone", "999-999-9990"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    // Just testing row level error even though decryption error won't occur for unencrypted data
    DataRecord errorRecord =
        getDataRecord(testData2).toBuilder().setErrorCode(DECRYPTION_ERROR).build();

    // Add normal DataRecord and one DataRecord with error code
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(dataRecord)
            .addRecord(errorRecord)
            .build();

    DataChunk result = lookupServerDataSource.lookup(dataChunk, Optional.empty()).lookupResults();

    assertEquals(2, result.records().size());
    assertEquals(1, result.records().get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals(1, result.records().get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, result.records().get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    List<LookupDataRecord> lookupRequestRecords =
        lookupServiceClientRequestCaptor.getValue().records();
    // Should only be two records since other DataRecord has error code
    assertEquals(2, lookupRequestRecords.size());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals("999-999-9999", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
  }

  @Test
  public void lookup_whenHashedRequestsHasUnrecognizedLookupResultThenThrowsException()
      throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(LookupProto.LookupResult.newBuilder())
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", ""},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () -> lookupServerDataSource.lookup(dataChunk, Optional.empty()));
    assertThat(ex.getMessage()).isEqualTo("Lookup server returned STATUS_UNSPECIFIED.");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_INVALID_ERROR);
  }

  @Test
  public void lookup_whenHashedRequestsHasFailedLookupResultThenThrowsException() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(LookupProto.LookupResult.newBuilder().setStatus(Status.STATUS_FAILED))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", ""},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () -> lookupServerDataSource.lookup(dataChunk, Optional.empty()));
    assertThat(ex.getMessage())
        .isEqualTo(
            "Lookup server results contain a failed result but partial success not allowed for"
                + " job.");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_FAILURE);
  }

  @Test
  public void lookup_singleWrappedKeyEncryptionRowAllMatchReturnsCompleteRecord() throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedFirstName = encryptString(dek, "fake_first_name");
    String encryptedLastName = encryptString(dek, "fake_last_name");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedEmail))))
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedPhone))));
              // Address response must be based on request input since encryption is
              // non-deterministic
              LookupServiceClientRequest request = invocation.getArgument(0);
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .build();
            });
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    DataChunk result =
        lookupServerDataSourceForEncryption
            .lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA))
            .lookupResults();

    assertEquals(3, result.records().size());
    var dataRecords = result.records();
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", dataRecords.get(0).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", dataRecords.get(1).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(2).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(2).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertEquals(dek, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_WIP, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords =
        sortLookupDataRecordsByKeyLength(lookupRequest.records());
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertTrue(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(encryptedPhone, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertTrue(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals(
        "fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(encryptedEmail, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    // Check Address
    assertTrue(lookupRequest.records().get(2).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(2).getLookupKey().getDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_singleAwsWrappedKeyEncryptionRowAllMatchReturnsCompleteRecord()
      throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedFirstName = encryptString(dek, "fake_first_name");
    String encryptedLastName = encryptString(dek, "fake_last_name");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedEmail))))
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedPhone))));
              // Address response must be based on request input since encryption is
              // non-deterministic
              LookupServiceClientRequest request = invocation.getArgument(0);
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .build();
            });
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "arn:test-region:123"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    DataChunk result =
        lookupServerDataSourceForEncryption
            .lookup(dataChunk, Optional.of(AWS_WRAPPED_ENCRYPTION_METADATA))
            .lookupResults();

    assertEquals(3, result.records().size());
    var dataRecords = result.records();
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", dataRecords.get(0).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", dataRecords.get(1).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(2).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(2).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertEquals(dek, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("arn:test-region:123", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_AUDIENCE, lookupServiceWrappedKey.getAwsWrappedKeyInfo().getAudience());
    assertEquals(TEST_ROLE, lookupServiceWrappedKey.getAwsWrappedKeyInfo().getRoleArn());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords =
        sortLookupDataRecordsByKeyLength(lookupRequest.records());
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertTrue(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(encryptedPhone, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertTrue(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals(
        "fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(encryptedEmail, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    // Check Address
    assertTrue(lookupRequest.records().get(2).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(2).getLookupKey().getDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_whenEncryptedRequestHasUnrecognizedLookupResultThenThrowsException()
      throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedFirstName = encryptString(dek, "fake_first_name");
    String encryptedLastName = encryptString(dek, "fake_last_name");
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(LookupProto.LookupResult.newBuilder())
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));
    assertThat(ex.getMessage()).isEqualTo("Lookup server returned STATUS_UNSPECIFIED.");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_INVALID_ERROR);
  }

  @Test
  public void
      lookup_whenEncryptedRequestsHasFailedLookupResultButNoPartialSuccessThenThrowsException()
          throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedFirstName = encryptString(dek, "fake_first_name");
    String encryptedLastName = encryptString(dek, "fake_last_name");
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(LookupProto.LookupResult.newBuilder().setStatus(Status.STATUS_FAILED))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () -> lookupServerDataSource.lookup(dataChunk, Optional.empty()));
    assertThat(ex.getMessage())
        .isEqualTo(
            "Lookup server results contain a failed result but partial success not allowed for"
                + " job.");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_FAILURE);
  }

  @Test
  public void lookup_multiEncryptionRowSameDekKekHasSameLookupRequest() throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedFirstName = encryptString(dek, "fake_first_name");
    String encryptedLastName = encryptString(dek, "fake_last_name");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedEmail))))
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedPhone))));

              // Address response must be based on request input since encryption is
              // non-deterministic
              LookupServiceClientRequest request = invocation.getArgument(0);
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .build();
            });
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    // Add two of the same row
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    DataChunk result =
        lookupServerDataSourceForEncryption
            .lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA))
            .lookupResults();

    assertEquals(4, result.records().size());
    var dataRecords = result.records();
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", dataRecords.get(0).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", dataRecords.get(1).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(2).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(2).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(3).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(3).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(3).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertEquals(dek, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_WIP, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords = lookupRequest.records();
    assertEquals(4, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertTrue(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(encryptedPhone, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertTrue(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals(
        "fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(encryptedEmail, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    // Check Addresses
    assertTrue(lookupRequest.records().get(2).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(2).getLookupKey().getDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
    assertTrue(lookupRequest.records().get(3).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(3).getLookupKey().getDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek, lookupRequestRecords.get(3).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(3).getMetadataCount());
  }

  @Test
  public void lookup_multiEncryptionRowDifferentDekKekHasDifferentLookupRequest() throws Exception {
    var dek0 = generateEncryptedDek();
    String encryptedEmail0 = encryptString(dek0, "Fake.email0@google.com");
    String encryptedPhone0 = encryptString(dek0, "099-999-9999");
    String encryptedFirstName0 = encryptString(dek0, "fake_first_name");
    String encryptedLastName0 = encryptString(dek0, "fake_last_name");
    var dek1 = generateEncryptedDek();
    String encryptedEmail1 = encryptString(dek1, "Fake.email1@google.com");
    String encryptedPhone1 = encryptString(dek1, "199-999-9999");
    String encryptedFirstName1 = encryptString(dek1, "fake_first_name1");
    String encryptedLastName1 = encryptString(dek1, "fake_last_name1");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              LookupServiceClientRequest request = invocation.getArgument(0);
              if (request
                  .encryptionKeyInfo()
                  .orElseThrow()
                  .getWrappedKeyInfo()
                  .getEncryptedDek()
                  .equals(dek0)) {
                responseBuilder
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedEmail0))))
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedPhone0))));
              } else {
                responseBuilder
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedEmail1))))
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedPhone1))));
              }
              // Address response must be based on request input since encryption is
              // non-deterministic
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .build();
            });
    String[][] testData0 = {
      {"email", "email", "Fake.email0@google.com"},
      {"phone", "phone", "099-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek0},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData0 = {
      {"email", encryptedEmail0},
      {"phone", encryptedPhone0},
      {"first_name", encryptedFirstName0},
      {"last_name", encryptedLastName0},
    };
    String[][] testData1 = {
      {"email", "email", "Fake.email1@google.com"},
      {"phone", "phone", "199-999-9999"},
      {"first_name", "first_name", "fake_first_name1"},
      {"last_name", "last_name", "fake_last_name1"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek1},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek1"},
    };
    String[][] encryptedData1 = {
      {"email", encryptedEmail1},
      {"phone", encryptedPhone1},
      {"first_name", encryptedFirstName1},
      {"last_name", encryptedLastName1},
    };
    // Two different rows
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData0))
            .addRecord(getDataRecord(testData0, encryptedData0))
            .addRecord(getDataRecord(testData1, encryptedData1))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    DataChunk result =
        lookupServerDataSourceForEncryption
            .lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA))
            .lookupResults();

    assertEquals(6, result.records().size());
    var dataRecords = sortDataRecordsByKeyValue(result.records());
    // Check phones
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("099-999-9999", dataRecords.get(0).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("199-999-9999", dataRecords.get(1).getKeyValues(0).getStringValue());
    // Check emails
    assertEquals(1, dataRecords.get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(2).getKeyValues(0).getKey());
    assertEquals("Fake.email0@google.com", dataRecords.get(2).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(3).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(3).getKeyValues(0).getKey());
    assertEquals("Fake.email1@google.com", dataRecords.get(3).getKeyValues(0).getStringValue());
    // Check addresses
    assertEquals(1, dataRecords.get(4).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(4).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(4).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(5).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(5).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        dataRecords.get(5).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient, times(2))
        .lookupRecords(lookupServiceClientRequestCaptor.capture());
    // Multiple requests means multiple values captured
    // Order of requests is nondeterministic, so sort by email value (so dek0 is first)
    var lookupRequestList = sortLookupRequests(lookupServiceClientRequestCaptor.getAllValues());
    var lookupRequest = lookupRequestList.get(0);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertThat(lookupServiceWrappedKey.getEncryptedDek()).isEqualTo(dek0);
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_WIP, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify first request records
    List<LookupDataRecord> lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    assertEquals(encryptedPhone0, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals("099-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals(encryptedEmail0, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(
        "Fake.email0@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek0, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
    // Verify second request
    lookupRequest = lookupRequestList.get(1);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertEquals(dek1, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("locations/testRegion/testKek1", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_WIP, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify individual records in this request
    lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    assertEquals(encryptedPhone1, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals("199-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals(encryptedEmail1, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(
        "Fake.email1@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        decryptString(dek1, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_multiEncryptionRowSameDekKekDifferentWipHasDifferentLookupRequest()
      throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail0 = encryptString(dek, "Fake.email0@google.com");
    String encryptedPhone0 = encryptString(dek, "099-999-9999");
    String encryptedFirstName0 = encryptString(dek, "fake_first_name");
    String encryptedLastName0 = encryptString(dek, "fake_last_name");
    String encryptedEmail1 = encryptString(dek, "Fake.email1@google.com");
    String encryptedPhone1 = encryptString(dek, "199-999-9999");
    String encryptedFirstName1 = encryptString(dek, "fake_first_name1");
    String encryptedLastName1 = encryptString(dek, "fake_last_name1");
    String wip0 = "testWip0";
    String wip1 = "testWip1";

    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              LookupServiceClientRequest request = invocation.getArgument(0);
              if (request
                  .encryptionKeyInfo()
                  .orElseThrow()
                  .getWrappedKeyInfo()
                  .getKmsWipProvider()
                  .equals(wip0)) {
                responseBuilder
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedEmail0))))
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedPhone0))));
              } else {
                responseBuilder
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedEmail1))))
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedPhone1))));
              }
              // Address response must be based on request input since encryption is
              // non-deterministic
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .build();
            });
    String[][] testData0 = {
      {"email", "email", "Fake.email0@google.com"},
      {"phone", "phone", "099-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
      {"wip_provider", "wip_provider", wip0},
    };
    String[][] encryptedData0 = {
      {"email", encryptedEmail0},
      {"phone", encryptedPhone0},
      {"first_name", encryptedFirstName0},
      {"last_name", encryptedLastName0},
    };
    String[][] testData1 = {
      {"email", "email", "Fake.email1@google.com"},
      {"phone", "phone", "199-999-9999"},
      {"first_name", "first_name", "fake_first_name1"},
      {"last_name", "last_name", "fake_last_name1"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
      {"wip_provider", "wip_provider", wip1},
    };
    String[][] encryptedData1 = {
      {"email", encryptedEmail1},
      {"phone", encryptedPhone1},
      {"first_name", encryptedFirstName1},
      {"last_name", encryptedLastName1},
    };
    // Two different rows
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData0))
            .addRecord(getDataRecord(testData0, encryptedData0))
            .addRecord(getDataRecord(testData1, encryptedData1))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7), Optional.of(8)))
                    .build())
            .build();

    DataChunk result =
        lookupServerDataSourceForEncryption
            .lookup(dataChunk, Optional.of(NO_WIP_WRAPPED_ENCRYPTION_METADATA))
            .lookupResults();

    assertEquals(6, result.records().size());
    var dataRecords = sortDataRecordsByKeyValue(result.records());
    // Check phones
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("099-999-9999", dataRecords.get(0).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("199-999-9999", dataRecords.get(1).getKeyValues(0).getStringValue());
    // Check emails
    assertEquals(1, dataRecords.get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(2).getKeyValues(0).getKey());
    assertEquals("Fake.email0@google.com", dataRecords.get(2).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(3).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(3).getKeyValues(0).getKey());
    assertEquals("Fake.email1@google.com", dataRecords.get(3).getKeyValues(0).getStringValue());
    // Check addresses
    assertEquals(1, dataRecords.get(4).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(4).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(4).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(5).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(5).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        dataRecords.get(5).getKeyValues(0).getStringValue());
    verify(mockLookupServiceClient, times(2))
        .lookupRecords(lookupServiceClientRequestCaptor.capture());
    // Multiple requests means multiple values captured
    // Order of requests is nondeterministic, so sort by email value (so wip0 is first)
    var lookupRequestList = sortLookupRequests(lookupServiceClientRequestCaptor.getAllValues());
    var lookupRequest = lookupRequestList.get(0);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertThat(lookupServiceWrappedKey.getEncryptedDek()).isEqualTo(dek);
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(wip0, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify first request records
    List<LookupDataRecord> lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    assertEquals(encryptedPhone0, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals("099-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals(encryptedEmail0, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(
        "Fake.email0@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        decryptString(dek, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
    // Verify second request
    lookupRequest = lookupRequestList.get(1);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertEquals(dek, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(wip1, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify individual records in this request
    lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    assertEquals(encryptedPhone1, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals("199-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    assertEquals(encryptedEmail1, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(
        "Fake.email1@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        decryptString(dek, lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_lookupServiceReturnsUnknownRecordThrowsException() throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder().setKey("invalid"))))
                .build());
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(1), Optional.of(2)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForEncryption.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("Lookup result key not found in original data record");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_FAILURE);
  }

  @Test
  public void lookup_encryptionMetadataWithCoordinatorKey() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              // Address response must be based on request input since encryption is
              // non-deterministic
              LookupServiceClientRequest request = invocation.getArgument(0);
              for (int i = 0; i < request.records().size(); i++) {
                var encryptedAddressKey = request.records().get(i).getLookupKey().getKey();
                responseBuilder.addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(encryptedAddressKey))));
              }
              return responseBuilder.build();
            });
    String dek = "123";
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"coordinator_key_id", "coordinator_key_id", dek},
    };
    String[][] encryptedData = {
      {
        "email",
        HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "fake.email@google.com")
      },
      {"phone", HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "999-999-9999")},
      {
        "first_name", HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "fake_first_name")
      },
      {"last_name", HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "fake_last_name")},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        DataRecordEncryptionColumns.EncryptionKeyColumnIndices.newBuilder()
                            .setCoordinatorKeyColumnIndices(
                                DataRecordEncryptionColumns.CoordinatorKeyColumnIndices.newBuilder()
                                    .setCoordinatorKeyColumnIndex(6))
                            .build())
                    .build())
            .build();
    when(mockCryptoClient.encrypt(any(DataRecordEncryptionKeys.class), anyString()))
        .thenAnswer(
            invocation -> {
              String plaintext = (String) invocation.getArguments()[1];
              return HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), plaintext);
            });
    MatchConfig testConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass()
                        .getResource(
                            "/com/google/cm/mrp/dataprocessor/testdata/coordinator_encryption_match_config.json")),
                UTF_8),
            MatchConfig.class);
    lookupServerDataSource =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            testConfig,
            mockCryptoClient,
            FeatureFlags.builder().setEnableMIC(true).build());

    DataChunk result =
        lookupServerDataSource
            .lookup(dataChunk, Optional.of(COORDINATOR_ENCRYPTION_METADATA))
            .lookupResults();

    // Verify Response
    assertEquals(3, result.records().size());
    var dataRecords = result.records();
    List<String> expected = new ArrayList<>();
    expected.add("fake.email@google.com");
    expected.add("999-999-9999");
    expected.add(hashString("fake_first_namefake_last_nameUS99999"));
    Collections.sort(expected);
    List<String> actual = new ArrayList<>();
    actual.add(dataRecords.get(0).getKeyValues(0).getStringValue());
    actual.add(dataRecords.get(1).getKeyValues(0).getStringValue());
    actual.add(dataRecords.get(2).getKeyValues(0).getStringValue());
    Collections.sort(actual);
    assertEquals(expected, actual);
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceCoordKey = lookupRequest.encryptionKeyInfo().get().getCoordinatorKeyInfo();
    assertEquals(dek, lookupServiceCoordKey.getKeyId());
    assertEquals(2, lookupServiceCoordKey.getCoordinatorInfoCount());
    assertEquals(TEST_WIP, lookupServiceCoordKey.getCoordinatorInfo(0).getKmsWipProvider());
    assertEquals(TEST_IDENTITY, lookupServiceCoordKey.getCoordinatorInfo(0).getKmsIdentity());
    assertEquals(
        TEST_ENDPOINT, lookupServiceCoordKey.getCoordinatorInfo(0).getKeyServiceEndpoint());
    assertEquals(TEST_WIP_B, lookupServiceCoordKey.getCoordinatorInfo(1).getKmsWipProvider());
    assertEquals(TEST_IDENTITY_B, lookupServiceCoordKey.getCoordinatorInfo(1).getKmsIdentity());
    assertEquals(
        TEST_ENDPOINT_B, lookupServiceCoordKey.getCoordinatorInfo(1).getKeyServiceEndpoint());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertTrue(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(
        "999-999-9999",
        HybridKeyGenerator.decryptString(
            getDefaultHybridDecrypt(), lookupRequestRecords.get(0).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertTrue(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals(
        "fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(
        "fake.email@google.com",
        HybridKeyGenerator.decryptString(
            getDefaultHybridDecrypt(), lookupRequestRecords.get(1).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    // Check Addresses
    assertTrue(lookupRequest.records().get(2).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(2).getLookupKey().getDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        HybridKeyGenerator.decryptString(
            getDefaultHybridDecrypt(), lookupRequestRecords.get(2).getLookupKey().getKey()));
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_encryptionMetadataWithCoordinatorKeyBatchEncryption() throws Exception {
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              // Address response must be based on request input since encryption is
              // non-deterministic
              LookupServiceClientRequest request = invocation.getArgument(0);
              for (int i = 0; i < request.records().size(); i++) {
                var encryptedAddressKey = request.records().get(i).getLookupKey().getKey();
                responseBuilder.addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setStatus(Status.STATUS_SUCCESS)
                        .addMatchedDataRecords(
                            LookupProto.MatchedDataRecord.newBuilder()
                                .setLookupKey(
                                    LookupProto.LookupKey.newBuilder()
                                        .setKey(encryptedAddressKey))));
              }
              return responseBuilder.build();
            });
    String dek = "123";
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"coordinator_key_id", "coordinator_key_id", dek},
    };
    String[][] encryptedData = {
      {
        "email",
        HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "fake.email@google.com")
      },
      {"phone", HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "999-999-9999")},
      {
        "first_name", HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "fake_first_name")
      },
      {"last_name", HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), "fake_last_name")},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        DataRecordEncryptionColumns.EncryptionKeyColumnIndices.newBuilder()
                            .setCoordinatorKeyColumnIndices(
                                DataRecordEncryptionColumns.CoordinatorKeyColumnIndices.newBuilder()
                                    .setCoordinatorKeyColumnIndex(6))
                            .build())
                    .build())
            .build();
    when(mockCryptoClient.encrypt(any(DataRecordEncryptionKeys.class), anyString()))
        .thenAnswer(
            invocation -> {
              String plaintext = (String) invocation.getArguments()[1];
              return HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), plaintext);
            });
    MatchConfig testConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass()
                        .getResource(
                            "/com/google/cm/mrp/dataprocessor/testdata/coordinator_encryption_match_config.json")),
                UTF_8),
            MatchConfig.class);
    lookupServerDataSource =
        new LookupServerDataSource(
            mockLookupServiceClient,
            dataRecordTransformerFactory,
            "",
            testConfig,
            mockCryptoClient,
            FeatureFlags.builder()
                .setEnableMIC(true)
                .setCoordinatorBatchEncryptionEnabled(true)
                .build());

    DataChunk result =
        lookupServerDataSource
            .lookup(dataChunk, Optional.of(COORDINATOR_ENCRYPTION_METADATA))
            .lookupResults();

    // Verify Response
    assertEquals(3, result.records().size());
    var dataRecords = result.records();
    List<String> expected = new ArrayList<>();
    expected.add("fake.email@google.com");
    expected.add("999-999-9999");
    expected.add(hashString("fake_first_namefake_last_nameUS99999"));
    Collections.sort(expected);
    List<String> actual = new ArrayList<>();
    actual.add(dataRecords.get(0).getKeyValues(0).getStringValue());
    actual.add(dataRecords.get(1).getKeyValues(0).getStringValue());
    actual.add(dataRecords.get(2).getKeyValues(0).getStringValue());
    Collections.sort(actual);
    assertEquals(expected, actual);
    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceCoordKey = lookupRequest.encryptionKeyInfo().get().getCoordinatorKeyInfo();
    assertEquals(dek, lookupServiceCoordKey.getKeyId());
    assertEquals(2, lookupServiceCoordKey.getCoordinatorInfoCount());
    assertEquals(TEST_WIP, lookupServiceCoordKey.getCoordinatorInfo(0).getKmsWipProvider());
    assertEquals(TEST_IDENTITY, lookupServiceCoordKey.getCoordinatorInfo(0).getKmsIdentity());
    assertEquals(
        TEST_ENDPOINT, lookupServiceCoordKey.getCoordinatorInfo(0).getKeyServiceEndpoint());
    assertEquals(TEST_WIP_B, lookupServiceCoordKey.getCoordinatorInfo(1).getKmsWipProvider());
    assertEquals(TEST_IDENTITY_B, lookupServiceCoordKey.getCoordinatorInfo(1).getKmsIdentity());
    assertEquals(
        TEST_ENDPOINT_B, lookupServiceCoordKey.getCoordinatorInfo(1).getKeyServiceEndpoint());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords = lookupRequest.records();
    assertEquals(3, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertFalse(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertFalse(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals("fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
    // Check Addresses
    assertFalse(lookupRequest.records().get(2).getLookupKey().hasDecryptedKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        lookupRequestRecords.get(2).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(2).getMetadataCount());
  }

  @Test
  public void lookup_encryptedDataRecordsWithErrorCodeSkipped() throws Exception {
    var dek = generateEncryptedDek();
    String encryptedEmail = encryptString(dek, "fake.email@google.com");
    String encryptedPhone = encryptString(dek, "999-999-9999");
    String encryptedEmail2 = encryptString(dek, "fake.email2@google.com");
    String encryptedPhone2 = encryptString(dek, "999-999-9990");
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedEmail))))
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder().setKey(encryptedPhone))));
              return responseBuilder.build();
            });
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
    };
    String[][] testData2 = {
      {"email", "email", "fake.email2@google.com"},
      {"phone", "phone", "999-999-9990"},
      {"encrypted_dek", "encrypted_dek", dek},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData2 = {
      {"email", encryptedEmail2},
      {"phone", encryptedPhone2},
    };
    DataRecord encryptedDataRecord = getDataRecord(testData, encryptedData);
    DataRecord encryptedDataRecordWithError =
        getDataRecord(testData2, encryptedData2).toBuilder().setErrorCode(DECRYPTION_ERROR).build();
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(encryptedDataRecord)
            .addRecord(encryptedDataRecordWithError)
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(2), Optional.of(3)))
                    .build())
            .build();

    DataChunk result =
        lookupServerDataSourceForEncryption
            .lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA))
            .lookupResults();

    assertEquals(2, result.records().size());
    var dataRecords = result.records();
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", dataRecords.get(0).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("999-999-9999", dataRecords.get(1).getKeyValues(0).getStringValue());

    verify(mockLookupServiceClient).lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequest = lookupServiceClientRequestCaptor.getValue();
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    // Verify EncryptionKeyInfo
    assertTrue(lookupRequest.encryptionKeyInfo().isPresent());
    var lookupServiceWrappedKey = lookupRequest.encryptionKeyInfo().get().getWrappedKeyInfo();
    assertEquals(dek, lookupServiceWrappedKey.getEncryptedDek());
    assertEquals("locations/testRegion/testKek", lookupServiceWrappedKey.getKekKmsResourceId());
    assertEquals(KEY_TYPE_XCHACHA20_POLY1305, lookupServiceWrappedKey.getKeyType());
    assertEquals(TEST_WIP, lookupServiceWrappedKey.getKmsWipProvider());
    // Verify individual records
    List<LookupDataRecord> lookupRequestRecords =
        sortLookupDataRecordsByKeyLength(lookupRequest.records());
    assertEquals(2, lookupRequestRecords.size());
    // results are nondeterministic. Only reproducible sort is phone,email,address
    lookupRequestRecords = sortLookupDataRecordsByKeyLength(lookupRequestRecords);
    // Check Phone
    assertTrue(lookupRequest.records().get(0).getLookupKey().hasDecryptedKey());
    assertEquals("999-999-9999", lookupRequestRecords.get(0).getLookupKey().getDecryptedKey());
    assertEquals(encryptedPhone, lookupRequestRecords.get(0).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(0).getMetadataCount());
    // Check Email
    assertTrue(lookupRequest.records().get(1).getLookupKey().hasDecryptedKey());
    assertEquals(
        "fake.email@google.com", lookupRequestRecords.get(1).getLookupKey().getDecryptedKey());
    assertEquals(encryptedEmail, lookupRequestRecords.get(1).getLookupKey().getKey());
    assertEquals(0, lookupRequestRecords.get(1).getMetadataCount());
  }

  @Test
  public void lookup_multiEncryptionRowPartialSuccessButNoDetails_ThrowsException()
      throws Exception {
    var dek0 = generateEncryptedDek();
    String encryptedEmail0 = encryptString(dek0, "Fake.email0@google.com");
    String encryptedPhone0 = encryptString(dek0, "099-999-9999");
    String encryptedFirstName0 = encryptString(dek0, "fake_first_name");
    String encryptedLastName0 = encryptString(dek0, "fake_last_name");
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenReturn(
            LookupServiceClientResponse.builder()
                .addResult(
                    LookupProto.LookupResult.newBuilder()
                        .setClientDataRecord(
                            LookupProto.DataRecord.newBuilder()
                                .setLookupKey(LookupKey.newBuilder().setKey(encryptedEmail0)))
                        .setStatus(STATUS_FAILED))
                .build());
    String[][] testData = {
      {"email", "email", "Fake.email0@google.com"},
      {"phone", "phone", "099-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek0},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail0},
      {"phone", encryptedPhone0},
      {"first_name", encryptedFirstName0},
      {"last_name", encryptedLastName0},
    };
    // Two different rows
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                lookupServerDataSourceForV2.lookup(
                    dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA)));

    assertThat(ex.getMessage()).isEqualTo("Lookup Service returned error result without details.");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_INVALID_ERROR);
  }

  @Test
  public void lookup_multiEncryptionRowPartialSuccessReturnsAndAddsErrorsToRecord()
      throws Exception {
    var dek0 = generateEncryptedDek();
    String encryptedEmail0 = encryptString(dek0, "Fake.email0@google.com");
    String encryptedPhone0 = encryptString(dek0, "099-999-9999");
    String encryptedFirstName0 = encryptString(dek0, "fake_first_name");
    String encryptedLastName0 = encryptString(dek0, "fake_last_name");
    var dek1 = generateEncryptedDek();
    String encryptedEmail1 = encryptString(dek1, "Fake.email1@google.com");
    String encryptedPhone1 = encryptString(dek1, "199-999-9999");
    String encryptedFirstName1 = encryptString(dek1, "fake_first_name1");
    String encryptedLastName1 = encryptString(dek1, "fake_last_name1");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              LookupServiceClientRequest request = invocation.getArgument(0);
              if (request
                  .encryptionKeyInfo()
                  .orElseThrow()
                  .getWrappedKeyInfo()
                  .getEncryptedDek()
                  .equals(dek0)) {
                responseBuilder
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setClientDataRecord(
                                LookupProto.DataRecord.newBuilder()
                                    .setLookupKey(LookupKey.newBuilder().setKey(encryptedEmail0)))
                            .setStatus(STATUS_FAILED)
                            .setErrorResponse(
                                ErrorResponse.newBuilder()
                                    .setCode(3)
                                    .setMessage(
                                        "An encryption/decryption error occurred while processing"
                                            + " the request.")
                                    .addDetails(
                                        Details.newBuilder()
                                            .setReason("2415853572")
                                            .setDomain("LookupService"))))
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedPhone0))));
              } else {
                responseBuilder
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setStatus(Status.STATUS_SUCCESS)
                            .addMatchedDataRecords(
                                LookupProto.MatchedDataRecord.newBuilder()
                                    .setLookupKey(
                                        LookupProto.LookupKey.newBuilder()
                                            .setKey(encryptedEmail1))))
                    .addResult(
                        LookupProto.LookupResult.newBuilder()
                            .setClientDataRecord(
                                LookupProto.DataRecord.newBuilder()
                                    .setLookupKey(LookupKey.newBuilder().setKey(encryptedPhone1)))
                            .setStatus(STATUS_FAILED)
                            .setErrorResponse(
                                ErrorResponse.newBuilder()
                                    .setCode(3)
                                    .setMessage(
                                        "An encryption/decryption error occurred while processing"
                                            + " the request.")
                                    .addDetails(
                                        Details.newBuilder()
                                            .setReason("2415853572")
                                            .setDomain("LookupService"))));
              }
              // Address response must be based on request input since encryption is
              // non-deterministic
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setStatus(Status.STATUS_SUCCESS)
                          .addMatchedDataRecords(
                              LookupProto.MatchedDataRecord.newBuilder()
                                  .setLookupKey(
                                      LookupProto.LookupKey.newBuilder()
                                          .setKey(encryptedAddressKey))))
                  .build();
            });
    String[][] testData0 = {
      {"email", "email", "Fake.email0@google.com"},
      {"phone", "phone", "099-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek0},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData0 = {
      {"email", encryptedEmail0},
      {"phone", encryptedPhone0},
      {"first_name", encryptedFirstName0},
      {"last_name", encryptedLastName0},
    };
    String[][] testData1 = {
      {"email", "email", "Fake.email1@google.com"},
      {"phone", "phone", "199-999-9999"},
      {"first_name", "first_name", "fake_first_name1"},
      {"last_name", "last_name", "fake_last_name1"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek1},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek1"},
    };
    String[][] encryptedData1 = {
      {"email", encryptedEmail1},
      {"phone", encryptedPhone1},
      {"first_name", encryptedFirstName1},
      {"last_name", encryptedLastName1},
    };
    // Two different rows
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData0))
            .addRecord(getDataRecord(testData0, encryptedData0))
            .addRecord(getDataRecord(testData1, encryptedData1))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    LookupDataSourceResult result =
        lookupServerDataSourceForV2.lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA));

    // Four matches
    assertEquals(4, result.lookupResults().records().size());
    var dataRecords = sortDataRecordsByKeyValue(result.lookupResults().records());
    // Check phone for dataRecord0
    assertEquals(1, dataRecords.get(0).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(0).getKeyValues(0).getKey());
    assertEquals("099-999-9999", dataRecords.get(0).getKeyValues(0).getStringValue());
    // Check email for dataRecord1
    assertEquals(1, dataRecords.get(1).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(1).getKeyValues(0).getKey());
    assertEquals("Fake.email1@google.com", dataRecords.get(1).getKeyValues(0).getStringValue());
    // Check addresses
    assertEquals(1, dataRecords.get(2).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(2).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecords.get(2).getKeyValues(0).getStringValue());
    assertEquals(1, dataRecords.get(3).getKeyValuesCount());
    assertEquals(PII_VALUE, dataRecords.get(3).getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_name1fake_last_name1US99999"),
        dataRecords.get(3).getKeyValues(0).getStringValue());
    // Multiple requests
    verify(mockLookupServiceClient, times(2))
        .lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequestList = sortLookupRequests(lookupServiceClientRequestCaptor.getAllValues());
    // Verify first request records
    var lookupRequest = lookupRequestList.get(0);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    assertEquals(3, lookupRequest.records().size());
    // Verify second request
    lookupRequest = lookupRequestList.get(1);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    assertEquals(3, lookupRequest.records().size());
    // Verify failed row is marked as error
    assertTrue(result.erroredLookupResults().isPresent());
    var errors = result.erroredLookupResults().get();
    var errorRecords = sortDataRecordsByKeyValue(errors.records());
    assertEquals(2, errorRecords.size());
    var dataRecord0 = errorRecords.get(0);
    assertEquals(1, dataRecord0.getKeyValuesCount());
    assertTrue(dataRecord0.hasErrorCode());
    assertEquals(DECRYPTION_ERROR, dataRecord0.getErrorCode());
    // Check phone
    assertEquals("pii_value", dataRecord0.getKeyValues(0).getKey());
    assertEquals("199-999-9999", dataRecord0.getKeyValues(0).getStringValue());
    var dataRecord1 = errorRecords.get(1);
    assertEquals(1, dataRecord1.getKeyValuesCount());
    assertTrue(dataRecord1.hasErrorCode());
    assertEquals(DECRYPTION_ERROR, dataRecord1.getErrorCode());
    // Check email
    assertEquals("pii_value", dataRecord1.getKeyValues(0).getKey());
    assertEquals("Fake.email0@google.com", dataRecord1.getKeyValues(0).getStringValue());
  }

  @Test
  public void lookup_multiEncryptionRow_OnlyStatusFailedFromLookupServer_ReturnsErrorCode()
      throws Exception {
    var dek0 = generateEncryptedDek();
    String encryptedEmail = encryptString(dek0, "Fake.email0@google.com");
    String encryptedPhone = encryptString(dek0, "099-999-9999");
    String encryptedFirstName = encryptString(dek0, "fake_first_name");
    String encryptedLastName = encryptString(dek0, "fake_last_name");
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    when(mockLookupServiceClient.lookupRecords(any(LookupServiceClientRequest.class)))
        .thenAnswer(
            invocation -> {
              var responseBuilder = LookupServiceClientResponse.builder();
              LookupServiceClientRequest request = invocation.getArgument(0);
              responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setClientDataRecord(
                              LookupProto.DataRecord.newBuilder()
                                  .setLookupKey(LookupKey.newBuilder().setKey(encryptedEmail)))
                          .setStatus(STATUS_FAILED)
                          .setErrorResponse(
                              ErrorResponse.newBuilder()
                                  .setCode(3)
                                  .setMessage(
                                      "An encryption/decryption error occurred while processing"
                                          + " the request.")
                                  .addDetails(
                                      Details.newBuilder()
                                          .setReason("2415853572")
                                          .setDomain("LookupService"))))
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setClientDataRecord(
                              LookupProto.DataRecord.newBuilder()
                                  .setLookupKey(LookupKey.newBuilder().setKey(encryptedPhone)))
                          .setStatus(STATUS_FAILED)
                          .setErrorResponse(
                              ErrorResponse.newBuilder()
                                  .setCode(3)
                                  .setMessage(
                                      "An encryption/decryption error occurred while processing"
                                          + " the request.")
                                  .addDetails(
                                      Details.newBuilder()
                                          .setReason("2415853572")
                                          .setDomain("LookupService"))));

              // Address response must be based on request input since encryption is
              // non-deterministic
              var encryptedAddressKey =
                  sortLookupDataRecordsByKeyLength(request.records())
                      .get(2)
                      .getLookupKey()
                      .getKey();
              return responseBuilder
                  .addResult(
                      LookupProto.LookupResult.newBuilder()
                          .setClientDataRecord(
                              LookupProto.DataRecord.newBuilder()
                                  .setLookupKey(LookupKey.newBuilder().setKey(encryptedAddressKey)))
                          .setStatus(STATUS_FAILED)
                          .setErrorResponse(
                              ErrorResponse.newBuilder()
                                  .setCode(3)
                                  .setMessage(
                                      "An encryption/decryption error occurred while processing"
                                          + " the request.")
                                  .addDetails(
                                      Details.newBuilder()
                                          .setReason("2415853572")
                                          .setDomain("LookupService"))))
                  .build();
            });
    String[][] testData = {
      {"email", "email", "Fake.email0@google.com"},
      {"phone", "phone", "099-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"encrypted_dek", "encrypted_dek", dek0},
      {"kek_uri", "kek_uri", "locations/testRegion/testKek"},
    };
    String[][] encryptedData = {
      {"email", encryptedEmail},
      {"phone", encryptedPhone},
      {"first_name", encryptedFirstName},
      {"last_name", encryptedLastName},
    };
    DataChunk dataChunk =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData, encryptedData))
            .setEncryptionColumns(
                DataRecordEncryptionColumns.newBuilder()
                    .addAllEncryptedColumnIndices(ImmutableList.of(0, 1, 2, 3))
                    .setEncryptionKeyColumnIndices(
                        getWrappedKeyColumnIndices(Optional.of(6), Optional.of(7)))
                    .build())
            .build();

    LookupDataSourceResult result =
        lookupServerDataSourceForV2.lookup(dataChunk, Optional.of(WRAPPED_ENCRYPTION_METADATA));

    // No matches
    assertEquals(0, result.lookupResults().records().size());
    // one requests
    verify(mockLookupServiceClient, times(1))
        .lookupRecords(lookupServiceClientRequestCaptor.capture());
    var lookupRequestList = sortLookupRequests(lookupServiceClientRequestCaptor.getAllValues());
    // Verify request records
    var lookupRequest = lookupRequestList.get(0);
    assertEquals(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED, lookupRequest.keyFormat());
    assertEquals(3, lookupRequest.records().size());
    // Verify failed row is marked as error
    assertTrue(result.erroredLookupResults().isPresent());
    var errors = result.erroredLookupResults().get();
    var errorRecords = sortDataRecordsByKeyValue(errors.records());
    assertEquals(3, errorRecords.size());
    var dataRecord0 = errorRecords.get(0);
    assertEquals(1, dataRecord0.getKeyValuesCount());
    assertTrue(dataRecord0.hasErrorCode());
    assertEquals(DECRYPTION_ERROR, dataRecord0.getErrorCode());
    // Check phone
    assertEquals("pii_value", dataRecord0.getKeyValues(0).getKey());
    assertEquals("099-999-9999", dataRecord0.getKeyValues(0).getStringValue());
    var dataRecord1 = errorRecords.get(1);
    assertEquals(1, dataRecord1.getKeyValuesCount());
    assertTrue(dataRecord1.hasErrorCode());
    assertEquals(DECRYPTION_ERROR, dataRecord1.getErrorCode());
    // Check email
    assertEquals("pii_value", dataRecord1.getKeyValues(0).getKey());
    assertEquals("Fake.email0@google.com", dataRecord1.getKeyValues(0).getStringValue());
    var dataRecord2 = errorRecords.get(2);
    assertEquals(1, dataRecord2.getKeyValuesCount());
    // Check address
    assertEquals("pii_value", dataRecord2.getKeyValues(0).getKey());
    assertEquals(
        hashString("fake_first_namefake_last_nameUS99999"),
        dataRecord2.getKeyValues(0).getStringValue());
  }

  private DataRecordProto.DataRecord getDataRecord(String[][] keyValueQuads) {
    return getDataRecord(keyValueQuads, new String[0][0]);
  }

  private DataRecordProto.DataRecord getDataRecord(
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

  private DataRecordEncryptionColumns.EncryptionKeyColumnIndices getWrappedKeyColumnIndices(
      Optional<Integer> dekIndex, Optional<Integer> kekIndex) {
    return getWrappedKeyColumnIndices(dekIndex, kekIndex, /* wipIndex= */ Optional.empty());
  }

  // TODO(b/368123408): Refactor not to use optionals
  private DataRecordEncryptionColumns.EncryptionKeyColumnIndices getWrappedKeyColumnIndices(
      Optional<Integer> dekIndex, Optional<Integer> kekIndex, Optional<Integer> wipIndex) {
    var wrappedIndicesBuilder = DataRecordEncryptionColumns.WrappedKeyColumnIndices.newBuilder();
    dekIndex.ifPresent(wrappedIndicesBuilder::setEncryptedDekColumnIndex);
    kekIndex.ifPresent(wrappedIndicesBuilder::setKekUriColumnIndex);
    wipIndex.ifPresent(
        index ->
            wrappedIndicesBuilder.setGcpColumnIndices(
                GcpWrappedKeyColumnIndices.newBuilder().setWipProviderIndex(index)));
    return DataRecordEncryptionColumns.EncryptionKeyColumnIndices.newBuilder()
        .setWrappedKeyColumnIndices(wrappedIndicesBuilder.build())
        .build();
  }

  private Schema getSchema(String[][] keyValueQuads) {
    Schema.Builder builder = Schema.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addColumns(
          Column.newBuilder()
              .setColumnName(keyValue[0])
              .setColumnAlias(keyValue[1])
              .setColumnGroup(keyValue.length > 3 ? Integer.parseInt(keyValue[3]) : 0)
              .setEncrypted(ENCRYPTED_COLUMNS.contains(keyValue[1]))
              .build());
    }
    return builder.build();
  }

  private String hashString(String s) {
    return BaseEncoding.base64().encode(sha256().hashBytes(s.getBytes(UTF_8)).asBytes());
  }

  private List<LookupDataRecord> sortLookupDataRecordsByKeyLength(
      List<LookupDataRecord> inputList) {
    var mutableList = new ArrayList<>(inputList);
    mutableList.sort(
        (record1, record2) -> {
          var key1 =
              record1.getLookupKey().hasDecryptedKey()
                  ? record1.getLookupKey().getDecryptedKey()
                  : record1.getLookupKey().getKey();
          var key2 =
              record2.getLookupKey().hasDecryptedKey()
                  ? record2.getLookupKey().getDecryptedKey()
                  : record2.getLookupKey().getKey();
          var result = key1.length() - key2.length();
          if (result != 0) {
            return result;
          } else {
            return key1.compareTo(key2);
          }
        });
    return mutableList;
  }

  private List<DataRecord> sortDataRecordsByKeyValue(List<DataRecord> inputList) {
    var mutableList = new ArrayList<>(inputList);
    mutableList.sort(
        (dataRecord1, dataRecord2) -> {
          var result = dataRecord1.getKeyValuesCount() - dataRecord2.getKeyValuesCount();
          if (result != 0) {
            return result;
          } else {
            return dataRecord1
                .getKeyValues(0)
                .getStringValue()
                .compareTo(dataRecord2.getKeyValues(0).getStringValue());
          }
        });
    return mutableList;
  }

  private List<LookupServiceClientRequest> sortLookupRequests(
      List<LookupServiceClientRequest> inputList) {
    var mutableList = new ArrayList<>(inputList);
    mutableList.sort(
        (request1, request2) ->
            // Compare the value of the decrypted email
            sortLookupDataRecordsByKeyLength(request1.records())
                .get(0)
                .getLookupKey()
                .getDecryptedKey()
                .compareTo(
                    sortLookupDataRecordsByKeyLength(request2.records())
                        .get(0)
                        .getLookupKey()
                        .getDecryptedKey()));
    return mutableList;
  }
}
