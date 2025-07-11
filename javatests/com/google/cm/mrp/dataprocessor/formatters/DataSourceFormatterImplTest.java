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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_NESTED_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_ENCRYPTION_TYPE;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridDecrypt;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridEncrypt;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.CoordinatorKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.EncryptionKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.AwsWrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.GcpWrappedKeyColumnIndices;
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
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns.WrappedKeyColumns;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.testutils.HybridKeyGenerator;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DataSourceFormatterImplTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  private static final FeatureFlags DEFAULT_FLAGS =
      FeatureFlags.builder().setEnableMIC(true).build();
  private static final EncryptionMetadata NO_WIP_WRAPPED_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setWrappedKeyInfo(
                      WrappedKeyInfo.newBuilder()
                          .setKeyType(KeyType.XCHACHA20_POLY1305)
                          .setGcpWrappedKeyInfo(GcpWrappedKeyInfo.newBuilder().setWipProvider(""))))
          .build();

  private static final EncryptionMetadata NO_ROLE_AWS_WRAPPED_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setWrappedKeyInfo(
                      WrappedKeyInfo.newBuilder()
                          .setKeyType(KeyType.XCHACHA20_POLY1305)
                          .setAwsWrappedKeyInfo(AwsWrappedKeyInfo.newBuilder().setRoleArn(""))))
          .build();

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
  private String[] emailList = {
    "FAKE.1.email@google.com", "FAKE.2.email@google.com", "FAKE.3.email@google.com"
  };
  private String[] phoneList = {"999-999-9991", "999-999-9992"};
  private String[][] addressList = {
    {"fake_1_first_name", "fake_1_last_name", "99999", "us"},
    {"fake_2_first_name", "fake_2_last_name", "99998", "ca"},
    {"fake_3_first_name", "fake_3_last_name", "99997", "us"},
    {"fake_4_first_name", "fake_4_last_name", "99996", "ca"}
  };

  private MatchConfig testMatchConfig;
  private MatchConfig encryptionKeyMatchConfig;
  private Schema testSchema;
  private Schema noEncryptionKeySchema;
  private Schema incompleteCompositeKeySchema;
  private Schema duplicateSingleColumnSchema;
  private Schema multiCompositeColumnsSchema;
  private Schema wrappedKeySchema;

  @Mock private CryptoClient mockCryptoClient;

  @Before
  public void setup() throws Exception {
    testMatchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/formatter_match_config.json")),
                UTF_8),
            MatchConfig.class);
    encryptionKeyMatchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/encryption_match_config.json")),
                UTF_8),
            MatchConfig.class);
    testSchema = getSchema("testdata/formatter_schema.json");
    noEncryptionKeySchema = getSchema("testdata/no_encryption_key_schema.json");
    incompleteCompositeKeySchema = getSchema("testdata/incomplete_composite_columns_schema.json");
    duplicateSingleColumnSchema = getSchema("testdata/duplicate_single_column_schema.json");
    multiCompositeColumnsSchema = getSchema("testdata/multi_composite_columns_schema.json");
    wrappedKeySchema = getSchema("testdata/wrapped_key_formatter_schema.json");
  }

  @Test
  public void dataSourceFormatter_throwErrorWhenNestedSchemaMissingColumnFormat() throws Exception {
    Schema inputSchema = getSchema("testdata/invalid_schema_no_column_format.json");

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(encryptionKeyMatchConfig, inputSchema, DEFAULT_FLAGS));

    assertEquals(INVALID_NESTED_SCHEMA_FILE_ERROR, ex.getErrorCode());
    assertEquals(
        "Column format not found for column gtag_grouped_pii with the nested schema.",
        ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  public void dataSourceFormatter_throwErrorWhenNestedSchemaInvalidGtagSchema() throws Exception {
    Schema inputSchema = getSchema("testdata/invalid_schema_invalid_gtag_schema.json");

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(encryptionKeyMatchConfig, inputSchema, DEFAULT_FLAGS));

    assertEquals(INVALID_NESTED_SCHEMA_FILE_ERROR, ex.getErrorCode());
    assertEquals("Schema file with invalid nested column schema.", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  public void dataSourceFormatter_formatSucceedWhenSchemaMissingEncryptionKey() {
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"pn", "phone", phoneList[1]},
      {"em", "email", emailList[2]},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      // The 'group_pii' column will be ignored as it's not included in the schema.
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
      {"fn1", "first_name", addressList[1][0]},
      {"ln1", "last_name", addressList[1][1]},
      {"pc1", "zip_code", addressList[1][2]},
      // The group index of key does not need to be consecutive
      {"co1", "country_code", addressList[1][3]},
      {"fn71", "first_name", addressList[2][0]},
      {"ln71", "last_name", addressList[2][1]},
      {"pc71", "zip_code", addressList[2][2]},
      {"co71", "country_code", addressList[2][3]},
      {"fn22", "first_name", addressList[3][0]},
      {"ln22", "last_name", addressList[3][1]},
      {"pc22", "zip_code", addressList[3][2]},
      {"co22", "country_code", addressList[3][3]},
      {"metadata", "metadata_column", "testMetadata"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(encryptionKeyMatchConfig, noEncryptionKeySchema, DEFAULT_FLAGS);

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(4, result.size());
    // Asserts output result DataRecord schema. No groupPii string column in the formatted data
    // record.
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(9, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("metadata", eachRecord.getKeyValues(7).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(8).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(emailList).collect(Collectors.toList()), formattedEmailList);
    // Asserts format phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(phoneList).collect(Collectors.toList()), formattedPhoneList);
    // Asserts formatted error_codes only has one value.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formattedErrorCodes);
    // Asserts format address keys are grouped as-is.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts formatted metadata column only has one value.
    var formattedMetadataList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(7).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(7).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("testMetadata"), formattedMetadataList);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(4, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
    // Asserts the output data record doesn't have error_code.
    result.forEach(record -> assertFalse(record.hasErrorCode()));
  }

  @Test
  public void dataSourceFormatter_jobHasCoordinatorKeyButNotSchema_throwsError() {
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    testMatchConfig,
                    incompleteCompositeKeySchema,
                    DEFAULT_FLAGS,
                    COORDINATOR_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage())
        .isEqualTo("Job has CoordinatorKey parameter but match config does not support it.");
    assertThat(ex.getErrorCode()).isEqualTo(UNSUPPORTED_ENCRYPTION_TYPE);
  }

  @Test
  public void dataSourceFormatter_badSchema_throwsError() {
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    encryptionKeyMatchConfig,
                    incompleteCompositeKeySchema,
                    DEFAULT_FLAGS,
                    COORDINATOR_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage()).isEqualTo("Composite column address missing key in schema.");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_SCHEMA_FILE_ERROR);
  }

  @Test
  public void dataSourceFormatter_throwErrorWithSchemaMissingEncryptionKeyColumn() {
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    encryptionKeyMatchConfig,
                    noEncryptionKeySchema,
                    DEFAULT_FLAGS,
                    COORDINATOR_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage()).isEqualTo("Coordinator key ID column missing in schema");
    assertThat(ex.getErrorCode()).isEqualTo(MISSING_ENCRYPTION_COLUMN);
  }

  @Test
  public void dataSourceFormatter_schemaMissingWipColumn_throws() {
    var wrappedKeyColumns =
        encryptionKeyMatchConfig.getEncryptionKeyColumns().getWrappedKeyColumns();
    var testConfig =
        encryptionKeyMatchConfig.toBuilder()
            .setEncryptionKeyColumns(
                encryptionKeyMatchConfig.getEncryptionKeyColumns().toBuilder()
                    .setWrappedKeyColumns(
                        WrappedKeyColumns.newBuilder()
                            .setEncryptedDekColumnAlias(
                                wrappedKeyColumns.getEncryptedDekColumnAlias())
                            .setKekUriColumnAlias(wrappedKeyColumns.getKekUriColumnAlias())));
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    testConfig.build(),
                    wrappedKeySchema,
                    DEFAULT_FLAGS,
                    NO_WIP_WRAPPED_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage())
        .isEqualTo("WIP missing in request and no WIP alias in match config.");
    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_COLUMNS_CONFIG_ERROR);
  }

  @Test
  public void dataSourceFormatter_matchConfigMissingWipColumn_throws() {
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    encryptionKeyMatchConfig,
                    wrappedKeySchema,
                    DEFAULT_FLAGS,
                    NO_WIP_WRAPPED_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage()).isEqualTo("WIP missing in request and no WIP column in schema.");
    assertThat(ex.getErrorCode()).isEqualTo(MISSING_ENCRYPTION_COLUMN);
  }

  @Test
  public void dataSourceFormatter_schemaMissingRoleArnColumn_throws() {
    var wrappedKeyColumns =
        encryptionKeyMatchConfig.getEncryptionKeyColumns().getWrappedKeyColumns();
    var testConfig =
        encryptionKeyMatchConfig.toBuilder()
            .setEncryptionKeyColumns(
                encryptionKeyMatchConfig.getEncryptionKeyColumns().toBuilder()
                    .setWrappedKeyColumns(
                        WrappedKeyColumns.newBuilder()
                            .setEncryptedDekColumnAlias(
                                wrappedKeyColumns.getEncryptedDekColumnAlias())
                            .setKekUriColumnAlias(wrappedKeyColumns.getKekUriColumnAlias())));
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    testConfig.build(),
                    wrappedKeySchema,
                    DEFAULT_FLAGS,
                    NO_ROLE_AWS_WRAPPED_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage())
        .isEqualTo("Role ARN missing in request and no Role ARN alias in match config.");
    assertThat(ex.getErrorCode()).isEqualTo(ENCRYPTION_COLUMNS_CONFIG_ERROR);
  }

  @Test
  public void dataSourceFormatter_matchConfigMissingRoleArnColumn_throws() {
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataSourceFormatterImpl(
                    encryptionKeyMatchConfig,
                    wrappedKeySchema,
                    DEFAULT_FLAGS,
                    NO_ROLE_AWS_WRAPPED_ENCRYPTION_METADATA,
                    mockCryptoClient));
    assertThat(ex.getMessage())
        .isEqualTo("Role ARN missing in request and no Role ARN column in schema.");
    assertThat(ex.getErrorCode()).isEqualTo(MISSING_ENCRYPTION_COLUMN);
  }

  @Test
  public void dataSourceFormatter_getDataRecordEncryptionColumnsForCoordinator() {
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            testSchema,
            DEFAULT_FLAGS,
            COORDINATOR_ENCRYPTION_METADATA,
            mockCryptoClient);
    var expectedDataRecordEncryptionColumns =
        DataRecordEncryptionColumns.newBuilder()
            .addAllEncryptedColumnIndices(List.of(0, 1, 2, 3))
            .setEncryptionKeyColumnIndices(
                EncryptionKeyColumnIndices.newBuilder()
                    .setCoordinatorKeyColumnIndices(
                        CoordinatorKeyColumnIndices.newBuilder().setCoordinatorKeyColumnIndex(7)))
            .build();

    var dataRecordEncryptionColumns = formatter.getDataRecordEncryptionColumns();

    assertTrue(dataRecordEncryptionColumns.isPresent());
    assertThat(dataRecordEncryptionColumns.get()).isEqualTo(expectedDataRecordEncryptionColumns);
  }

  @Test
  public void dataSourceFormatter_getDataRecordEncryptionColumnsForRowGcpWrappedKeys() {
    var schemaWithWip =
        wrappedKeySchema.toBuilder()
            .addColumns(
                Column.newBuilder()
                    .setColumnType(ColumnType.STRING)
                    .setColumnAlias("wip_provider")
                    .setColumnName("WIP"));
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            schemaWithWip.build(),
            DEFAULT_FLAGS,
            NO_WIP_WRAPPED_ENCRYPTION_METADATA,
            mockCryptoClient);
    var expectedDataRecordEncryptionColumns =
        DataRecordEncryptionColumns.newBuilder()
            .addAllEncryptedColumnIndices(List.of(0, 1, 2, 3))
            .setEncryptionKeyColumnIndices(
                EncryptionKeyColumnIndices.newBuilder()
                    .setWrappedKeyColumnIndices(
                        // wrappedKeySchema has 7 regular columns
                        WrappedKeyColumnIndices.newBuilder()
                            .setEncryptedDekColumnIndex(7)
                            .setKekUriColumnIndex(8)
                            .setGcpColumnIndices(
                                GcpWrappedKeyColumnIndices.newBuilder().setWipProviderIndex(9))))
            .build();

    var dataRecordEncryptionColumns = formatter.getDataRecordEncryptionColumns();

    assertTrue(dataRecordEncryptionColumns.isPresent());
    assertThat(dataRecordEncryptionColumns.get()).isEqualTo(expectedDataRecordEncryptionColumns);
  }

  @Test
  public void dataSourceFormatter_getDataRecordEncryptionColumnsForRowAwsWrappedKeys() {
    var schemaWithWip =
        wrappedKeySchema.toBuilder()
            .addColumns(
                Column.newBuilder()
                    .setColumnType(ColumnType.STRING)
                    .setColumnAlias("role_arn")
                    .setColumnName("ROLE"));
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            schemaWithWip.build(),
            DEFAULT_FLAGS,
            NO_ROLE_AWS_WRAPPED_ENCRYPTION_METADATA,
            mockCryptoClient);
    var expectedDataRecordEncryptionColumns =
        DataRecordEncryptionColumns.newBuilder()
            .addAllEncryptedColumnIndices(List.of(0, 1, 2, 3))
            .setEncryptionKeyColumnIndices(
                EncryptionKeyColumnIndices.newBuilder()
                    .setWrappedKeyColumnIndices(
                        // wrappedKeySchema has 7 regular columns
                        WrappedKeyColumnIndices.newBuilder()
                            .setEncryptedDekColumnIndex(7)
                            .setKekUriColumnIndex(8)
                            .setAwsColumnIndices(
                                AwsWrappedKeyColumnIndices.newBuilder().setRoleArnIndex(9))))
            .build();

    var dataRecordEncryptionColumns = formatter.getDataRecordEncryptionColumns();

    assertTrue(dataRecordEncryptionColumns.isPresent());
    assertThat(dataRecordEncryptionColumns.get()).isEqualTo(expectedDataRecordEncryptionColumns);
  }

  @Test
  public void formatDataRecord_success() throws Exception {
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"pn", "phone", phoneList[1]},
      {"em", "email", emailList[2]},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      // The 'group_pii' column will be ignored as it's not included in the schema.
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
      {"fn1", "first_name", addressList[1][0]},
      {"ln1", "last_name", addressList[1][1]},
      {"pc1", "zip_code", addressList[1][2]},
      // The group index of key does not need to be consecutive
      {"co1", "country_code", addressList[1][3]},
      {"fn71", "first_name", addressList[2][0]},
      {"ln71", "last_name", addressList[2][1]},
      {"pc71", "zip_code", addressList[2][2]},
      {"co71", "country_code", addressList[2][3]},
      {"fn22", "first_name", addressList[3][0]},
      {"ln22", "last_name", addressList[3][1]},
      {"pc22", "zip_code", addressList[3][2]},
      {"co22", "country_code", addressList[3][3]},
      // Coordinator key column name ending in digits should not be confused with composite keys.
      {"key_id123", "coordinator_key_id", "testCoordKey"},
      {"metadata", "metadata_column", "testMetadata"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            testSchema,
            DEFAULT_FLAGS,
            COORDINATOR_ENCRYPTION_METADATA,
            mockCryptoClient);
    when(mockCryptoClient.encrypt(any(), anyString())).thenReturn("encrypted");

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(4, result.size());
    // Asserts output result DataRecord schema. No groupPii string column in the formatted data
    // record.
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(10, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("key_id123", eachRecord.getKeyValues(7).getKey());
              assertEquals("metadata", eachRecord.getKeyValues(8).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(9).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(emailList).collect(Collectors.toList()), formattedEmailList);
    // Asserts format phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(phoneList).collect(Collectors.toList()), formattedPhoneList);
    // Asserts formatted error_codes only has one value.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formattedErrorCodes);
    // Asserts format address keys are grouped as-is.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts formatted encryption key has being added to each record split.
    result.stream()
        .forEach(
            eachRecord ->
                assertEquals("testCoordKey", eachRecord.getKeyValues(7).getStringValue()));
    // Asserts formatted metadata column only has one value.
    var formattedMetadataList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("testMetadata"), formattedMetadataList);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(9).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(9).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(4, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
    // Asserts the output data record doesn't have error_code.
    result.forEach(record -> assertFalse(record.hasErrorCode()));
  }

  @Test
  public void formatDataRecordForBatchEncryption_success() {
    FeatureFlags featureFlagsWithBatchEncryption =
        FeatureFlags.builder()
            .setEnableMIC(true)
            .setCoordinatorBatchEncryptionEnabled(true)
            .build();
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"pn", "phone", phoneList[1]},
      {"em", "email", emailList[2]},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      // The 'group_pii' column will be ignored as it's not included in the schema.
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
      {"fn1", "first_name", addressList[1][0]},
      {"ln1", "last_name", addressList[1][1]},
      {"pc1", "zip_code", addressList[1][2]},
      // The group index of key does not need to be consecutive
      {"co1", "country_code", addressList[1][3]},
      {"fn71", "first_name", addressList[2][0]},
      {"ln71", "last_name", addressList[2][1]},
      {"pc71", "zip_code", addressList[2][2]},
      {"co71", "country_code", addressList[2][3]},
      {"fn22", "first_name", addressList[3][0]},
      {"ln22", "last_name", addressList[3][1]},
      {"pc22", "zip_code", addressList[3][2]},
      {"co22", "country_code", addressList[3][3]},
      // Coordinator key column name ending in digital will not be confused with composite keys.
      {"key_id123", "coordinator_key_id", "testCoordKey"},
      {"metadata", "metadata_column", "testMetadata"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            testSchema,
            featureFlagsWithBatchEncryption,
            COORDINATOR_ENCRYPTION_METADATA,
            mockCryptoClient);

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(4, result.size());
    // Asserts output result DataRecord schema. No groupPii string column in the formatted data
    // record.
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(10, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("key_id123", eachRecord.getKeyValues(7).getKey());
              assertEquals("metadata", eachRecord.getKeyValues(8).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(9).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(emailList).collect(Collectors.toList()), formattedEmailList);
    // Asserts format phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(phoneList).collect(Collectors.toList()), formattedPhoneList);
    // Asserts formatted error_codes only has one value.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formattedErrorCodes);
    // Asserts format address keys are grouped as-is.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts formatted encryption key has being added to each record split.
    result.stream()
        .forEach(
            eachRecord ->
                assertEquals("testCoordKey", eachRecord.getKeyValues(7).getStringValue()));
    // Asserts formatted metadata column only has one value.
    var formattedMetadataList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("testMetadata"), formattedMetadataList);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(9).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(9).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(4, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
    // Asserts the output data record doesn't have error_code.
    result.forEach(record -> assertFalse(record.hasErrorCode()));

    // Verify that encrypted key values are not populated for batch encryption enabled use case
    result.forEach(record -> assertTrue(record.getEncryptedKeyValuesCount() == 0));
  }

  @Test
  public void formatDataRecord_singleColumnPiiEncryption() throws Exception {
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
      {"key_id123", "coordinator_key_id", "testCoordKey"},
      {"metadata", "metadata_column", "testMetadata"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            testSchema,
            DEFAULT_FLAGS,
            COORDINATOR_ENCRYPTION_METADATA,
            mockCryptoClient);
    when(mockCryptoClient.encrypt(any(), anyString()))
        .thenAnswer(
            invocation -> {
              String plaintext = (String) invocation.getArguments()[1];
              return HybridKeyGenerator.encryptString(getDefaultHybridEncrypt(), plaintext);
            });

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(2, result.size());
    // Asserts output result DataRecord schema. No groupPii string column in the formatted data
    // record.
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(10, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("key_id123", eachRecord.getKeyValues(7).getKey());
              assertEquals("metadata", eachRecord.getKeyValues(8).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(9).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(emailList).limit(2).collect(Collectors.toList()), formattedEmailList);
    // Asserts format phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(phoneList).limit(1).collect(Collectors.toList()), formattedPhoneList);
    // Asserts formatted error_codes only has one value.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formattedErrorCodes);
    // Asserts format address keys are grouped as-is.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .limit(1)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts formatted encryption key has being added to each record split.
    result.stream()
        .forEach(
            eachRecord ->
                assertEquals("testCoordKey", eachRecord.getKeyValues(7).getStringValue()));
    // Asserts formatted metadata column only has one value.
    var formattedMetadataList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("testMetadata"), formattedMetadataList);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(9).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(9).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(2, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
    // Asserts the encryptedKeyValues. Only `em` and `ph` columns have encrypted key value.
    assertEquals(2, result.get(0).getEncryptedKeyValuesCount());
    assertEquals(
        emailList[0],
        HybridKeyGenerator.decryptString(
            getDefaultHybridDecrypt(), result.get(0).getEncryptedKeyValuesOrThrow(0)));
    assertEquals(
        phoneList[0],
        HybridKeyGenerator.decryptString(
            getDefaultHybridDecrypt(), result.get(0).getEncryptedKeyValuesOrThrow(1)));
    assertEquals(1, result.get(1).getEncryptedKeyValuesCount());
    assertEquals(
        emailList[1],
        HybridKeyGenerator.decryptString(
            getDefaultHybridDecrypt(), result.get(1).getEncryptedKeyValuesOrThrow(0)));
  }

  @Test
  public void formatDataRecord_dataRecordWithoutParsedPiiData() {
    String[][] testData = {
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"key_id123", "coordinator_key_id", "testCoordKey"},
      {"metadata", "metadata_column", "testMetadata"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    var dataRecordWithErrorCode = dataRecord.toBuilder().setErrorCode(DECRYPTION_ERROR).build();
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(testMatchConfig, testSchema, DEFAULT_FLAGS);

    ImmutableList<DataRecord> result = formatter.format(dataRecordWithErrorCode);

    // Empty data record return data record with defined schema.
    assertEquals(1, result.size());
    // Asserts the output data record contains error_code.
    assertEquals(result.get(0).getErrorCode(), DECRYPTION_ERROR);
    // Asserts output result DataRecord schema.
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(10, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("key_id123", eachRecord.getKeyValues(7).getKey());
              assertEquals("metadata", eachRecord.getKeyValues(8).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(9).getKey());
            });
    // Asserts formatted email column is empty.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    Assert.assertTrue(formattedEmailList.isEmpty());

    // Asserts format phone number column is empty
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    Assert.assertTrue(formattedPhoneList.isEmpty());

    // Asserts format error_codes column is empty.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    Assert.assertTrue(formattedErrorCodes.isEmpty());

    // Asserts format address keys columns are empty.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    Assert.assertTrue(formattedAddress.isEmpty());
    // Asserts format encryption key has being added to each record split.
    result.stream()
        .forEach(
            eachRecord ->
                assertEquals("testCoordKey", eachRecord.getKeyValues(7).getStringValue()));
    // Asserts format metadata column only has one value.
    var formattedMetadataList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("testMetadata"), formattedMetadataList);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(9).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(9).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(1, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
  }

  @Test
  public void formatDataRecord_throwErrorWhenMissingEncryptionKey() {
    String[][] testData = {
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"metadata", "metadata_column", "testMetadata"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(
            encryptionKeyMatchConfig,
            testSchema,
            DEFAULT_FLAGS,
            COORDINATOR_ENCRYPTION_METADATA,
            mockCryptoClient);

    var ex = assertThrows(JobProcessorException.class, () -> formatter.format(dataRecord));
    assertThat(ex.getMessage()).isEqualTo("Invalid encryption key in data record");
  }

  @Test
  public void formatDataRecord_noEncryptionKeyInSchema() {
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"pn", "phone", phoneList[1]},
      {"em", "email", emailList[2]},
      {"group_pii", "groupPiiColumn", "testGroupPii"},
      {"metadata", "metadata", "testMetadata"},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
      {"key_id", "coordinator_key_id", "testCoordKey"},
    };
    DataRecord dataRecord = getDataRecord(testData);
    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(testMatchConfig, noEncryptionKeySchema, DEFAULT_FLAGS);

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(3, result.size());
    // Asserts output result DataRecord schema
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(9, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("metadata", eachRecord.getKeyValues(7).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(8).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(emailList).collect(Collectors.toList()), formattedEmailList);
    // Asserts formatted phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(phoneList).collect(Collectors.toList()), formattedPhoneList);
    // Asserts formatted error_codes only has one value.
    var formatErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formatErrorCodes);
    // Asserts formatted address keys are grouped as-is.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .limit(1)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts formatted metadata column only has one value.
    var formattedMetadataList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(7).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(7).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("testMetadata"), formattedMetadataList);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(3, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
  }

  @Test
  public void formatDataRecord_duplicateSingleColumnInSchema() {
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"pn", "phone", phoneList[1]},
      {"em", "email", emailList[2]},
      {"metadata", "metadata", "testMetadata"},
      {"coordinator_key_id", "coordinator_key_id", "testCoordinatorKey"},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
    };
    DataRecord dataRecord = getDataRecord(testData);

    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(testMatchConfig, duplicateSingleColumnSchema, DEFAULT_FLAGS);

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(2, result.size());
    // Asserts output result DataRecord schema
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(9, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("em", eachRecord.getKeyValues(7).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(8).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        Stream.concat(
                result.stream()
                    .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
                    .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue()),
                result.stream()
                    .filter(eachRecord -> !eachRecord.getKeyValues(7).getStringValue().isEmpty())
                    .map(eachRecord -> eachRecord.getKeyValues(7).getStringValue()))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(emailList).collect(Collectors.toList()), formattedEmailList);
    // Asserts formatted phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(phoneList).collect(Collectors.toList()), formattedPhoneList);
    // Asserts format error_codes only has one value.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formattedErrorCodes);
    // Asserts format address keys are grouped as-is.
    var formattedAddress =
        result.stream()
            .filter(
                eachRecord ->
                    !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                        || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
            .map(
                eachRecord ->
                    eachRecord.getKeyValues(2).getStringValue()
                        + eachRecord.getKeyValues(3).getStringValue()
                        + eachRecord.getKeyValues(4).getStringValue()
                        + eachRecord.getKeyValues(5).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .limit(1)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(8).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(8).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(2, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
  }

  @Test
  public void formatDataRecord_multiCompositeColumnInSchema() {
    String[][] testData = {
      {"em", "email", emailList[0]},
      {"pn", "phone", phoneList[0]},
      {"em", "email", emailList[1]},
      {"pn", "phone", phoneList[1]},
      {"em", "email", emailList[2]},
      {"error_codes", "error_codes", "em.e1~em.e3"},
      {"fn0", "first_name", addressList[0][0]},
      {"ln0", "last_name", addressList[0][1]},
      {"pc0", "zip_code", addressList[0][2]},
      {"co0", "country_code", addressList[0][3]},
      {"fn1", "first_name", addressList[1][0]},
      {"ln1", "last_name", addressList[1][1]},
      {"pc1", "zip_code", addressList[1][2]},
      {"co1", "country_code", addressList[1][3]},
      {"fn2", "first_name", addressList[2][0]},
      {"ln2", "last_name", addressList[2][1]},
      {"pc2", "zip_code", addressList[2][2]},
      {"co2", "country_code", addressList[2][3]},
      {"fn3", "first_name", addressList[3][0]},
      {"ln3", "last_name", addressList[3][1]},
      {"pc3", "zip_code", addressList[3][2]},
      {"co3", "country_code", addressList[3][3]},
    };
    DataRecord dataRecord = getDataRecord(testData);

    DataSourceFormatterImpl formatter =
        new DataSourceFormatterImpl(testMatchConfig, multiCompositeColumnsSchema, DEFAULT_FLAGS);

    ImmutableList<DataRecord> result = formatter.format(dataRecord);

    assertEquals(3, result.size());
    // Asserts output result DataRecord schema
    result.stream()
        .forEach(
            eachRecord -> {
              assertEquals(12, eachRecord.getKeyValuesCount());
              assertEquals("em", eachRecord.getKeyValues(0).getKey());
              assertEquals("pn", eachRecord.getKeyValues(1).getKey());
              assertEquals("fn", eachRecord.getKeyValues(2).getKey());
              assertEquals("ln", eachRecord.getKeyValues(3).getKey());
              assertEquals("pc", eachRecord.getKeyValues(4).getKey());
              assertEquals("co", eachRecord.getKeyValues(5).getKey());
              assertEquals("error_codes", eachRecord.getKeyValues(6).getKey());
              assertEquals("fn", eachRecord.getKeyValues(7).getKey());
              assertEquals("ln", eachRecord.getKeyValues(8).getKey());
              assertEquals("pc", eachRecord.getKeyValues(9).getKey());
              assertEquals("co", eachRecord.getKeyValues(10).getKey());
              assertEquals(ROW_MARKER_COLUMN_NAME, eachRecord.getKeyValues(11).getKey());
            });
    // Asserts formatted email list contains all emails.
    var formattedEmailList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(0).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(0).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(emailList).collect(Collectors.toList()), formattedEmailList);
    // Asserts formatted phone number list contains all phone numbers
    var formattedPhoneList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(1).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(1).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(Arrays.stream(phoneList).collect(Collectors.toList()), formattedPhoneList);
    // Asserts formatted error_codes only has one value.
    var formattedErrorCodes =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(6).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(6).getStringValue())
            .collect(Collectors.toList());
    assertEquals(List.of("em.e1~em.e3"), formattedErrorCodes);
    // Asserts formatted address keys are grouped as-is.
    var formattedAddress =
        Stream.concat(
                result.stream()
                    .filter(
                        eachRecord ->
                            !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                                || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                                || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                                || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
                    .map(
                        eachRecord ->
                            eachRecord.getKeyValues(2).getStringValue()
                                + eachRecord.getKeyValues(3).getStringValue()
                                + eachRecord.getKeyValues(4).getStringValue()
                                + eachRecord.getKeyValues(5).getStringValue()),
                result.stream()
                    .filter(
                        eachRecord ->
                            !eachRecord.getKeyValues(2).getStringValue().isEmpty()
                                || !eachRecord.getKeyValues(3).getStringValue().isEmpty()
                                || !eachRecord.getKeyValues(4).getStringValue().isEmpty()
                                || !eachRecord.getKeyValues(5).getStringValue().isEmpty())
                    .map(
                        eachRecord ->
                            eachRecord.getKeyValues(7).getStringValue()
                                + eachRecord.getKeyValues(8).getStringValue()
                                + eachRecord.getKeyValues(9).getStringValue()
                                + eachRecord.getKeyValues(10).getStringValue()))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(
        Arrays.stream(addressList)
            .map(list -> String.join("", list))
            .sorted()
            .collect(Collectors.toList()),
        formattedAddress);
    // Asserts all data records have the same row_marker value.
    var rowMarkerList =
        result.stream()
            .filter(eachRecord -> !eachRecord.getKeyValues(11).getStringValue().isEmpty())
            .map(eachRecord -> eachRecord.getKeyValues(11).getStringValue())
            .sorted()
            .collect(Collectors.toList());
    assertEquals(3, rowMarkerList.size());
    assertTrue(rowMarkerList.stream().distinct().count() == 1);
  }

  private DataRecordProto.DataRecord getDataRecord(String[][] keyValueQuads) {
    DataRecordProto.DataRecord.Builder builder = DataRecordProto.DataRecord.newBuilder();
    for (int i = 0; i < keyValueQuads.length; ++i) {
      builder.addKeyValues(
          DataRecordProto.DataRecord.KeyValue.newBuilder()
              .setKey(keyValueQuads[i][0])
              .setStringValue(keyValueQuads[i][2]));
    }
    return builder.build();
  }

  private Schema getSchema(String path) throws Exception {
    return ProtoUtils.getProtoFromJson(
        Resources.toString(Objects.requireNonNull(getClass().getResource(path)), UTF_8),
        Schema.class);
  }
}
