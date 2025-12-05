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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_DUPLICATE_METADATA_KEY;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_MATCH_KEY_MISSING_FIELD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_METADATA_CONTAINING_RESTRICTED_ALIAS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_MISSING_MATCH_KEYS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_METADATA_EXCEEDS_LIMIT;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.AwsWrappedKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.CoordinatorKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.GcpWrappedKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchKey;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.AwsWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ConfidentialMatchDataRecordParserImplTest {

  private static final String TEST_KEY_ID = "test-key-id";
  private static final MatchConfig MIC_MATCH_CONFIG = getMatchConfig("mic");

  // Column Keys
  private static final String METADATA_KEY = "metadata";
  private static final String EMAIL_KEY = "email";
  private static final String PHONE_KEY = "phone";
  private static final String ADDRESS_KEY = "address";
  private static final String FIRST_NAME_KEY = "first_name";
  private static final String LAST_NAME_KEY = "last_name";
  private static final String COUNTRY_CODE_KEY = "country_code";
  private static final String ZIP_CODE_KEY = "zip_code";
  private static final String COORDINATOR_KEY_ID_KEY = "coordinator_key_id";
  private static final String ENCRYPTED_DEK_KEY = "encrypted_dek";
  private static final String KEK_URI_KEY = "kek_uri";
  private static final String WIP_PROVIDER_KEY = "wip_provider";
  private static final String ROLE_ARN_KEY = "role_arn";

  // Field indices based on testdata/mic_proto_schema_encrypted.json
  private static final int METADATA_INDEX = 0;
  private static final int COORDINATOR_KEY_ID_INDEX = 1;
  private static final int EMAIL_INDEX = 2;
  private static final int PHONE_INDEX = 3;
  private static final int FIRST_NAME_INDEX = 4;
  private static final int LAST_NAME_INDEX = 5;
  private static final int COUNTRY_CODE_INDEX = 6;
  private static final int ZIP_CODE_INDEX = 7;
  private static final int ROW_MARKER_INDEX = 8;
  private static final int ADDRESS_GROUP = 0;

  // Test Values
  private static final String USER_EMAIL = "user@example.com";
  private static final String USER_PHONE = "1234567890";
  private static final String FAKE_METADATA_VALUE = "fake metadata";
  private static final String FAKE_METADATA_VALUE_2 = "fake metadata 2";

  private static final EncryptionKey DEFAULT_COORDINATOR_KEY =
      EncryptionKey.newBuilder()
          .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId(TEST_KEY_ID).build())
          .build();

  private static final EncryptionMetadata TEST_GCP_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setWrappedKeyInfo(
                      WrappedKeyInfo.newBuilder()
                          .setGcpWrappedKeyInfo(
                              GcpWrappedKeyInfo.newBuilder().setWipProvider("testWip"))))
          .build();

  private static final EncryptionMetadata TEST_COORDINATOR_ENCRYPTION_METADATA =
      EncryptionMetadata.newBuilder()
          .setEncryptionKeyInfo(
              EncryptionKeyInfo.newBuilder()
                  .setCoordinatorKeyInfo(CoordinatorKeyInfo.getDefaultInstance()))
          .build();

  private static final ImmutableList<CompositeChildField> DEFAULT_ADDRESS_FIELDS =
      ImmutableList.of(
          createCompositeChildField(FIRST_NAME_KEY, "John"),
          createCompositeChildField(LAST_NAME_KEY, "Doe"),
          createCompositeChildField(COUNTRY_CODE_KEY, "US"),
          createCompositeChildField(ZIP_CODE_KEY, "94043"));

  @Test
  public void parse_encryptedMultipleEncryptionKey() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(coordKey("123"))
            .setField(Field.newBuilder().setKeyValue(kv(EMAIL_KEY, USER_EMAIL)).build())
            .build();
    ImmutableList<CompositeChildField> addressFields =
        ImmutableList.of(
            createCompositeChildField(FIRST_NAME_KEY, "fname1"),
            createCompositeChildField(LAST_NAME_KEY, "lname1"),
            createCompositeChildField(COUNTRY_CODE_KEY, "usa"),
            createCompositeChildField(ZIP_CODE_KEY, "12345"));
    MatchKey addressKey =
        MatchKey.newBuilder()
            .setEncryptionKey(coordKey("124"))
            .setCompositeField(
                CompositeField.newBuilder()
                    .setKey(ADDRESS_KEY)
                    .addAllChildFields(addressFields)
                    .build())
            .build();
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(ImmutableList.of(email, addressKey))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(2);
    DataRecord record1 = resultList.get(0);
    DataRecord record2 = resultList.get(1);
    // Common expectations
    assertThat(record1.hasFieldLevelMetadata()).isFalse();
    assertThat(record2.hasFieldLevelMetadata()).isFalse();
    assertThat(record1.getKeyValues(ROW_MARKER_INDEX).getStringValue())
        .isEqualTo(record2.getKeyValues(ROW_MARKER_INDEX).getStringValue());
    // Find records by key ID
    DataRecord emailRecord =
        record1.getKeyValues(COORDINATOR_KEY_ID_INDEX).getStringValue().equals("123")
            ? record1
            : record2;
    DataRecord addressRecord =
        record1.getKeyValues(COORDINATOR_KEY_ID_INDEX).getStringValue().equals("124")
            ? record1
            : record2;

    // Top-level metadata is only on the first record. Find which record has it.
    DataRecord recordWithMetadata =
        record1.getKeyValues(METADATA_INDEX).getStringValue().equals(FAKE_METADATA_VALUE)
            ? record1
            : record2;
    DataRecord recordWithoutMetadata =
        record1.getKeyValues(METADATA_INDEX).getStringValue().equals(FAKE_METADATA_VALUE)
            ? record2
            : record1;

    assertThat(recordWithMetadata.getKeyValues(METADATA_INDEX))
        .isEqualTo(drKv(METADATA_KEY, FAKE_METADATA_VALUE));
    assertThat(recordWithoutMetadata.getKeyValues(METADATA_INDEX).getKey()).isEqualTo(METADATA_KEY);
    assertThat(recordWithoutMetadata.getKeyValues(METADATA_INDEX).hasStringValue()).isFalse();

    // Email Record assertions
    assertThat(emailRecord.getKeyValues(COORDINATOR_KEY_ID_INDEX))
        .isEqualTo(drKv(COORDINATOR_KEY_ID_KEY, "123"));
    assertThat(emailRecord.getKeyValues(EMAIL_INDEX)).isEqualTo(drKv(EMAIL_KEY, USER_EMAIL));
    // Address Record assertions
    assertThat(addressRecord.getKeyValues(COORDINATOR_KEY_ID_INDEX))
        .isEqualTo(drKv(COORDINATOR_KEY_ID_KEY, "124"));
    assertThat(addressRecord.getKeyValues(FIRST_NAME_INDEX))
        .isEqualTo(drKv(FIRST_NAME_KEY, "fname1"));
    assertThat(addressRecord.getKeyValues(LAST_NAME_INDEX))
        .isEqualTo(drKv(LAST_NAME_KEY, "lname1"));
    assertThat(addressRecord.getKeyValues(COUNTRY_CODE_INDEX))
        .isEqualTo(drKv(COUNTRY_CODE_KEY, "usa"));
    assertThat(addressRecord.getKeyValues(ZIP_CODE_INDEX)).isEqualTo(drKv(ZIP_CODE_KEY, "12345"));
  }

  @Test
  public void parse_singleFieldWithMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    MatchKey emailWithMetadata =
        createEmailMatchKey(USER_EMAIL, kv("source", "web"), kv("confidence", 0.95));
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder().addMatchKeys(emailWithMetadata).build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.getKeyValues(EMAIL_INDEX)).isEqualTo(drKv(EMAIL_KEY, USER_EMAIL));
    // validate email metadata
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    assertEmailMetadata(internalRecord, drKv("source", "web"), drKv("confidence", 0.95));
  }

  @Test
  public void parse_compositeFieldWithMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    MatchKey addressKeyWithMetadata =
        createDefaultAddressMatchKey(kv("type", "home"), kv("confidence", 0.9));
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder().addMatchKeys(addressKeyWithMetadata).build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertDataRecordKeyValue(internalRecord, FIRST_NAME_INDEX, FIRST_NAME_KEY, "John");
    assertDataRecordKeyValue(internalRecord, LAST_NAME_INDEX, LAST_NAME_KEY, "Doe");
    assertDataRecordKeyValue(internalRecord, COUNTRY_CODE_INDEX, COUNTRY_CODE_KEY, "US");
    assertDataRecordKeyValue(internalRecord, ZIP_CODE_INDEX, ZIP_CODE_KEY, "94043");
    // validate address metadata
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    assertAddressMetadata(internalRecord, drKv("type", "home"), drKv("confidence", 0.9));
  }

  @Test
  public void parse_allFieldsWithMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(
                ImmutableList.of(
                    createEmailMatchKey(USER_EMAIL, kv("source", "web"), kv("confidence", 0.95)),
                    createPhoneMatchKey(USER_PHONE, kv("type", "mobile")),
                    createDefaultAddressMatchKey(kv("type", "home"), kv("confidence", 0.9))))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    // validate email and email metadata
    assertDataRecordKeyValue(internalRecord, EMAIL_INDEX, EMAIL_KEY, USER_EMAIL);
    assertEmailMetadata(internalRecord, drKv("source", "web"), drKv("confidence", 0.95));
    // validate phone and phone metadata
    assertDataRecordKeyValue(internalRecord, PHONE_INDEX, PHONE_KEY, USER_PHONE);
    assertPhoneMetadata(internalRecord, drKv("type", "mobile"));
    // validate address and address metadata
    assertDataRecordKeyValue(internalRecord, FIRST_NAME_INDEX, FIRST_NAME_KEY, "John");
    assertDataRecordKeyValue(internalRecord, LAST_NAME_INDEX, LAST_NAME_KEY, "Doe");
    assertDataRecordKeyValue(internalRecord, COUNTRY_CODE_INDEX, COUNTRY_CODE_KEY, "US");
    assertDataRecordKeyValue(internalRecord, ZIP_CODE_INDEX, ZIP_CODE_KEY, "94043");
    assertAddressMetadata(internalRecord, drKv("type", "home"), drKv("confidence", 0.9));
  }

  @Test
  public void parse_emailMetadata_phoneNoMetadata_addressMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(
                ImmutableList.of(
                    createEmailMatchKey(USER_EMAIL, kv("source", "web"), kv("confidence", 0.95)),
                    createPhoneMatchKey(USER_PHONE),
                    createDefaultAddressMatchKey(kv("type", "home"), kv("confidence", 0.9))))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    assertEmailMetadata(internalRecord, drKv("source", "web"), drKv("confidence", 0.95));
    assertNoPhoneMetadata(internalRecord);
    assertAddressMetadata(internalRecord, drKv("type", "home"), drKv("confidence", 0.9));
  }

  @Test
  public void parse_emailNoMetadata_phoneMetadata_addressMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(
                ImmutableList.of(
                    createEmailMatchKey(USER_EMAIL),
                    createPhoneMatchKey(USER_PHONE, kv("type", "mobile")),
                    createDefaultAddressMatchKey(kv("type", "home"), kv("confidence", 0.9))))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    assertNoEmailMetadata(internalRecord);
    assertPhoneMetadata(internalRecord, drKv("type", "mobile"));
    assertAddressMetadata(internalRecord, drKv("type", "home"), drKv("confidence", 0.9));
  }

  @Test
  public void parse_emailNoMetadata_phoneNoMetadata_addressMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(
                ImmutableList.of(
                    createEmailMatchKey(USER_EMAIL),
                    createPhoneMatchKey(USER_PHONE),
                    createDefaultAddressMatchKey(kv("type", "home"), kv("confidence", 0.9))))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    assertNoEmailMetadata(internalRecord);
    assertNoPhoneMetadata(internalRecord);
    assertAddressMetadata(internalRecord, drKv("type", "home"), drKv("confidence", 0.9));
  }

  @Test
  public void parse_emailMetadata_phoneMetadata_addressNoMetadata() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(
                ImmutableList.of(
                    createEmailMatchKey(USER_EMAIL, kv("source", "web"), kv("confidence", 0.95)),
                    createPhoneMatchKey(USER_PHONE, kv("type", "mobile")),
                    createDefaultAddressMatchKey()))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.hasFieldLevelMetadata()).isTrue();
    assertEmailMetadata(internalRecord, drKv("source", "web"), drKv("confidence", 0.95));
    assertPhoneMetadata(internalRecord, drKv("type", "mobile"));
    assertNoAddressMetadata(internalRecord);
  }

  @Test
  public void parse_encryptedMultipleSameMatchKeyType() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    MatchKey email1 =
        createEmailMatchKey("user1@example.com", kv("source", "web"), kv("confidence", 0.95));
    MatchKey email2 =
        createEmailMatchKey("user2@example.com", kv("domain", "example.com"), kv("score", 0.80));
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(ImmutableList.of(email1, email2))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(2);
    DataRecord record1 = resultList.get(0);
    DataRecord record2 = resultList.get(1);
    // validate coordinator key id
    assertThat(record1.getKeyValues(COORDINATOR_KEY_ID_INDEX))
        .isEqualTo(drKv(COORDINATOR_KEY_ID_KEY, TEST_KEY_ID));
    assertThat(record2.getKeyValues(COORDINATOR_KEY_ID_INDEX))
        .isEqualTo(drKv(COORDINATOR_KEY_ID_KEY, TEST_KEY_ID));
    assertThat(record1.getKeyValues(ROW_MARKER_INDEX).getStringValue())
        .isEqualTo(record2.getKeyValues(ROW_MARKER_INDEX).getStringValue());
    // validate metadata and email metadata for record 1
    assertThat(record1.getKeyValues(METADATA_INDEX))
        .isEqualTo(drKv(METADATA_KEY, FAKE_METADATA_VALUE));
    assertThat(record1.getKeyValues(EMAIL_INDEX)).isEqualTo(drKv(EMAIL_KEY, "user1@example.com"));
    assertEmailMetadata(record1, drKv("source", "web"), drKv("confidence", 0.95));
    // validate metadata and email metadata for record 2
    assertThat(record2.getKeyValues(METADATA_INDEX).hasStringValue()).isFalse();
    assertThat(record2.getKeyValues(EMAIL_INDEX)).isEqualTo(drKv(EMAIL_KEY, "user2@example.com"));
    assertEmailMetadata(record2, drKv("domain", "example.com"), drKv("score", 0.80));
  }

  @Test
  public void parse_encryptedSameValuesInMultipleMatchKeys() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    MatchKey email1 =
        createEmailMatchKey("user1@example.com", kv("source", "web"), kv("confidence", 0.95));
    MatchKey email2 =
        createEmailMatchKey(
            "user1@example.com", // duplicate email
            kv("domain", "example.com"),
            kv("score", 0.80));
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(ImmutableList.of(email1, email2))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(2);
    DataRecord record1 = resultList.get(0);
    DataRecord record2 = resultList.get(1);
    // validate coordinator key id
    assertThat(record1.getKeyValues(COORDINATOR_KEY_ID_INDEX))
        .isEqualTo(drKv(COORDINATOR_KEY_ID_KEY, TEST_KEY_ID));
    assertThat(record2.getKeyValues(COORDINATOR_KEY_ID_INDEX))
        .isEqualTo(drKv(COORDINATOR_KEY_ID_KEY, TEST_KEY_ID));
    assertThat(record1.getKeyValues(ROW_MARKER_INDEX).getStringValue())
        .isEqualTo(record2.getKeyValues(ROW_MARKER_INDEX).getStringValue());
    // validate metadata and email metadata for record 1
    assertThat(record1.getKeyValues(METADATA_INDEX))
        .isEqualTo(drKv(METADATA_KEY, FAKE_METADATA_VALUE));
    assertThat(record1.getKeyValues(EMAIL_INDEX)).isEqualTo(drKv(EMAIL_KEY, "user1@example.com"));
    assertEmailMetadata(record1, drKv("source", "web"), drKv("confidence", 0.95));
    // validate metadata and email metadata for record 2
    assertThat(record2.getKeyValues(METADATA_INDEX).hasStringValue()).isFalse();
    assertThat(record2.getKeyValues(EMAIL_INDEX)).isEqualTo(drKv(EMAIL_KEY, "user1@example.com"));
    assertEmailMetadata(record2, drKv("domain", "example.com"), drKv("score", 0.80));
  }

  @Test
  public void parse_encryptedWrappedEncryptionKey() throws Exception {
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_wrapped_encrypted.json", TEST_GCP_ENCRYPTION_METADATA);

    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();

    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).hasErrorCode()).isFalse();
    assertEquals("metadata", resultList.get(0).getKeyValues(0).getKey());
    assertEquals("fake metadata", resultList.get(0).getKeyValues(0).getStringValue());
    List<String> rowIds = new ArrayList<>();
    for (int i = 0; i < resultList.size(); i++) {
      DataRecord dataRecord = resultList.get(i);
      // encryption key 123 data record
      assertEquals("encrypted_dek", dataRecord.getKeyValues(1).getKey());
      assertEquals("123", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("kek_uri", dataRecord.getKeyValues(2).getKey());
      assertEquals("fake.com", dataRecord.getKeyValues(2).getStringValue());
      assertEquals("email", dataRecord.getKeyValues(3).getKey());
      assertEquals("user@example.com", dataRecord.getKeyValues(3).getStringValue());
      rowIds.add(dataRecord.getKeyValues(8).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_encryptedWrappedEncryptionKeyWithWip() throws Exception {
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_wrapped_with_wip_encrypted.json",
            TEST_GCP_ENCRYPTION_METADATA);

    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .setWip("fakewip")
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();

    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).hasErrorCode()).isFalse();
    assertEquals("metadata", resultList.get(0).getKeyValues(0).getKey());
    assertEquals("fake metadata", resultList.get(0).getKeyValues(0).getStringValue());
    List<String> rowIds = new ArrayList<>();
    for (int i = 0; i < resultList.size(); i++) {
      DataRecord dataRecord = resultList.get(i);
      // encryption key 123 data record
      assertEquals("encrypted_dek", dataRecord.getKeyValues(1).getKey());
      assertEquals("123", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("kek_uri", dataRecord.getKeyValues(2).getKey());
      assertEquals("fake.com", dataRecord.getKeyValues(2).getStringValue());
      assertEquals("wip_provider", dataRecord.getKeyValues(3).getKey());
      assertEquals("fakewip", dataRecord.getKeyValues(3).getStringValue());
      assertEquals("email", dataRecord.getKeyValues(4).getKey());
      assertEquals("user@example.com", dataRecord.getKeyValues(4).getStringValue());
      rowIds.add(dataRecord.getKeyValues(8).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_encryptedWrappedEncryptionKeyWithAwsRoleArn() throws Exception {
    EncryptionMetadata awsEncryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder()
                            .setAwsWrappedKeyInfo(AwsWrappedKeyInfo.newBuilder())))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_wrapped_with_aws_role_arn_encrypted.json",
            awsEncryptionMetadata);
    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setAwsWrappedKey(
                        AwsWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .setRoleArn("testRole")
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder().addMatchKeys(email).build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).hasErrorCode()).isFalse();
    List<String> rowIds = new ArrayList<>();
    for (int i = 0; i < resultList.size(); i++) {
      DataRecord dataRecord = resultList.get(i);
      // encryption key 123 data record
      assertEquals("encrypted_dek", dataRecord.getKeyValues(0).getKey());
      assertEquals("123", dataRecord.getKeyValues(0).getStringValue());
      assertEquals("kek_uri", dataRecord.getKeyValues(1).getKey());
      assertEquals("fake.com", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("role_arn", dataRecord.getKeyValues(2).getKey());
      assertEquals("testRole", dataRecord.getKeyValues(2).getStringValue());
      assertEquals("email", dataRecord.getKeyValues(3).getKey());
      assertEquals("user@example.com", dataRecord.getKeyValues(3).getStringValue());
      rowIds.add(dataRecord.getKeyValues(7).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_encryptedWrappedEncryptionKeyWithRowAwsRoleArn() throws Exception {
    EncryptionMetadata awsEncryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder()
                            .setAwsWrappedKeyInfo(AwsWrappedKeyInfo.newBuilder())))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_wrapped_with_aws_role_arn_encrypted.json",
            awsEncryptionMetadata);
    MatchKey email =
        MatchKey.newBuilder()
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMatchKeys(email)
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setAwsWrappedKey(
                        AwsWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .setRoleArn("testRole")
                            .build())
                    .build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).hasErrorCode()).isFalse();
    List<String> rowIds = new ArrayList<>();
    for (int i = 0; i < resultList.size(); i++) {
      DataRecord dataRecord = resultList.get(i);
      // encryption key 123 data record
      assertEquals("encrypted_dek", dataRecord.getKeyValues(0).getKey());
      assertEquals("123", dataRecord.getKeyValues(0).getStringValue());
      assertEquals("kek_uri", dataRecord.getKeyValues(1).getKey());
      assertEquals("fake.com", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("role_arn", dataRecord.getKeyValues(2).getKey());
      assertEquals("testRole", dataRecord.getKeyValues(2).getStringValue());
      assertEquals("email", dataRecord.getKeyValues(3).getKey());
      assertEquals("user@example.com", dataRecord.getKeyValues(3).getStringValue());
      rowIds.add(dataRecord.getKeyValues(7).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_unencrypted() throws Exception {
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserUnencrypted(
            "testdata/mic_proto_schema_unencrypted.json");

    MatchKey email =
        MatchKey.newBuilder()
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();
    List<CompositeChildField> addressFields = new ArrayList<>();
    addressFields.add(
        CompositeChildField.newBuilder()
            .setKeyValue(
                KeyValue.newBuilder().setKey("first_name").setStringValue("fname1").build())
            .build());
    addressFields.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("last_name").setStringValue("lname1").build())
            .build());
    addressFields.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("country_code").setStringValue("usa").build())
            .build());
    addressFields.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("zip_code").setStringValue("12345").build())
            .build());
    CompositeField address =
        CompositeField.newBuilder().setKey("address").addAllChildFields(addressFields).build();
    MatchKey addressKey = MatchKey.newBuilder().setCompositeField(address).build();
    List<MatchKey> matchKeyList = Arrays.asList(email, addressKey);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    DataRecord dataRecord = resultList.get(0);
    assertFalse(dataRecord.hasFieldLevelMetadata()); // No field-level metadata
    assertEquals("metadata", dataRecord.getKeyValues(0).getKey());
    assertEquals("fake metadata", dataRecord.getKeyValues(0).getStringValue());
    assertEquals("email", dataRecord.getKeyValues(1).getKey());
    assertEquals("user@example.com", dataRecord.getKeyValues(1).getStringValue());
    assertEquals("first_name", dataRecord.getKeyValues(3).getKey());
    assertEquals("fname1", dataRecord.getKeyValues(3).getStringValue());
    assertEquals("last_name", dataRecord.getKeyValues(4).getKey());
    assertEquals("lname1", dataRecord.getKeyValues(4).getStringValue());
    assertEquals("country_code", dataRecord.getKeyValues(5).getKey());
    assertEquals("usa", dataRecord.getKeyValues(5).getStringValue());
    assertEquals("zip_code", dataRecord.getKeyValues(6).getKey());
    assertEquals("12345", dataRecord.getKeyValues(6).getStringValue());
  }

  @Test
  public void parse_matchKeyInMetadata() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(CoordinatorKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", encryptionMetadata);

    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(KeyValue.newBuilder().setKey("email").setStringValue("fake_email").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    // verify only one DataRecord
    assertEquals(1, resultList.size());
    // verify KeyValue (column) count
    assertEquals(9, resultList.get(0).getKeyValuesCount());
    assertEquals(PROTO_METADATA_CONTAINING_RESTRICTED_ALIAS, resultList.get(0).getErrorCode());
  }

  @Test
  public void parse_missingMatchKeyData() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(CoordinatorKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", encryptionMetadata);

    List<MatchKey> matchKeyList = new ArrayList<>();
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertEquals(PROTO_MISSING_MATCH_KEYS, resultList.get(0).getErrorCode());
  }

  @Test
  public void parse_matchKeyMissingFieldAndCompositeField() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(CoordinatorKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", encryptionMetadata);

    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
                    .build())
            .addMetadata(kv("source", "test"))
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    // verify only one DataRecord
    assertEquals(1, resultList.size());
    // verify KeyValue (column) count
    assertEquals(9, resultList.get(0).getKeyValuesCount());
    assertEquals(PROTO_MATCH_KEY_MISSING_FIELD, resultList.get(0).getErrorCode());
  }

  @Test
  public void parse_duplicateMetadata() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setCoordinatorKeyInfo(CoordinatorKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", encryptionMetadata);

    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .addMetadata(
                KeyValue.newBuilder()
                    .setKey("metadata")
                    .setStringValue("another fake metadata")
                    .build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertEquals(PROTO_DUPLICATE_METADATA_KEY, resultList.get(0).getErrorCode());
  }

  @Test
  public void parse_nameToAliasMapping() throws Exception {
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_custom_names.json", TEST_GCP_ENCRYPTION_METADATA);

    MatchKey email =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("em")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();

    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).hasErrorCode()).isFalse();
    assertEquals("metadata", resultList.get(0).getKeyValues(0).getKey());
    assertEquals("fake metadata", resultList.get(0).getKeyValues(0).getStringValue());
    List<String> rowIds = new ArrayList<>();
    for (int i = 0; i < resultList.size(); i++) {
      DataRecord dataRecord = resultList.get(i);
      // encryption key 123 data record
      assertEquals("dek", dataRecord.getKeyValues(1).getKey());
      assertEquals("123", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("kek", dataRecord.getKeyValues(2).getKey());
      assertEquals("fake.com", dataRecord.getKeyValues(2).getStringValue());
      assertEquals("em", dataRecord.getKeyValues(3).getKey());
      assertEquals("user@example.com", dataRecord.getKeyValues(3).getStringValue());
      rowIds.add(dataRecord.getKeyValues(9).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_duplicateAliasMapping() throws Exception {
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_duplicate_aliases.json", TEST_GCP_ENCRYPTION_METADATA);

    // Email 1
    MatchKey email1 =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("em1")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();

    // Email 2
    MatchKey email2 =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("em2")
                            .setStringValue("user@example.com")
                            .build())
                    .build())
            .build();

    // Address 1
    List<CompositeChildField> addressFields1 = new ArrayList<>();
    addressFields1.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("fn1").setStringValue("fname1").build())
            .build());
    addressFields1.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("ln1").setStringValue("lname1").build())
            .build());
    addressFields1.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("cc1").setStringValue("usa").build())
            .build());
    addressFields1.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("zc1").setStringValue("12345").build())
            .build());
    CompositeField address1 =
        CompositeField.newBuilder().setKey("address").addAllChildFields(addressFields1).build();
    MatchKey addressKey1 =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setCompositeField(address1)
            .build();

    // Address 2
    List<CompositeChildField> addressFields2 = new ArrayList<>();
    addressFields2.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("fn2").setStringValue("fname2").build())
            .build());
    addressFields2.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("ln2").setStringValue("lname2").build())
            .build());
    addressFields2.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("cc2").setStringValue("ca").build())
            .build());
    addressFields2.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("zc2").setStringValue("54321").build())
            .build());
    CompositeField address2 =
        CompositeField.newBuilder().setKey("address").addAllChildFields(addressFields2).build();
    MatchKey addressKey2 =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setCompositeField(address2)
            .build();

    // Address 3
    List<CompositeChildField> addressFields3 = new ArrayList<>();
    addressFields3.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("fn1").setStringValue("fname3").build())
            .build());
    addressFields3.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("ln1").setStringValue("lname3").build())
            .build());
    addressFields3.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("cc1").setStringValue("usa").build())
            .build());
    addressFields3.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("zc1").setStringValue("00000").build())
            .build());
    CompositeField address3 =
        CompositeField.newBuilder().setKey("address").addAllChildFields(addressFields3).build();
    MatchKey addressKey3 =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setCompositeField(address3)
            .build();

    // Intentionally included out of order from the schema
    List<MatchKey> matchKeyList =
        Arrays.asList(email1, email2, addressKey1, addressKey2, addressKey3);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(2, resultList.size());

    List<String> rowIds = new ArrayList<>();
    // Evaluate keys and any values that will be consistent across both records
    for (DataRecord dataRecord : resultList) {
      assertThat(dataRecord.hasErrorCode()).isFalse();
      assertEquals("metadata", dataRecord.getKeyValues(0).getKey());
      assertEquals("dek", dataRecord.getKeyValues(1).getKey());
      assertEquals("123", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("kek", dataRecord.getKeyValues(2).getKey());
      assertEquals("fake.com", dataRecord.getKeyValues(2).getStringValue());
      assertEquals("em1", dataRecord.getKeyValues(3).getKey());
      assertEquals("em2", dataRecord.getKeyValues(4).getKey());
      assertEquals("ph1", dataRecord.getKeyValues(5).getKey());
      assertEquals("", dataRecord.getKeyValues(5).getStringValue());
      assertEquals("ph2", dataRecord.getKeyValues(6).getKey());
      assertEquals("", dataRecord.getKeyValues(6).getStringValue());
      assertEquals("fn1", dataRecord.getKeyValues(7).getKey());
      assertEquals("ln1", dataRecord.getKeyValues(8).getKey());
      assertEquals("cc1", dataRecord.getKeyValues(9).getKey());
      assertEquals("zc1", dataRecord.getKeyValues(10).getKey());
      assertEquals("fn2", dataRecord.getKeyValues(11).getKey());
      assertEquals("ln2", dataRecord.getKeyValues(12).getKey());
      assertEquals("cc2", dataRecord.getKeyValues(13).getKey());
      assertEquals("zc2", dataRecord.getKeyValues(14).getKey());
      assertEquals(ROW_MARKER_COLUMN_NAME, dataRecord.getKeyValues(15).getKey());
      rowIds.add(dataRecord.getKeyValues(15).getStringValue());
    }

    // Evaluate metadata
    assertEquals("fake metadata", resultList.get(0).getKeyValues(0).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(0).getStringValue());

    // Evaluate emails
    assertEquals("user@example.com", resultList.get(0).getKeyValues(3).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(3).getStringValue());
    assertEquals("user@example.com", resultList.get(0).getKeyValues(4).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(4).getStringValue());

    // Evaluate address group 0
    assertEquals("fname1", resultList.get(0).getKeyValues(7).getStringValue());
    assertEquals("fname3", resultList.get(1).getKeyValues(7).getStringValue());
    assertEquals("lname1", resultList.get(0).getKeyValues(8).getStringValue());
    assertEquals("lname3", resultList.get(1).getKeyValues(8).getStringValue());
    assertEquals("usa", resultList.get(0).getKeyValues(9).getStringValue());
    assertEquals("usa", resultList.get(1).getKeyValues(9).getStringValue());
    assertEquals("12345", resultList.get(0).getKeyValues(10).getStringValue());
    assertEquals("00000", resultList.get(1).getKeyValues(10).getStringValue());

    // Evaluate address group 1
    assertEquals("fname2", resultList.get(0).getKeyValues(11).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(11).getStringValue());
    assertEquals("lname2", resultList.get(0).getKeyValues(12).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(12).getStringValue());
    assertEquals("ca", resultList.get(0).getKeyValues(13).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(13).getStringValue());
    assertEquals("54321", resultList.get(0).getKeyValues(14).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(14).getStringValue());

    // Evaluate row markers
    assertEquals(2, rowIds.size());
    assertEquals(rowIds.get(0), rowIds.get(1));
  }

  @Test
  public void parse_badChildFieldGroupings() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_custom_names.json", encryptionMetadata);

    List<CompositeChildField> addressFields1 = new ArrayList<>();
    addressFields1.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("fn1").setStringValue("fname1").build())
            .build());
    addressFields1.add(
        CompositeChildField.newBuilder()
            .setKeyValue(KeyValue.newBuilder().setKey("fn2").setStringValue("fname2").build())
            .build());
    CompositeField address =
        CompositeField.newBuilder().setKey("address").addAllChildFields(addressFields1).build();
    MatchKey addressKey =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setCompositeField(address)
            .build();

    List<MatchKey> matchKeyList = Arrays.asList(addressKey);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder().addAllMatchKeys(matchKeyList).build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).getErrorCode()).isEqualTo(PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
  }

  @Test
  public void parse_missingChildFields() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_custom_names.json", encryptionMetadata);

    CompositeField address = CompositeField.newBuilder().setKey("address").build();
    MatchKey addressKey =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setCompositeField(address)
            .build();

    List<MatchKey> matchKeyList = Arrays.asList(addressKey);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder().addAllMatchKeys(matchKeyList).build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(cfmRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).getErrorCode()).isEqualTo(PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
  }

  @Test
  public void
      parse_withMissingChildFieldsError_metadataPassthroughFeatureFlagTrueSchemaFlagTrue_SetsRowLevelMetadata()
          throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted_passthrough_metadata_true.json",
            TEST_GCP_ENCRYPTION_METADATA,
            /* passthroughMetadata= */ true);
    CompositeField address = CompositeField.newBuilder().setKey("address").build();
    MatchKey addressKey =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setWrappedKey(
                        GcpWrappedKey.newBuilder()
                            .setEncryptedDek("123")
                            .setKekUri("fake.com")
                            .build())
                    .build())
            .setCompositeField(address)
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(addressKey);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
    assertThat(record.hasRowLevelMetadata()).isTrue();
    assertThat(record.getRowLevelMetadata().getMetadataList())
        .containsExactly(
            drKv(METADATA_KEY, FAKE_METADATA_VALUE), drKv(METADATA_KEY, FAKE_METADATA_VALUE_2));
  }

  @Test
  public void
      parse_withError_unencrypted_metadataPassthroughFeatureFlagTrueSchemaFlagNotSet_doesNotSetRowLevelMetadata()
          throws Exception {
    // proto_passthrough_metadata_enabled flag not set in the schema
    String schemaPath = "testdata/mic_proto_schema_unencrypted.json";
    EncryptionMetadata encryptionMetadata = null;
    boolean passthroughMetadataFlag = true;
    JobResultCode expectedErrorCode = PROTO_MISSING_MATCH_KEYS;
    ConfidentialMatchDataRecordParserImpl parser =
        createParser(schemaPath, encryptionMetadata, passthroughMetadataFlag);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(expectedErrorCode);
    // If row_level_metadata is not expected, its metadata list should be empty.
    assertThat(record.getRowLevelMetadata().getMetadataList()).isEmpty();
  }

  @Test
  public void
      parse_withError_metadataPassthroughFeatureFlagFalseSchemaFlagNotSet_doesNotSetRowLevelMetadata()
          throws Exception {
    // proto_passthrough_metadata_enabled flag not set in the schema
    String schemaPath = "testdata/mic_proto_schema_encrypted.json";
    EncryptionMetadata encryptionMetadata = TEST_GCP_ENCRYPTION_METADATA;
    boolean passthroughMetadataFlag = false;
    JobResultCode expectedErrorCode = PROTO_MISSING_MATCH_KEYS;
    ConfidentialMatchDataRecordParserImpl parser =
        createParser(schemaPath, encryptionMetadata, passthroughMetadataFlag);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(expectedErrorCode);
    // If row_level_metadata is not expected, its metadata list should be empty.
    assertThat(record.getRowLevelMetadata().getMetadataList()).isEmpty();
  }

  @Test
  public void
      parse_withError_metadataPassthroughFeatureFlagTrueSchemaFlagNotSet_doesNotSetRowLevelMetadata()
          throws Exception {
    // proto_passthrough_metadata_enabled flag not set in the schema
    String schemaPath = "testdata/mic_proto_schema_encrypted.json";
    EncryptionMetadata encryptionMetadata = TEST_GCP_ENCRYPTION_METADATA;
    boolean passthroughMetadataFlag = true;
    JobResultCode expectedErrorCode = PROTO_MISSING_MATCH_KEYS;
    ConfidentialMatchDataRecordParserImpl parser =
        createParser(schemaPath, encryptionMetadata, passthroughMetadataFlag);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(expectedErrorCode);
    // If row_level_metadata is not expected, its metadata list should be empty.
    assertThat(record.getRowLevelMetadata().getMetadataList()).isEmpty();
  }

  @Test
  public void
      parse_withError_metadataPassthroughFeatureFlagTrueSchemaFlagFalse_doesNotSetRowLevelMetadata()
          throws Exception {
    // proto_passthrough_metadata_enabled flag is false in the schema
    String schemaPath = "testdata/mic_proto_schema_encrypted_passthrough_metadata_false.json";
    EncryptionMetadata encryptionMetadata = TEST_GCP_ENCRYPTION_METADATA;
    boolean passthroughMetadataFlag = true;
    JobResultCode expectedErrorCode = PROTO_MISSING_MATCH_KEYS;
    ConfidentialMatchDataRecordParserImpl parser =
        createParser(schemaPath, encryptionMetadata, passthroughMetadataFlag);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(expectedErrorCode);
    // If row_level_metadata is not expected, its metadata list should be empty.
    assertThat(record.getRowLevelMetadata().getMetadataList()).isEmpty();
  }

  @Test
  public void
      parse_withError_metadataPassthroughFeatureFlagFalseSchemaFlagTrue_doesNotSetRowLevelMetadata()
          throws Exception {
    String schemaPath = "testdata/mic_proto_schema_encrypted_passthrough_metadata_true.json";
    EncryptionMetadata encryptionMetadata = TEST_GCP_ENCRYPTION_METADATA;
    boolean passthroughMetadataFlag = false;
    JobResultCode expectedErrorCode = PROTO_MISSING_MATCH_KEYS;
    ConfidentialMatchDataRecordParserImpl parser =
        createParser(schemaPath, encryptionMetadata, passthroughMetadataFlag);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(expectedErrorCode);
    // If row_level_metadata is not expected, its metadata list should be empty.
    assertThat(record.getRowLevelMetadata().getMetadataList()).isEmpty();
  }

  @Test
  public void
      parse_withError_metadataPassthroughFeatureFlagTrueSchemaFlagTrue_SetsRowLevelMetadata()
          throws Exception {
    String schemaPath = "testdata/mic_proto_schema_encrypted_passthrough_metadata_true.json";
    EncryptionMetadata encryptionMetadata = TEST_GCP_ENCRYPTION_METADATA;
    boolean passthroughMetadataFlag = true;
    JobResultCode expectedErrorCode = PROTO_MISSING_MATCH_KEYS;
    DataRecord.KeyValue[] expectedMetadata =
        new DataRecord.KeyValue[] {
          drKv(METADATA_KEY, FAKE_METADATA_VALUE), drKv(METADATA_KEY, FAKE_METADATA_VALUE_2)
        };
    ConfidentialMatchDataRecordParserImpl parser =
        createParser(schemaPath, encryptionMetadata, passthroughMetadataFlag);
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE))
            .addMetadata(kv(METADATA_KEY, FAKE_METADATA_VALUE_2))
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord record = resultList.get(0);
    assertThat(record.getErrorCode()).isEqualTo(expectedErrorCode);
    assertThat(record.hasRowLevelMetadata()).isTrue();
    assertThat(record.getRowLevelMetadata().getMetadataList())
        .containsExactlyElementsIn(Arrays.asList(expectedMetadata));
  }

  @Test
  public void parse_singleFieldMetadataExceedsLimit_returnsError() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    List<KeyValue> metadataList = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      metadataList.add(kv("key" + i, "value" + i));
    }
    MatchKey emailWithMetadataExceedingLimit =
        createEmailMatchKey(USER_EMAIL, metadataList.toArray(new KeyValue[0]));
    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMatchKeys(emailWithMetadataExceedingLimit)
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.getErrorCode()).isEqualTo(PROTO_METADATA_EXCEEDS_LIMIT);
  }

  @Test
  public void parse_compositeFieldMetadataExceedsLimit_returnsError() throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted.json", TEST_COORDINATOR_ENCRYPTION_METADATA);
    List<KeyValue> metadataList = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      metadataList.add(kv("key" + i, "value" + i));
    }
    MatchKey addressWithMetadataExceedingLimit =
        createAddressMatchKey(DEFAULT_ADDRESS_FIELDS, metadataList.toArray(new KeyValue[0]));

    ConfidentialMatchDataRecord cfmRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addMatchKeys(addressWithMetadataExceedingLimit)
            .build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.getErrorCode()).isEqualTo(PROTO_METADATA_EXCEEDS_LIMIT);
  }

  @Test
  public void parse_rowLevelMetadataExceedsLimit_passthroughEnabled_returnsError()
      throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted_passthrough_metadata_true.json",
            TEST_COORDINATOR_ENCRYPTION_METADATA,
            /* passthroughMetadata= */ true);
    ConfidentialMatchDataRecord.Builder cfmRecordBuilder =
        ConfidentialMatchDataRecord.newBuilder().addMatchKeys(createEmailMatchKey(USER_EMAIL));
    for (int i = 0; i < 101; i++) {
      cfmRecordBuilder.addMetadata(kv("row_key" + i, "row_value" + i));
    }
    ConfidentialMatchDataRecord cfmRecord = cfmRecordBuilder.build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.getErrorCode()).isEqualTo(PROTO_METADATA_EXCEEDS_LIMIT);
  }

  @Test
  public void parse_missingMatchKeys_rowLevelMetadataExceedsLimit_passthroughEnabled_returnsError()
      throws Exception {
    ConfidentialMatchDataRecordParserImpl parser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_encrypted_passthrough_metadata_true.json",
            TEST_COORDINATOR_ENCRYPTION_METADATA,
            /* passthroughMetadata= */ true);
    // No MatchKeys added to trigger PROTO_MISSING_MATCH_KEYS in internalParse
    ConfidentialMatchDataRecord.Builder cfmRecordBuilder = ConfidentialMatchDataRecord.newBuilder();
    for (int i = 0; i < 101; i++) {
      cfmRecordBuilder.addMetadata(kv("row_key" + i, "row_value" + i));
    }
    ConfidentialMatchDataRecord cfmRecord = cfmRecordBuilder.build();

    List<DataRecord> resultList = parser.parse(cfmRecord);

    assertThat(resultList).hasSize(1);
    DataRecord internalRecord = resultList.get(0);
    assertThat(internalRecord.getErrorCode()).isEqualTo(PROTO_MISSING_MATCH_KEYS);
    assertThat(internalRecord.hasRowLevelMetadata()).isFalse();
  }

  private ConfidentialMatchDataRecordParserImpl getConfidentialMatchDataRecordParserUnencrypted(
      String schemaPath) throws IOException {
    return createParser(schemaPath, null);
  }

  private ConfidentialMatchDataRecordParserImpl getConfidentialMatchDataRecordParserEncrypted(
      String schemaPath, EncryptionMetadata encryptionMetadata) throws IOException {
    return createParser(schemaPath, encryptionMetadata, false);
  }

  private ConfidentialMatchDataRecordParserImpl getConfidentialMatchDataRecordParserEncrypted(
      String schemaPath, EncryptionMetadata encryptionMetadata, boolean passthroughMetadata)
      throws IOException {
    return createParser(schemaPath, encryptionMetadata, passthroughMetadata);
  }

  private ConfidentialMatchDataRecordParserImpl createParser(
      String schemaPath, EncryptionMetadata encryptionMetadata) throws IOException {
    return createParser(schemaPath, encryptionMetadata, false);
  }

  private ConfidentialMatchDataRecordParserImpl createParser(
      String schemaPath, EncryptionMetadata encryptionMetadata, boolean passthroughMetadata)
      throws IOException {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(Objects.requireNonNull(getClass().getResource(schemaPath)), UTF_8),
            Schema.class);
    schema =
        schema.toBuilder()
            .addColumns(
                Column.newBuilder()
                    .setColumnType(ColumnType.STRING)
                    .setColumnName(ROW_MARKER_COLUMN_NAME)
                    .setColumnAlias(ROW_MARKER_COLUMN_NAME)
                    .build())
            .build();
    if (encryptionMetadata == null) {
      return new ConfidentialMatchDataRecordParserImpl(
          MIC_MATCH_CONFIG,
          schema,
          SuccessMode.ALLOW_PARTIAL_SUCCESS,
          FeatureFlags.builder().setProtoPassthroughMetadataEnabled(passthroughMetadata).build());
    } else {
      return new ConfidentialMatchDataRecordParserImpl(
          MIC_MATCH_CONFIG,
          schema,
          SuccessMode.ALLOW_PARTIAL_SUCCESS,
          encryptionMetadata,
          FeatureFlags.builder().setProtoPassthroughMetadataEnabled(passthroughMetadata).build());
    }
  }

  private EncryptionKey coordKey(String keyId) {
    return EncryptionKey.newBuilder()
        .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId(keyId).build())
        .build();
  }

  // Helper Methods for creating KeyValue objects
  private static KeyValue kv(String key, String value) {
    return KeyValue.newBuilder().setKey(key).setStringValue(value).build();
  }

  private static KeyValue kv(String key, double value) {
    return KeyValue.newBuilder().setKey(key).setDoubleValue(value).build();
  }

  private static DataRecord.KeyValue drKv(String key, String value) {
    return DataRecord.KeyValue.newBuilder().setKey(key).setStringValue(value).build();
  }

  private static DataRecord.KeyValue drKv(String key, double value) {
    return DataRecord.KeyValue.newBuilder().setKey(key).setDoubleValue(value).build();
  }

  // Helper Methods for creating MatchKey objects
  private static MatchKey createEmailMatchKey(String value, KeyValue... metadataKeyValues) {
    return MatchKey.newBuilder()
        .setEncryptionKey(DEFAULT_COORDINATOR_KEY)
        .setField(
            Field.newBuilder()
                .setKeyValue(KeyValue.newBuilder().setKey(EMAIL_KEY).setStringValue(value).build())
                .build())
        .addAllMetadata(Arrays.asList(metadataKeyValues))
        .build();
  }

  private static MatchKey createPhoneMatchKey(String value, KeyValue... metadataKeyValues) {
    return MatchKey.newBuilder()
        .setEncryptionKey(DEFAULT_COORDINATOR_KEY)
        .setField(
            Field.newBuilder()
                .setKeyValue(KeyValue.newBuilder().setKey(PHONE_KEY).setStringValue(value).build())
                .build())
        .addAllMetadata(Arrays.asList(metadataKeyValues))
        .build();
  }

  private static MatchKey createAddressMatchKey(
      ImmutableList<CompositeChildField> childFields, KeyValue... metadataKeyValues) {
    return MatchKey.newBuilder()
        .setEncryptionKey(DEFAULT_COORDINATOR_KEY)
        .setCompositeField(
            CompositeField.newBuilder().setKey(ADDRESS_KEY).addAllChildFields(childFields).build())
        .addAllMetadata(Arrays.asList(metadataKeyValues))
        .build();
  }

  private static CompositeChildField createCompositeChildField(String key, String value) {
    return CompositeChildField.newBuilder().setKeyValue(kv(key, value)).build();
  }

  private static MatchKey createDefaultAddressMatchKey(KeyValue... metadataKeyValues) {
    return createAddressMatchKey(DEFAULT_ADDRESS_FIELDS, metadataKeyValues);
  }

  private void assertDataRecordKeyValue(DataRecord record, int index, String key, String value) {
    assertThat(record.getKeyValues(index)).isEqualTo(drKv(key, value));
  }

  private void assertEmailMetadata(
      DataRecord internalRecord, DataRecord.KeyValue... expectedMetadata) {
    assertThat(internalRecord.getFieldLevelMetadata().containsSingleFieldMetadata(EMAIL_INDEX))
        .isTrue();
    assertThat(
            internalRecord
                .getFieldLevelMetadata()
                .getSingleFieldMetadataOrThrow(EMAIL_INDEX)
                .getMetadataList())
        .containsExactlyElementsIn(expectedMetadata);
  }

  private void assertNoEmailMetadata(DataRecord internalRecord) {
    assertThat(internalRecord.getFieldLevelMetadata().containsSingleFieldMetadata(EMAIL_INDEX))
        .isFalse();
  }

  private void assertPhoneMetadata(
      DataRecord internalRecord, DataRecord.KeyValue... expectedMetadata) {
    assertThat(internalRecord.getFieldLevelMetadata().containsSingleFieldMetadata(PHONE_INDEX))
        .isTrue();
    assertThat(
            internalRecord
                .getFieldLevelMetadata()
                .getSingleFieldMetadataOrThrow(PHONE_INDEX)
                .getMetadataList())
        .containsExactlyElementsIn(expectedMetadata);
  }

  private void assertNoPhoneMetadata(DataRecord internalRecord) {
    assertThat(internalRecord.getFieldLevelMetadata().containsSingleFieldMetadata(PHONE_INDEX))
        .isFalse();
  }

  private void assertAddressMetadata(
      DataRecord internalRecord, DataRecord.KeyValue... expectedMetadata) {
    assertThat(internalRecord.getFieldLevelMetadata().containsCompositeFieldMetadata(ADDRESS_GROUP))
        .isTrue();
    assertThat(
            internalRecord
                .getFieldLevelMetadata()
                .getCompositeFieldMetadataOrThrow(ADDRESS_GROUP)
                .getMetadataList())
        .containsExactlyElementsIn(expectedMetadata);
  }

  private void assertNoAddressMetadata(DataRecord internalRecord) {
    assertThat(internalRecord.getFieldLevelMetadata().containsCompositeFieldMetadata(ADDRESS_GROUP))
        .isFalse();
  }
}
