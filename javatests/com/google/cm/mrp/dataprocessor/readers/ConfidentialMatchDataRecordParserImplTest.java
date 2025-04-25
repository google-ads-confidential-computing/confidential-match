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
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey;
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
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ConfidentialMatchDataRecordParserImplTest {

  private static final MatchConfig micMatchConfig = getMatchConfig("mic");

  @Test
  public void parse_encryptedMultipleEncryptionKey() throws Exception {
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
                            .setStringValue("fakeemail@fake.com")
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
    MatchKey addressKey =
        MatchKey.newBuilder()
            .setEncryptionKey(
                EncryptionKey.newBuilder()
                    .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("124").build())
                    .build())
            .setCompositeField(address)
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email, addressKey);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(2, resultList.size());
    List<String> rowIds = new ArrayList<>();
    for (DataRecord dataRecord : resultList) {
      rowIds.add(dataRecord.getKeyValues(8).getStringValue());
      // encryption key 123 data record
      if (dataRecord.getKeyValues(1).getStringValue().equals("123")) {
        assertEquals("metadata", resultList.get(0).getKeyValues(0).getKey());
        assertEquals("fake metadata", resultList.get(0).getKeyValues(0).getStringValue());
        assertEquals("coordinator_key_id", dataRecord.getKeyValues(1).getKey());
        assertEquals("123", dataRecord.getKeyValues(1).getStringValue());
        assertEquals("email", dataRecord.getKeyValues(2).getKey());
        assertEquals("fakeemail@fake.com", dataRecord.getKeyValues(2).getStringValue());
      } else if (dataRecord.getKeyValues(1).getStringValue().equals("124")) {
        assertEquals("metadata", resultList.get(1).getKeyValues(0).getKey());
        assertFalse(resultList.get(1).getKeyValues(0).hasStringValue());
        assertEquals("coordinator_key_id", dataRecord.getKeyValues(1).getKey());
        assertEquals("124", dataRecord.getKeyValues(1).getStringValue());
        assertEquals("first_name", dataRecord.getKeyValues(4).getKey());
        assertEquals("fname1", dataRecord.getKeyValues(4).getStringValue());
        assertEquals("last_name", dataRecord.getKeyValues(5).getKey());
        assertEquals("lname1", dataRecord.getKeyValues(5).getStringValue());
        assertEquals("country_code", dataRecord.getKeyValues(6).getKey());
        assertEquals("usa", dataRecord.getKeyValues(6).getStringValue());
        assertEquals("zip_code", dataRecord.getKeyValues(7).getKey());
        assertEquals("12345", dataRecord.getKeyValues(7).getStringValue());
      }
    }
    assertEquals(2, rowIds.size());
    assertEquals(rowIds.get(0), rowIds.get(1));
  }

  @Test
  public void parse_encryptedMultipleSameMatchKeyType() throws Exception {
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
                            .setStringValue("fakeemail@fake.com")
                            .build())
                    .build())
            .build();
    MatchKey email2 =
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
                            .setStringValue("fakeemail2@fake.com")
                            .build())
                    .build())
            .build();

    List<MatchKey> matchKeyList = Arrays.asList(email, email2);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(2, resultList.size());
    List<String> rowIds = new ArrayList<>();
    for (int i = 0; i < resultList.size(); i++) {
      DataRecord dataRecord = resultList.get(i);
      // encryption key 123 data record
      assertEquals("coordinator_key_id", dataRecord.getKeyValues(1).getKey());
      assertEquals("123", dataRecord.getKeyValues(1).getStringValue());
      assertEquals("email", dataRecord.getKeyValues(2).getKey());
      rowIds.add(dataRecord.getKeyValues(8).getStringValue());

      if (i == 0) {
        assertEquals("metadata", dataRecord.getKeyValues(0).getKey());
        assertEquals("fake metadata", dataRecord.getKeyValues(0).getStringValue());
        assertEquals("fakeemail@fake.com", dataRecord.getKeyValues(2).getStringValue());
      } else if (i == 1) {
        assertEquals("metadata", dataRecord.getKeyValues(0).getKey());
        assertFalse(dataRecord.getKeyValues(0).hasStringValue());
        assertEquals("fakeemail2@fake.com", dataRecord.getKeyValues(2).getStringValue());
      }
    }
    assertEquals(2, rowIds.size());
    assertEquals(rowIds.get(0), rowIds.get(1));
  }

  @Test
  public void parse_encryptedWrappedEncryptionKey() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_wrapped_encrypted.json", encryptionMetadata);

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
                            .setStringValue("fakeemail@fake.com")
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

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
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
      assertEquals("fakeemail@fake.com", dataRecord.getKeyValues(3).getStringValue());
      rowIds.add(dataRecord.getKeyValues(8).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_encryptedWrappedEncryptionKeyWithWip() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_wrapped_with_wip_encrypted.json", encryptionMetadata);

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
                            .setStringValue("fakeemail@fake.com")
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

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
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
      assertEquals("fakeemail@fake.com", dataRecord.getKeyValues(4).getStringValue());
      rowIds.add(dataRecord.getKeyValues(8).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_unencrypted() throws Exception {
    Schema schema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/mic_proto_schema_unencrypted.json")),
                UTF_8),
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
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        new ConfidentialMatchDataRecordParserImpl(
            micMatchConfig, schema, SuccessMode.ALLOW_PARTIAL_SUCCESS);

    MatchKey email =
        MatchKey.newBuilder()
            .setField(
                Field.newBuilder()
                    .setKeyValue(
                        KeyValue.newBuilder()
                            .setKey("email")
                            .setStringValue("fakeemail@fake.com")
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
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
    DataRecord dataRecord = resultList.get(0);
    assertEquals("metadata", dataRecord.getKeyValues(0).getKey());
    assertEquals("fake metadata", dataRecord.getKeyValues(0).getStringValue());
    assertEquals("email", dataRecord.getKeyValues(1).getKey());
    assertEquals("fakeemail@fake.com", dataRecord.getKeyValues(1).getStringValue());
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
                            .setStringValue("fakeemail@fake.com")
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(KeyValue.newBuilder().setKey("email").setStringValue("fake_email").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

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
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
    assertEquals(PROTO_MISSING_MATCH_KEYS, resultList.get(0).getErrorCode());
  }

  @Test
  public void parse_matchKeyMissingField() throws Exception {
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
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

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
                            .setStringValue("fakeemail@fake.com")
                            .build())
                    .build())
            .build();
    List<MatchKey> matchKeyList = Arrays.asList(email);
    ConfidentialMatchDataRecord testRecord =
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

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
    assertEquals(PROTO_DUPLICATE_METADATA_KEY, resultList.get(0).getErrorCode());
  }

  @Test
  public void parse_nameToAliasMapping() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_custom_names.json", encryptionMetadata);

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
                            .setStringValue("fakeemail@fake.com")
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

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
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
      assertEquals("fakeemail@fake.com", dataRecord.getKeyValues(3).getStringValue());
      rowIds.add(dataRecord.getKeyValues(9).getStringValue());
    }
    assertEquals(1, rowIds.size());
  }

  @Test
  public void parse_duplicateAliasMapping() throws Exception {
    EncryptionMetadata encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().build()))
            .build();
    ConfidentialMatchDataRecordParserImpl confidentialMatchDataRecordParser =
        getConfidentialMatchDataRecordParserEncrypted(
            "testdata/mic_proto_schema_duplicate_aliases.json", encryptionMetadata);

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
                            .setStringValue("fakeemail1@fake.com")
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
                            .setStringValue("fakeemail2@fake.com")
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
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder()
            .addAllMatchKeys(matchKeyList)
            .addMetadata(
                KeyValue.newBuilder().setKey("metadata").setStringValue("fake metadata").build())
            .build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(2, resultList.size());

    List<String> rowIds = new ArrayList<>();
    // Evaluate keys and any values that will be consistent across both records
    for (DataRecord dataRecord : resultList) {
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
    assertEquals("fakeemail1@fake.com", resultList.get(0).getKeyValues(3).getStringValue());
    assertEquals("", resultList.get(1).getKeyValues(3).getStringValue());
    assertEquals("fakeemail2@fake.com", resultList.get(0).getKeyValues(4).getStringValue());
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
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder().addAllMatchKeys(matchKeyList).build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

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
    ConfidentialMatchDataRecord testRecord =
        ConfidentialMatchDataRecord.newBuilder().addAllMatchKeys(matchKeyList).build();

    List<DataRecord> resultList = confidentialMatchDataRecordParser.parse(testRecord);

    assertEquals(1, resultList.size());
    assertThat(resultList.get(0).getErrorCode()).isEqualTo(PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
  }

  private ConfidentialMatchDataRecordParserImpl getConfidentialMatchDataRecordParserEncrypted(
      String schemaPath, EncryptionMetadata encryptionMetadata) throws IOException {
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
    return new ConfidentialMatchDataRecordParserImpl(
        micMatchConfig, schema, SuccessMode.ALLOW_PARTIAL_SUCCESS, encryptionMetadata);
  }
}
