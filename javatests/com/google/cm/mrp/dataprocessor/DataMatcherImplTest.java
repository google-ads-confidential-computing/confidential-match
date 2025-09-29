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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.common.hash.Hashing.sha256;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.MatchConfigProvider;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.MatchedOutputField;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.MatchedOutputField.Field;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.ModeConfigs;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.ModeConfigs.RedactModeConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.ModeProto.Mode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.DataMatchResult;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerFactory;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerImpl;
import com.google.cm.mrp.models.JobParameters;
import com.google.cm.mrp.models.JobParameters.OutputDataLocation;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class DataMatcherImplTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String REDACT_UNMATCHED_WITH = "UNMATCHED";
  private static final List<String> DEFAULT_FIELDS =
      ImmutableList.of("email", "phone", "first_name", "last_name", "zip_code", "country_code");
  private static final FieldParser DEFAULT_FIELD_PARSER = new FieldParser(DEFAULT_FIELDS);
  private static final Schema DEFAULT_SCHEMA = getDefaultSchemaFromDefaultFields();
  private static final JobParameters DEFAULT_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .build();
  private static final JobParameters JOIN_MODE_PARAMS =
      JobParameters.builder()
          .setMode(Mode.JOIN)
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .build();
  private DataMatcher dataMatcher;
  private DataMatcher dataMatcherWithPartialSuccess;
  private DataMatcher dataMatcherWithJoinMode;

  @Mock DataRecordTransformerFactory dataRecordTransformerFactory;

  @Before
  public void setUp() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("customer_match"),
            DEFAULT_PARAMS);
    dataMatcherWithPartialSuccess =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("copla"),
            DEFAULT_PARAMS);
    dataMatcherWithJoinMode =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("mic"),
            JOIN_MODE_PARAMS);
    when(dataRecordTransformerFactory.create(any(), any(), any()))
        .thenReturn(
            new DataRecordTransformerImpl(
                MatchConfig.getDefaultInstance(), Schema.getDefaultInstance(), DEFAULT_PARAMS));
  }

  @Test
  public void match_whenMatchFoundThenKeeps() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("email", result.records().get(1).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(1).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(1).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(1).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(1).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(1).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(1).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(1).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(1).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(1).getKeyValues(5).getStringValue());
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("email"));
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
  }

  @Test
  public void match_whenOnlyEmailMatchFoundThenKeepsEmail() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "888-888-8888"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS88888"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(1, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(1, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));

    // Check that row status column was not appended for complete success jobs.
    Schema expectedSchema = getSchema(testData);
    assertEquals(result.schema(), expectedSchema);
  }

  @Test
  public void match_whenOnlyPhoneNumberMatchFoundThenKeepsPhoneNumber() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "different.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS88888"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(1, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(1, conditionMatchCounts.size());
    assertEquals(0, datasource1Errors.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
  }

  @Test
  public void match_whenOnlyAddressMatchFoundThenKeepsAddress() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "different.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "888-888-8888"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(1, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(1, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenDifferentFieldsHaveSameHashThenDoesNotMatch() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfig.newBuilder()
                .mergeFrom(MatchConfigProvider.getMatchConfig("customer_match"))
                .setMatchConditions(
                    0,
                    MatchConfig.MatchCondition.newBuilder()
                        .setOperator(MatchConfig.MatchOperator.EQUALS)
                        .setDataSource1Column(
                            MatchConfig.CompositeColumn.newBuilder()
                                .setColumnAlias("email")
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(0)
                                        .setColumnAlias("email")))
                        .setDataSource2Column(
                            MatchConfig.CompositeColumn.newBuilder()
                                .setColumnAlias("email")
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(0)
                                        .setColumnAlias("email"))))
                .setMatchConditions(
                    1,
                    MatchConfig.MatchCondition.newBuilder()
                        .setOperator(MatchConfig.MatchOperator.EQUALS)
                        .setDataSource1Column(
                            MatchConfig.CompositeColumn.newBuilder()
                                .setColumnAlias("phone")
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(0)
                                        .setColumnAlias("phone")))
                        .setDataSource2Column(
                            MatchConfig.CompositeColumn.newBuilder()
                                .setColumnAlias("phone")
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(0)
                                        .setColumnAlias("phone"))))
                .build(),
            DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", "email_hash"},
      {"phone", "phone", "phone_hash"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiData = {
      {"email", "email", "phone_hash"},
      {"phone", "phone", "email_hash"},
      {"pii_value", "pii_value", ""}
    };
    String[][] piiDataAddress = {
      {"email", "email", ""},
      {"phone", "phone", ""},
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiData))
            .addRecord(getDataRecord(piiData))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(1, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(1, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenMatchNotFoundThenRedactsAll() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    DataChunk dataChunk2 = DataChunk.builder().setSchema(Schema.getDefaultInstance()).build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals(REDACT_UNMATCHED_WITH, result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(0, conditionMatchCounts.size());
    assertEquals(0, datasource1Errors.size());
    assertEquals(0, datasource2ConditionMatchCounts.size());
  }

  @Test
  public void match_whenMatchNotFoundAndReductionWithEmptyStringThenRemovesTheRow() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfig.newBuilder()
                .mergeFrom(MatchConfigProvider.getMatchConfig("customer_match"))
                .setModeConfigs(
                    ModeConfigs.newBuilder()
                        .setRedactModeConfig(
                            RedactModeConfig.newBuilder().setRedactUnmatchedWith("")))
                .build(),
            DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    DataChunk dataChunk2 = DataChunk.builder().setSchema(Schema.getDefaultInstance()).build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertTrue(result.records().isEmpty());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(0, conditionMatchCounts.size());
    assertEquals(0, datasource1Errors.size());
    assertEquals(0, datasource2ConditionMatchCounts.size());
  }

  @Test
  public void match_whenBlankEmailThenBlankResult() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("customer_match"),
            DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", ""},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataPhone))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(0), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(2, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(2, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenBlankPhoneThenBlankResult() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("customer_match"),
            DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", ""},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(0), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(2, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(2, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenBlankAddressThenBlankResult() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("customer_match"),
            DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", ""},
      {"last_name", "last_name", ""},
      {"zip_code", "zip_code", ""},
      {"country_code", "country_code", ""},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(0), conditionValidCounts.get("address"));
    assertEquals(2, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(2, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
  }

  @Test
  public void match_someAddressColumnsEmpty_matches() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfigProvider.getMatchConfig("customer_match"),
            DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "first"},
      {"last_name", "last_name", ""},
      {"zip_code", "zip_code", ""},
      {"country_code", "country_code", "country"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {{"pii_value", "pii_value", "firstcountry"}};
    DataChunk dataChunk2 = buildDataChunk(piiDataEmail, piiDataPhone, piiDataAddress);

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertEquals(1, result.records().size());
    assertEquals(6, result.records().get(0).getKeyValuesCount());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("first", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("country", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenAllEmptyFieldsThenRemovesTheRow() {
    String[][] testData = {
      {"email", "email", ""},
      {"phone", "phone", ""},
      {"first_name", "first_name", ""},
      {"last_name", "last_name", ""},
      {"zip_code", "zip_code", ""},
      {"country_code", "country_code", ""},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    DataChunk dataChunk2 = DataChunk.builder().setSchema(Schema.getDefaultInstance()).build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertTrue(result.records().isEmpty());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(0), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(0), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(0), conditionValidCounts.get("address"));
    assertEquals(0, conditionMatchCounts.size());
    assertEquals(0, datasource1Errors.size());
    assertEquals(0, datasource2ConditionMatchCounts.size());
  }

  @Test
  public void match_whenMatchFoundThenKeepsMultiColumn() {
    String[][] testData = {
      {"Email1", "email", "fake.email1@google.com"},
      {"Email2", "email", "fake.email2@google.com"},
      {"Phone1", "phone", "999-999-9999"},
      {"Phone2", "phone", "888-888-8888"},
      {"FirstName1", "first_name", "fake_first_name1", "0"},
      {"FirstName2", "first_name", "fake_first_name2", "1"},
      {"LastName1", "last_name", "fake_last_name1", "0"},
      {"LastName2", "last_name", "fake_last_name2", "1"},
      {"ZipCode1", "zip_code", "99999", "0"},
      {"ZipCode2", "zip_code", "88888", "1"},
      {"CountryCode1", "country_code", "US", "0"},
      {"CountryCode2", "country_code", "CA", "1"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "fake.email1@google.com"},
      {"pii_value", "pii_value", "fake.email2@google.com"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "999-999-9999"},
      {"pii_value", "pii_value", "888-888-8888"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_name1fake_last_name1US99999"},
      {"pii_value", "pii_value", "fake_first_name2fake_last_name2CA88888"}
    };
    DataChunk dataChunk2 = buildDataChunk(piiDataEmail, piiDataPhone, piiDataAddress);

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("Email1", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("email", result.schema().getColumns(0).getColumnAlias());
    assertEquals(
        "fake.email1@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("Email2", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("email", result.schema().getColumns(1).getColumnAlias());
    assertEquals(
        "fake.email2@google.com", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("Phone1", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("phone", result.schema().getColumns(2).getColumnAlias());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("Phone2", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("phone", result.schema().getColumns(3).getColumnAlias());
    assertEquals("888-888-8888", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("FirstName1", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("fake_first_name1", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("FirstName2", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("fake_first_name2", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("LastName1", result.records().get(0).getKeyValues(6).getKey());
    assertEquals("fake_last_name1", result.records().get(0).getKeyValues(6).getStringValue());
    assertEquals("LastName2", result.records().get(0).getKeyValues(7).getKey());
    assertEquals("fake_last_name2", result.records().get(0).getKeyValues(7).getStringValue());
    assertEquals("ZipCode1", result.records().get(0).getKeyValues(8).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(8).getStringValue());
    assertEquals("ZipCode2", result.records().get(0).getKeyValues(9).getKey());
    assertEquals("88888", result.records().get(0).getKeyValues(9).getStringValue());
    assertEquals("CountryCode1", result.records().get(0).getKeyValues(10).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(10).getStringValue());
    assertEquals("CountryCode2", result.records().get(0).getKeyValues(11).getKey());
    assertEquals("CA", result.records().get(0).getKeyValues(11).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(2), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(2), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(2), datasource2ConditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(2), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenPartialMatchFoundThenKeepsColumnsThatMatch() {
    String[][] testData = {
      {"Email1", "email", "fake.email1@google.com"},
      {"Email2", "email", "fake.email2@google.com"},
      {"Phone1", "phone", "999-999-9999"},
      {"Phone2", "phone", "888-888-8888"},
      {"FirstName1", "first_name", "fake_first_name1", "0"},
      {"FirstName2", "first_name", "fake_first_name2", "1"},
      {"LastName1", "last_name", "fake_last_name1", "0"},
      {"LastName2", "last_name", "fake_last_name2", "1"},
      {"ZipCode1", "zip_code", "99999", "0"},
      {"ZipCode2", "zip_code", "88888", "1"},
      {"CountryCode1", "country_code", "US", "0"},
      {"CountryCode2", "country_code", "CA", "1"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "fake.email1@google.com"},
      {"pii_value", "pii_value", "fake.email3@google.com"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "999-999-9999"},
      {"pii_value", "pii_value", "777-777-7777"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_name1fake_last_name1US99999"},
      {"pii_value", "pii_value", "fake_first_name3fake_last_name3MX77777"}
    };
    DataChunk dataChunk2 = buildDataChunk(piiDataEmail, piiDataPhone, piiDataAddress);

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("Email1", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("email", result.schema().getColumns(0).getColumnAlias());
    assertEquals(
        "fake.email1@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("Email2", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("email", result.schema().getColumns(1).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("Phone1", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("phone", result.schema().getColumns(2).getColumnAlias());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("Phone2", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("phone", result.schema().getColumns(3).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("FirstName1", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("fake_first_name1", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("FirstName2", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("LastName1", result.records().get(0).getKeyValues(6).getKey());
    assertEquals("fake_last_name1", result.records().get(0).getKeyValues(6).getStringValue());
    assertEquals("LastName2", result.records().get(0).getKeyValues(7).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(7).getStringValue());
    assertEquals("ZipCode1", result.records().get(0).getKeyValues(8).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(8).getStringValue());
    assertEquals("ZipCode2", result.records().get(0).getKeyValues(9).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(9).getStringValue());
    assertEquals("CountryCode1", result.records().get(0).getKeyValues(10).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(10).getStringValue());
    assertEquals("CountryCode2", result.records().get(0).getKeyValues(11).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(11).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(2), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenPartialMatchFoundThenKeepsColumnsThatMatch2() {
    String[][] testData = {
      {"Email1", "email", "fake.email1@google.com"},
      {"Email2", "email", "fake.email2@google.com"},
      {"Phone1", "phone", "999-999-9999"},
      {"Phone2", "phone", "888-888-8888"},
      {"FirstName1", "first_name", "fake_first_name1", "0"},
      {"FirstName2", "first_name", "fake_first_name2", "1"},
      {"LastName1", "last_name", "fake_last_name1", "0"},
      {"LastName2", "last_name", "fake_last_name2", "1"},
      {"ZipCode1", "zip_code", "99999", "0"},
      {"ZipCode2", "zip_code", "88888", "1"},
      {"CountryCode1", "country_code", "US", "0"},
      {"CountryCode2", "country_code", "CA", "1"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "fake.email3@google.com"},
      {"pii_value", "pii_value", "fake.email2@google.com"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "777-777-7777"},
      {"pii_value", "pii_value", "888-888-8888"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_name3fake_last_name3MX77777"},
      {"pii_value", "pii_value", "fake_first_name2fake_last_name2CA88888"}
    };
    DataChunk dataChunk2 = buildDataChunk(piiDataEmail, piiDataPhone, piiDataAddress);

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("Email1", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("email", result.schema().getColumns(0).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("Email2", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("email", result.schema().getColumns(1).getColumnAlias());
    assertEquals(
        "fake.email2@google.com", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("Phone1", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("phone", result.schema().getColumns(2).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("Phone2", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("phone", result.schema().getColumns(3).getColumnAlias());
    assertEquals("888-888-8888", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("FirstName1", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("FirstName2", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("fake_first_name2", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("LastName1", result.records().get(0).getKeyValues(6).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(6).getStringValue());
    assertEquals("LastName2", result.records().get(0).getKeyValues(7).getKey());
    assertEquals("fake_last_name2", result.records().get(0).getKeyValues(7).getStringValue());
    assertEquals("ZipCode1", result.records().get(0).getKeyValues(8).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(8).getStringValue());
    assertEquals("ZipCode2", result.records().get(0).getKeyValues(9).getKey());
    assertEquals("88888", result.records().get(0).getKeyValues(9).getStringValue());
    assertEquals("CountryCode1", result.records().get(0).getKeyValues(10).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(10).getStringValue());
    assertEquals("CountryCode2", result.records().get(0).getKeyValues(11).getKey());
    assertEquals("CA", result.records().get(0).getKeyValues(11).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(2), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_whenNoMatchFoundThenRedactsMultiColumn() {
    String[][] testData = {
      {"Email1", "email", "fake.email1@google.com"},
      {"Email2", "email", "fake.email2@google.com"},
      {"Phone1", "phone", "999-999-9999"},
      {"Phone2", "phone", "888-888-8888"},
      {"FirstName1", "first_name", "fake_first_name1", "0"},
      {"FirstName2", "first_name", "fake_first_name2", "1"},
      {"LastName1", "last_name", "fake_last_name1", "0"},
      {"LastName2", "last_name", "fake_last_name2", "1"},
      {"ZipCode1", "zip_code", "99999", "0"},
      {"ZipCode2", "zip_code", "88888", "1"},
      {"CountryCode1", "country_code", "US", "0"},
      {"CountryCode2", "country_code", "CA", "1"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "fake.email3@google.com"},
      {"pii_value", "pii_value", "fake.email4@google.com"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "777-777-7777"},
      {"pii_value", "pii_value", "666-666-6666"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_name3fake_last_name3MX77777"},
      {"pii_value", "pii_value", "fake_first_name4fake_last_name4ZZ66666"}
    };
    DataChunk dataChunk2 = buildDataChunk(piiDataEmail, piiDataPhone, piiDataAddress);

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("Email1", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("email", result.schema().getColumns(0).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("Email2", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("email", result.schema().getColumns(1).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("Phone1", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("phone", result.schema().getColumns(2).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("Phone2", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("phone", result.schema().getColumns(3).getColumnAlias());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("FirstName1", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("FirstName2", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("LastName1", result.records().get(0).getKeyValues(6).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(6).getStringValue());
    assertEquals("LastName2", result.records().get(0).getKeyValues(7).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(7).getStringValue());
    assertEquals("ZipCode1", result.records().get(0).getKeyValues(8).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(8).getStringValue());
    assertEquals("ZipCode2", result.records().get(0).getKeyValues(9).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(9).getStringValue());
    assertEquals("CountryCode1", result.records().get(0).getKeyValues(10).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(10).getStringValue());
    assertEquals("CountryCode2", result.records().get(0).getKeyValues(11).getKey());
    assertEquals("UNMATCHED", result.records().get(0).getKeyValues(11).getStringValue());
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(2), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("email"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(0, conditionMatchCounts.size());
    assertEquals(0, datasource1Errors.size());
    assertEquals(0, datasource2ConditionMatchCounts.size());
  }

  @Test
  public void match_whenMatchConfigNotSorted() {
    dataMatcher =
        new DataMatcherImpl(
            dataRecordTransformerFactory,
            MatchConfig.newBuilder()
                .mergeFrom(MatchConfigProvider.getMatchConfig("customer_match"))
                .setMatchConditions(
                    0,
                    MatchConfig.MatchCondition.newBuilder()
                        .setOperator(MatchConfig.MatchOperator.EQUALS)
                        .setDataSource1Column(
                            MatchConfig.CompositeColumn.newBuilder()
                                .setColumnAlias("address")
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(2)
                                        .setColumnAlias("country_code"))
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(3)
                                        .setColumnAlias("zip_code"))
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(0)
                                        .setColumnAlias("first_name"))
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(1)
                                        .setColumnAlias("last_name")))
                        .setDataSource2Column(
                            MatchConfig.CompositeColumn.newBuilder()
                                .setColumnAlias("address")
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(2)
                                        .setColumnAlias("zip_code"))
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(0)
                                        .setColumnAlias("full_name"))
                                .addColumns(
                                    MatchConfig.Column.newBuilder()
                                        .setOrder(1)
                                        .setColumnAlias("country_code"))))
                .build(),
            DEFAULT_PARAMS);
    String[][] testData1 = {
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    String[][] testData2 = {
      {"country_code", "country_code", "US"},
      {"zip_code", "zip_code", "99999"},
      {"full_name", "full_name", "fake_first_namefake_last_name"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData1))
            .addRecord(getDataRecord(testData1))
            .build();
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(testData2))
            .addRecord(getDataRecord(testData2))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("first_name", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals(2, conditionValidCounts.size());
    assertEquals(Long.valueOf(0), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(1, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(1, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
  }

  @Test
  public void match_multipleGaiaMatched() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(2), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(3), datasource2ConditionMatchCounts.get("email"));
  }

  @Test
  public void match_multipleGaiaMatched_repeatedValues() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    String[][] testData2 = {
      {"email", "email", "fake.email2@google.com"},
      {"phone", "phone", "299-999-9999"},
      {"first_name", "first_name", "fake_first_name2"},
      {"last_name", "last_name", "fake_last_name2"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .addRecord(getDataRecord(testData))
            .addRecord(getDataRecord(testData2))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    String[][] piiDataEmail2 = {{"pii_value", "pii_value", "fake.email2@google.com"}};
    String[][] piiDataPhone2 = {{"pii_value", "pii_value", "299-999-9999"}};
    String[][] piiDataAddress2 = {
      {"pii_value", "pii_value", "fake_first_name2fake_last_name2US99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail2))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataPhone2))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .addRecord(getDataRecordHashed(piiDataAddress2))
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("email", result.records().get(1).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(1).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(1).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(1).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(1).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(1).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(1).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(1).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(1).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(1).getKeyValues(5).getStringValue());
    assertEquals("email", result.records().get(2).getKeyValues(0).getKey());
    assertEquals(
        "fake.email2@google.com", result.records().get(2).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(2).getKeyValues(1).getKey());
    assertEquals("299-999-9999", result.records().get(2).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(2).getKeyValues(2).getKey());
    assertEquals("fake_first_name2", result.records().get(2).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(2).getKeyValues(3).getKey());
    assertEquals("fake_last_name2", result.records().get(2).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(2).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(2).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(2).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(2).getKeyValues(5).getStringValue());
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(3), conditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(3), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(3), conditionMatchCounts.get("email"));
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(3), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(3), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(3), conditionValidCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(3), datasource2ConditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(5), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(7), datasource2ConditionMatchCounts.get("email"));
  }

  @Test
  public void match_whenMatchFoundWithTransformations_matches() throws Exception {
    MatchConfig testConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass()
                        .getResource(
                            "/com/google/cm/mrp/dataprocessor/testdata/transformation_match_config.json")),
                UTF_8),
            MatchConfig.class);
    DataMatcher testDataMatcher =
        new DataMatcherImpl(dataRecordTransformerFactory, testConfig, DEFAULT_PARAMS);
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "9999"},
      {"country_code", "country_code", "us"},
    };
    Schema ds1Schema = getSchema(testData);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(ds1Schema)
            .addRecord(getDataRecord(testData))
            .addRecord(getDataRecord(testData))
            .build();
    when(dataRecordTransformerFactory.create(testConfig, ds1Schema, DEFAULT_PARAMS))
        .thenReturn(new DataRecordTransformerImpl(testConfig, ds1Schema, DEFAULT_PARAMS));
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameus09999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = testDataMatcher.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertFalse(result.records().isEmpty());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("FAKE.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("9999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("us", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("email", result.records().get(1).getKeyValues(0).getKey());
    assertEquals("FAKE.email@google.com", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(1).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(1).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(1).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(1).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(1).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(1).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(1).getKeyValues(4).getKey());
    assertEquals("9999", result.records().get(1).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(1).getKeyValues(5).getKey());
    assertEquals("us", result.records().get(1).getKeyValues(5).getStringValue());
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionMatchCounts.get("email"));
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(2), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(2), conditionValidCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));
  }

  @Test
  public void match_whenAllMatchesFoundWithPartialSuccessAddsStatusColumn() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {{"pii_value", "pii_value", "fake.email@google.com"}};
    String[][] piiDataPhone = {{"pii_value", "pii_value", "999-999-9999"}};
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", "fake_first_namefake_last_nameUS99999"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecordHashed(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithPartialSuccess.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertFalse(result.records().isEmpty());
    assertEquals(7, result.records().get(0).getKeyValuesCount());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("fake.email@google.com", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("999-999-9999", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("fake_first_name", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("fake_last_name", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("99999", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("US", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("row_status", result.records().get(0).getKeyValues(6).getKey());
    assertEquals("SUCCESS", result.records().get(0).getKeyValues(6).getStringValue());
    assertEquals(3, conditionMatchCounts.size());
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionMatchCounts.get("email"));
    assertEquals(3, conditionValidCounts.size());
    assertEquals(Long.valueOf(1), conditionValidCounts.get("address"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("phone"));
    assertEquals(Long.valueOf(1), conditionValidCounts.get("email"));
    assertEquals(0, datasource1Errors.size());
    assertEquals(3, datasource2ConditionMatchCounts.size());
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("address"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("phone"));
    assertEquals(Long.valueOf(1), datasource2ConditionMatchCounts.get("email"));

    // Check that row status column was appended for partial success jobs.
    String recordStatusColumnName =
        MatchConfigProvider.getMatchConfig("copla")
            .getSuccessConfig()
            .getPartialSuccessAttributes()
            .getRecordStatusFieldName();
    Column recordStatusColumn =
        Column.newBuilder()
            .setColumnAlias(recordStatusColumnName)
            .setColumnName(recordStatusColumnName)
            .setColumnType(ColumnType.STRING)
            .build();
    Schema expectedSchema = getSchema(testData).toBuilder().addColumns(recordStatusColumn).build();
    assertEquals(result.schema(), expectedSchema);
  }

  @Test
  public void match_whenErrorCodeInRecordRedactsMatchColumnsOnly() {
    String[][] testData0 = {
      {"email", "email", "fake.email@google.com"},
      {"extraColumn0", "extraColumn0", "extraColumn0"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"extraColumn1", "extraColumn1", "extraColumn1"},
    };
    var errorCode0 = DECRYPTION_ERROR;
    var dataRecord0 = getDataRecord(testData0).toBuilder().setErrorCode(errorCode0);
    String[][] testData1 = {
      {"email", "email", "fake.email1@google.com"},
      {"extraColumn0", "extraColumn0", "extraColumn0"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
      {"extraColumn1", "extraColumn1", "extraColumn1"},
    };
    var errorCode1 = DEK_DECRYPTION_ERROR;
    var dataRecord1 = getDataRecord(testData1).toBuilder().setErrorCode(errorCode1);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData0))
            .addRecord(dataRecord0)
            .addRecord(dataRecord1)
            .build();
    DataChunk dataChunk2 = DataChunk.builder().setSchema(Schema.getDefaultInstance()).build();

    DataMatchResult dataMatchResult = dataMatcherWithPartialSuccess.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    var matchStats = dataMatchResult.matchStatistics();
    assertThat(matchStats.numDataRecords()).isEqualTo(2);
    assertThat(matchStats.numDataRecordsWithMatch()).isEqualTo(0);
    Map<String, Long> conditionMatchCounts = matchStats.conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = matchStats.datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts = matchStats.datasource2ConditionMatches();
    assertFalse(result.records().isEmpty());
    assertEquals(9, result.records().get(0).getKeyValuesCount());
    assertEquals("email", result.records().get(0).getKeyValues(0).getKey());
    assertEquals("ERROR", result.records().get(0).getKeyValues(0).getStringValue());
    assertEquals("extraColumn0", result.records().get(0).getKeyValues(1).getKey());
    assertEquals("extraColumn0", result.records().get(0).getKeyValues(1).getStringValue());
    assertEquals("phone", result.records().get(0).getKeyValues(2).getKey());
    assertEquals("ERROR", result.records().get(0).getKeyValues(2).getStringValue());
    assertEquals("first_name", result.records().get(0).getKeyValues(3).getKey());
    assertEquals("ERROR", result.records().get(0).getKeyValues(3).getStringValue());
    assertEquals("last_name", result.records().get(0).getKeyValues(4).getKey());
    assertEquals("ERROR", result.records().get(0).getKeyValues(4).getStringValue());
    assertEquals("zip_code", result.records().get(0).getKeyValues(5).getKey());
    assertEquals("ERROR", result.records().get(0).getKeyValues(5).getStringValue());
    assertEquals("country_code", result.records().get(0).getKeyValues(6).getKey());
    assertEquals("ERROR", result.records().get(0).getKeyValues(6).getStringValue());
    assertEquals("extraColumn1", result.records().get(0).getKeyValues(7).getKey());
    assertEquals("extraColumn1", result.records().get(0).getKeyValues(7).getStringValue());
    assertEquals("row_status", result.records().get(0).getKeyValues(8).getKey());
    assertEquals(errorCode0.name(), result.records().get(0).getKeyValues(8).getStringValue());
    assertEquals(9, result.records().get(1).getKeyValuesCount());
    assertEquals("email", result.records().get(1).getKeyValues(0).getKey());
    assertEquals("ERROR", result.records().get(1).getKeyValues(0).getStringValue());
    assertEquals("extraColumn0", result.records().get(1).getKeyValues(1).getKey());
    assertEquals("extraColumn0", result.records().get(1).getKeyValues(1).getStringValue());
    assertEquals("phone", result.records().get(1).getKeyValues(2).getKey());
    assertEquals("ERROR", result.records().get(1).getKeyValues(2).getStringValue());
    assertEquals("first_name", result.records().get(1).getKeyValues(3).getKey());
    assertEquals("ERROR", result.records().get(1).getKeyValues(3).getStringValue());
    assertEquals("last_name", result.records().get(1).getKeyValues(4).getKey());
    assertEquals("ERROR", result.records().get(1).getKeyValues(4).getStringValue());
    assertEquals("zip_code", result.records().get(1).getKeyValues(5).getKey());
    assertEquals("ERROR", result.records().get(1).getKeyValues(5).getStringValue());
    assertEquals("country_code", result.records().get(1).getKeyValues(6).getKey());
    assertEquals("ERROR", result.records().get(1).getKeyValues(6).getStringValue());
    assertEquals("extraColumn1", result.records().get(1).getKeyValues(7).getKey());
    assertEquals("extraColumn1", result.records().get(1).getKeyValues(7).getStringValue());
    assertEquals("row_status", result.records().get(1).getKeyValues(8).getKey());
    assertEquals(errorCode1.name(), result.records().get(1).getKeyValues(8).getStringValue());
    assertEquals(2, datasource1Errors.size());
    // boxing due to compiler warning
    assertEquals(Long.valueOf(1), datasource1Errors.get(errorCode0.name()));
    assertEquals(Long.valueOf(1), datasource1Errors.get(errorCode1.name()));
    assertEquals(0, conditionValidCounts.size());
    assertEquals(0, conditionMatchCounts.size());
    assertEquals(0, datasource2ConditionMatchCounts.size());
  }

  @Test
  public void match_whenNoPartialAttributesSetThrowsJobFailure() {
    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataMatcherImpl(
                    dataRecordTransformerFactory,
                    MatchConfig.newBuilder()
                        .mergeFrom(MatchConfigProvider.getMatchConfig("adh"))
                        .setSuccessConfig(
                            SuccessConfig.newBuilder()
                                .setSuccessMode(SuccessMode.ALLOW_PARTIAL_SUCCESS))
                        .build(),
                    DEFAULT_PARAMS));

    assertEquals(ex.getErrorCode(), PARTIAL_SUCCESS_CONFIG_ERROR);
    assertEquals(
        ex.getMessage(),
        "SUCCESS_MODE is ALLOW_PARTIAL_SUCCESS, but partial_success_attributes empty");
  }

  @Test
  public void match_dataRecordsWithRowMarker_statsBasedOnRowMarkers() {
    List<String> ds1Fields = ImmutableList.of("email", ROW_MARKER_COLUMN_NAME);
    FieldParser ds1FieldParser = new FieldParser(ds1Fields);
    var ds1Records =
        ImmutableList.of(
            ds1FieldParser.getDataRecord(/*email*/ "fake.email0@google.com", /*row_marker*/ "rm0"),
            ds1FieldParser.getDataRecord(/*email*/ "fake.email1@google.com", /*row_marker*/ "rm0"),
            ds1FieldParser.getDataRecord(/*email*/ "fake.email2@google.com", /*row_marker*/ "rm0"),
            ds1FieldParser.getDataRecord(/*email*/ "fake.email3@google.com", /*row_marker*/ "rm1"),
            ds1FieldParser.getDataRecord(/*email*/ "fake.email4@google.com", /*row_marker*/ "rm1"),
            ds1FieldParser.getDataRecord(/*email*/ "fake.email5@google.com", /*row_marker*/ "rm2"));
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(ds1Fields))
            .setRecords(ds1Records)
            .build();
    List<String> ds2Fields = ImmutableList.of("pii_value");
    FieldParser ds2FieldParser = new FieldParser(ds2Fields);
    var ds2Records =
        ImmutableList.of(
            ds2FieldParser.getDataRecord(/*pii_value*/ "fake.email0@google.com"),
            ds2FieldParser.getDataRecord(/*pii_value*/ "fake.email1@google.com"),
            ds2FieldParser.getDataRecord(/*pii_value*/ "fake.email4@google.com"));
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(ds2Fields))
            .setRecords(ds2Records)
            .build();

    DataMatchResult dataMatchResult = dataMatcher.match(dataChunk1, dataChunk2);

    var matchStats = dataMatchResult.matchStatistics();
    assertThat(matchStats.numDataRecords()).isEqualTo(3);
    assertThat(matchStats.numDataRecordsWithMatch()).isEqualTo(2);
    // assert condition matches for completeness
    Map<String, Long> conditionMatchCounts = matchStats.conditionMatches();
    assertThat(conditionMatchCounts).containsExactly("email", 3L);
    Map<String, Long> conditionValidCounts = matchStats.validConditionChecks();
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 0L);
    assertThat(conditionValidCounts).containsEntry("phone", 0L);
    assertThat(conditionValidCounts).containsEntry("email", 6L);
    Map<String, Long> datasource1Errors = matchStats.datasource1Errors();
    assertThat(datasource1Errors).isEmpty();
    Map<String, Long> datasource2ConditionMatchCounts = matchStats.datasource2ConditionMatches();
    assertThat(datasource2ConditionMatchCounts).containsExactly("email", 3L);
  }

  @Test
  public void match_dataRecordsWithRowMarker_errorRowsAreCountedCorrectly() {
    List<String> ds1Fields = ImmutableList.of("email", ROW_MARKER_COLUMN_NAME);
    FieldParser ds1FieldParser = new FieldParser(ds1Fields);
    var ds1Records =
        ImmutableList.of(
            ds1FieldParser.getDataRecord(DECRYPTION_ERROR, /*email*/ "fake.email0@google.com", /*row_marker*/ "rm0"),
            ds1FieldParser.getDataRecord(/*email*/ "fake.email2@google.com", /*row_marker*/ "rm1"));
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(ds1Fields))
            .setRecords(ds1Records)
            .build();
    List<String> ds2Fields = ImmutableList.of("pii_value");
    FieldParser ds2FieldParser = new FieldParser(ds2Fields);
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(ds2Fields))
            .addRecord(ds2FieldParser.getDataRecord(/*pii_value*/ "fake.email2@google.com"))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithPartialSuccess.match(dataChunk1, dataChunk2);

    var matchStats = dataMatchResult.matchStatistics();
    assertThat(matchStats.numDataRecords()).isEqualTo(2);
    assertThat(matchStats.numDataRecordsWithMatch()).isEqualTo(1);
    Map<String, Long> datasource1Errors = matchStats.datasource1Errors();
    assertThat(datasource1Errors).hasSize(1);
    // assert condition matches for completeness
    Map<String, Long> conditionMatchCounts = matchStats.conditionMatches();
    assertThat(conditionMatchCounts).containsExactly("email", 1L);
    Map<String, Long> conditionValidCounts = matchStats.validConditionChecks();
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 0L);
    assertThat(conditionValidCounts).containsEntry("phone", 0L);
    assertThat(conditionValidCounts).containsEntry("email", 1L);
    Map<String, Long> datasource2ConditionMatchCounts = matchStats.datasource2ConditionMatches();
    assertThat(datasource2ConditionMatchCounts).containsExactly("email", 1L);
  }

  @Test
  public void match_joinModeWhenMatchFound_keepsAndReturns() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name", "1"},
      {"last_name", "last_name", "fake_last_name", "1"},
      {"zip_code", "zip_code", "99999", "1"},
      {"country_code", "country_code", "US", "1"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "fake.email@google.com"},
      {"encrypted_gaia_id", "encrypted_gaia_id", "123"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "999-999-9999"}, {"encrypted_gaia_id", "encrypted_gaia_id", "234"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", hashString("fake_first_namefake_last_nameUS99999")},
      {"encrypted_gaia_id", "encrypted_gaia_id", "345"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertThat(result.records()).hasSize(1);
    DataRecord dataRecord = result.records().get(0);
    assertThat(dataRecord.getKeyValuesCount()).isEqualTo(7);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo("fake.email@google.com");
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo("999-999-9999");
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo("fake_first_name");
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo("fake_last_name");
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo("99999");
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo("US");
    assertThat(dataRecord.getKeyValues(6).getKey()).isEqualTo("row_status");
    assertThat(dataRecord.getKeyValues(6).getStringValue()).isEqualTo("SUCCESS");
    assertThat(conditionMatchCounts).hasSize(3);
    assertThat(conditionMatchCounts).containsEntry("address", 1L);
    assertThat(conditionMatchCounts).containsEntry("phone", 1L);
    assertThat(conditionMatchCounts).containsEntry("email", 1L);
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 1L);
    assertThat(conditionValidCounts).containsEntry("phone", 1L);
    assertThat(conditionValidCounts).containsEntry("email", 1L);
    assertThat(datasource1Errors).isEmpty();
    assertThat(datasource2ConditionMatchCounts).hasSize(3);
    assertThat(datasource2ConditionMatchCounts).containsEntry("address", 1L);
    assertThat(datasource2ConditionMatchCounts).containsEntry("phone", 1L);
    assertThat(datasource2ConditionMatchCounts).containsEntry("email", 1L);
    var singleFieldMatches = dataRecord.getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches).hasSize(2);
    // check email
    assertThat(singleFieldMatches).containsKey(0);
    assertThat(singleFieldMatches.get(0).hasSingleFieldMatchedOutput()).isTrue();
    var singleFieldOutputs =
        singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    var field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("123");
    // check phone
    assertThat(singleFieldMatches).containsKey(1);
    assertThat(singleFieldMatches.get(1).hasSingleFieldMatchedOutput()).isTrue();
    singleFieldOutputs =
        singleFieldMatches.get(1).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("234");
    // check address
    assertThat(singleFieldMatches).doesNotContainKey(2);
    assertThat(singleFieldMatches).doesNotContainKey(3);
    assertThat(singleFieldMatches).doesNotContainKey(4);
    assertThat(singleFieldMatches).doesNotContainKey(5);
    var compositeFieldMatches = dataRecord.getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches).containsKey(1);
    assertThat(compositeFieldMatches.get(1).hasCompositeFieldMatchedOutput()).isTrue();
    var compositeFieldOutputs =
        compositeFieldMatches.get(1).getCompositeFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(compositeFieldOutputs).hasSize(1);
    assertThat(compositeFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = compositeFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("345");
  }

  @Test
  public void match_joinModeWhenOnlyEmailMatchFound_keepsAndReturnsForEmailOnly() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "fake.email@google.com"},
      {"encrypted_gaia_id", "encrypted_gaia_id", "123"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "000-000-0000"}, {"encrypted_gaia_id", "encrypted_gaia_id", "234"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", hashString("fake_first_name0fake_last_name0US0000")},
      {"encrypted_gaia_id", "encrypted_gaia_id", "345"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertThat(result.records()).hasSize(1);
    DataRecord dataRecord = result.records().get(0);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo("fake.email@google.com");
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(conditionMatchCounts).hasSize(1);
    assertThat(conditionMatchCounts).containsEntry("email", 1L);
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 1L);
    assertThat(conditionValidCounts).containsEntry("phone", 1L);
    assertThat(conditionValidCounts).containsEntry("email", 1L);
    assertThat(datasource1Errors).isEmpty();
    assertThat(datasource2ConditionMatchCounts).hasSize(1);
    assertThat(datasource2ConditionMatchCounts).containsEntry("email", 1L);
    var singleFieldMatches = dataRecord.getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches).hasSize(1);
    // check email
    assertThat(singleFieldMatches).containsKey(0);
    assertThat(singleFieldMatches.get(0).hasSingleFieldMatchedOutput()).isTrue();
    var singleFieldOutputs =
        singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    var field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("123");
    // No more matches
    assertThat(singleFieldMatches).doesNotContainKey(1);
    assertThat(singleFieldMatches).doesNotContainKey(2);
    assertThat(singleFieldMatches).doesNotContainKey(3);
    assertThat(singleFieldMatches).doesNotContainKey(4);
    assertThat(singleFieldMatches).doesNotContainKey(5);
    var compositeFieldMatches = dataRecord.getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches).isEmpty();
  }

  @Test
  public void match_joinModeWhenOnlyAddressMatchFound_keepsAndReturnsForAddressOnly() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "invalid@google.com"},
      {"encrypted_gaia_id", "encrypted_gaia_id", "123"}
    };
    String[][] piiDataPhone = {
      {"pii_value", "pii_value", "000-000-0000"}, {"encrypted_gaia_id", "encrypted_gaia_id", "234"}
    };
    String[][] piiDataAddress = {
      {"pii_value", "pii_value", hashString("fake_first_namefake_last_nameUS99999")},
      {"encrypted_gaia_id", "encrypted_gaia_id", "345"}
    };
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchema(piiDataEmail))
            .addRecord(getDataRecord(piiDataEmail))
            .addRecord(getDataRecord(piiDataPhone))
            .addRecord(getDataRecord(piiDataAddress))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();

    assertThat(result.records()).hasSize(1);
    DataRecord dataRecord = result.records().get(0);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo("fake_first_name");
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo("fake_last_name");
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo("99999");
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo("US");
    assertThat(conditionMatchCounts).hasSize(1);
    assertThat(conditionMatchCounts).containsEntry("address", 1L);
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 1L);
    assertThat(conditionValidCounts).containsEntry("phone", 1L);
    assertThat(conditionValidCounts).containsEntry("email", 1L);
    assertThat(datasource1Errors).isEmpty();
    assertThat(datasource2ConditionMatchCounts).hasSize(1);
    assertThat(datasource2ConditionMatchCounts).containsEntry("address", 1L);
    // check address
    var compositeFieldMatches = dataRecord.getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches).containsKey(0);
    assertThat(compositeFieldMatches.get(0).hasCompositeFieldMatchedOutput()).isTrue();
    var compositeFieldOutputs =
        compositeFieldMatches.get(0).getCompositeFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(compositeFieldOutputs).hasSize(1);
    assertThat(compositeFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    var field = compositeFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("345");
    // No more matches
    assertThat(dataRecord.getJoinFields().getSingleFieldRecordMatchesMap()).isEmpty();
  }

  @Test
  public void match_joinModeWhenMatchNotFound_redactsAllReturnsNone() {
    String[][] testData = {
      {"email", "email", "fake.email@google.com"},
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "99999"},
      {"country_code", "country_code", "US"},
    };
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(getSchema(testData))
            .addRecord(getDataRecord(testData))
            .build();
    String[][] piiDataEmail = {
      {"pii_value", "pii_value", "invalid@google.com"},
      {"encrypted_gaia_id", "encrypted_gaia_id", "123"}
    };
    DataChunk dataChunk2 = DataChunk.builder().setSchema(getSchema(piiDataEmail)).build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);
    DataChunk result = dataMatchResult.dataChunk();
    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertThat(result.records()).hasSize(1);
    DataRecord dataRecord = result.records().get(0);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo(REDACT_UNMATCHED_WITH);
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 1L);
    assertThat(conditionValidCounts).containsEntry("phone", 1L);
    assertThat(conditionValidCounts).containsEntry("email", 1L);
    assertThat(datasource1Errors).isEmpty();
    assertThat(conditionMatchCounts).isEmpty();
    assertThat(datasource2ConditionMatchCounts).isEmpty();
    var singleFieldMatches = dataRecord.getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches).isEmpty();
    var compositeFieldMatches = dataRecord.getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches).isEmpty();
  }

  @Test
  public void match_joinModeAndMultipleGaiaResults_validConditionCounts() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    DataChunk dataChunk1 =
        DataChunk.builder().setSchema(DEFAULT_SCHEMA).addRecord(dataSource1Record).build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "pii_type", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var dataSource2Records =
        ImmutableList.of(
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_2"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
                /*pii_type*/ "A",
                /*encrypted_gaia_id*/ "a_0"));
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(dataSource2Records)
            .build();
    DataMatcherImpl testDataMatcher = getDataMatcherWithPiiType();

    DataMatchResult dataMatchResult = testDataMatcher.match(dataChunk1, dataChunk2);

    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    assertThat(conditionMatchCounts).hasSize(3);
    assertThat(conditionMatchCounts).containsEntry("address", 1L);
    assertThat(conditionMatchCounts).containsEntry("phone", 1L);
    assertThat(conditionMatchCounts).containsEntry("email", 1L);
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 1L);
    assertThat(conditionValidCounts).containsEntry("phone", 1L);
    assertThat(conditionValidCounts).containsEntry("email", 1L);
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    assertThat(datasource1Errors).isEmpty();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertThat(datasource2ConditionMatchCounts).hasSize(3);
    assertThat(datasource2ConditionMatchCounts).containsEntry("address", 1L);
    assertThat(datasource2ConditionMatchCounts).containsEntry("phone", 2L);
    assertThat(datasource2ConditionMatchCounts).containsEntry("email", 3L);
  }

  @Test
  public void match_joinModeAndMultipleGaiaResults_validDataMatchResultValues() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    DataChunk dataChunk1 =
        DataChunk.builder().setSchema(DEFAULT_SCHEMA).addRecord(dataSource1Record).build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "pii_type", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var dataSource2Records =
        ImmutableList.of(
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_2"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
                /*pii_type*/ "A",
                /*encrypted_gaia_id*/ "a_0"));
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(dataSource2Records)
            .build();
    DataMatcherImpl testDataMatcher = getDataMatcherWithPiiType();

    DataMatchResult dataMatchResult = testDataMatcher.match(dataChunk1, dataChunk2);

    assertThat(dataMatchResult.dataChunk().records()).hasSize(1);
    DataRecord dataRecord = dataMatchResult.dataChunk().records().get(0);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo("fake.email@google.com");
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo("999-999-9999");
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo("fake_first_name");
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo("fake_last_name");
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo("99999");
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo("US");
  }

  @Test
  public void match_joinModeAndMultipleGaiaResults_validEmailJoinedField() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    DataChunk dataChunk1 =
        DataChunk.builder().setSchema(DEFAULT_SCHEMA).addRecord(dataSource1Record).build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "pii_type", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var dataSource2Records =
        ImmutableList.of(
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_2"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
                /*pii_type*/ "A",
                /*encrypted_gaia_id*/ "a_0"));
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(dataSource2Records)
            .build();
    DataMatcherImpl testDataMatcher = getDataMatcherWithPiiType();

    DataMatchResult dataMatchResult = testDataMatcher.match(dataChunk1, dataChunk2);

    DataRecord dataRecord = dataMatchResult.dataChunk().records().get(0);
    var singleFieldMatches = dataRecord.getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches).hasSize(2);
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    assertThat(singleFieldMatches.get(0).hasSingleFieldMatchedOutput()).isTrue();
    var singleFieldOutputs =
        sortFieldByGaia(
            singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList());
    assertThat(singleFieldOutputs).hasSize(3);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(2);
    var gaia = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(gaia.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(gaia.getValue()).isEqualTo("e_0");
    var piiType = singleFieldOutputs.get(0).getIndividualFields(1);
    assertThat(piiType.getKey()).isEqualTo("pii_type");
    assertThat(piiType.getValue()).isEqualTo("E");
    assertThat(singleFieldOutputs.get(1).getIndividualFieldsCount()).isEqualTo(2);
    gaia = singleFieldOutputs.get(1).getIndividualFields(0);
    assertThat(gaia.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(gaia.getValue()).isEqualTo("e_1");
    piiType = singleFieldOutputs.get(1).getIndividualFields(1);
    assertThat(piiType.getKey()).isEqualTo("pii_type");
    assertThat(piiType.getValue()).isEqualTo("E");
    assertThat(singleFieldOutputs.get(2).getIndividualFieldsCount()).isEqualTo(2);
    gaia = singleFieldOutputs.get(2).getIndividualFields(0);
    assertThat(gaia.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(gaia.getValue()).isEqualTo("e_2");
    piiType = singleFieldOutputs.get(2).getIndividualFields(1);
    assertThat(piiType.getKey()).isEqualTo("pii_type");
    assertThat(piiType.getValue()).isEqualTo("E");
  }

  @Test
  public void match_joinModeAndMultipleGaiaResults_validPhoneJoinedField() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    DataChunk dataChunk1 =
        DataChunk.builder().setSchema(DEFAULT_SCHEMA).addRecord(dataSource1Record).build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "pii_type", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var dataSource2Records =
        ImmutableList.of(
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_2"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
                /*pii_type*/ "A",
                /*encrypted_gaia_id*/ "a_0"));
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(dataSource2Records)
            .build();
    DataMatcherImpl testDataMatcher = getDataMatcherWithPiiType();

    DataMatchResult dataMatchResult = testDataMatcher.match(dataChunk1, dataChunk2);

    var dataRecord = dataMatchResult.dataChunk().records().get(0);
    var singleFieldMatches = dataRecord.getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches).hasSize(2);
    var singleFieldOutputs =
        sortFieldByGaia(
            singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList());
    // check phone
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    assertThat(singleFieldMatches.get(1).hasSingleFieldMatchedOutput()).isTrue();
    singleFieldOutputs =
        sortFieldByGaia(
            singleFieldMatches.get(1).getSingleFieldMatchedOutput().getMatchedOutputFieldsList());
    assertThat(singleFieldOutputs).hasSize(2);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(2);
    var gaia = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(gaia.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(gaia.getValue()).isEqualTo("p_0");
    var piiType = singleFieldOutputs.get(0).getIndividualFields(1);
    assertThat(piiType.getKey()).isEqualTo("pii_type");
    assertThat(piiType.getValue()).isEqualTo("P");
    assertThat(singleFieldOutputs.get(1).getIndividualFieldsCount()).isEqualTo(2);
    gaia = singleFieldOutputs.get(1).getIndividualFields(0);
    assertThat(gaia.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(gaia.getValue()).isEqualTo("p_1");
    piiType = singleFieldOutputs.get(1).getIndividualFields(1);
    assertThat(piiType.getKey()).isEqualTo("pii_type");
    assertThat(piiType.getValue()).isEqualTo("P");
  }

  @Test
  public void match_joinModeAndMultipleGaiaResults_validAddressJoinedField() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    DataChunk dataChunk1 =
        DataChunk.builder().setSchema(DEFAULT_SCHEMA).addRecord(dataSource1Record).build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "pii_type", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var dataSource2Records =
        ImmutableList.of(
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "fake.email@google.com",
                /*pii_type*/ "E",
                /*encrypted_gaia_id*/ "e_2"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_0"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ "999-999-9999", /*pii_type*/ "P", /*encrypted_gaia_id*/ "p_1"),
            dataSource2FieldParser.getDataRecord(
                /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
                /*pii_type*/ "A",
                /*encrypted_gaia_id*/ "a_0"));
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(dataSource2Records)
            .build();
    DataMatcherImpl testDataMatcher = getDataMatcherWithPiiType();

    DataMatchResult dataMatchResult = testDataMatcher.match(dataChunk1, dataChunk2);

    DataRecord dataRecord = dataMatchResult.dataChunk().records().get(0);
    // check address
    var compositeFieldMatches = dataRecord.getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches.keySet()).containsExactly(0);
    assertThat(compositeFieldMatches.get(0).hasCompositeFieldMatchedOutput()).isTrue();
    var compositeFieldOutputs =
        sortFieldByGaia(
            compositeFieldMatches
                .get(0)
                .getCompositeFieldMatchedOutput()
                .getMatchedOutputFieldsList());
    assertThat(compositeFieldOutputs).hasSize(1);
    assertThat(compositeFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(2);
    var gaia = compositeFieldOutputs.get(0).getIndividualFields(0);
    assertThat(gaia.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(gaia.getValue()).isEqualTo("a_0");
    var piiType = compositeFieldOutputs.get(0).getIndividualFields(1);
    assertThat(piiType.getKey()).isEqualTo("pii_type");
    assertThat(piiType.getValue()).isEqualTo("A");
  }

  @Test
  public void match_joinModeWhenMultipleRepeatedGaia_validateConditionCounts() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record0 = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    List<String> testData1 =
        ImmutableList.of(
            /*email*/ "fake.email2@google.com",
            /*phone*/ "299-999-9999",
            /*first_name*/ "fake_first_name2",
            /*last_name*/ "fake_last_name2",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record1 = DEFAULT_FIELD_PARSER.getDataRecord(testData1);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(DEFAULT_SCHEMA)
            .setRecords(
                ImmutableList.of(dataSource1Record0, dataSource1Record0, dataSource1Record1))
            .build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var piiDataEmail0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email@google.com", /*encrypted_gaia_id*/ "e_0");
    var piiDataPhone0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "999-999-9999", /*encrypted_gaia_id*/ "p_0");
    var piiDataAddress0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
            /*encrypted_gaia_id*/ "a_0");
    var piiDataEmail1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email2@google.com", /*encrypted_gaia_id*/ "e_1");
    var piiDataPhone1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "299-999-9999", /*encrypted_gaia_id*/ "p_1");
    var piiDataAddress1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_name2fake_last_name2US99999"),
            /*encrypted_gaia_id*/ "a_1");
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(
                ImmutableList.of(
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0, // 6
                    piiDataEmail1,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0, // 4
                    piiDataPhone1,
                    piiDataAddress0,
                    piiDataAddress0, // 2
                    piiDataAddress1))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);

    Map<String, Long> conditionMatchCounts = dataMatchResult.matchStatistics().conditionMatches();
    assertThat(conditionMatchCounts).hasSize(3);
    assertThat(conditionMatchCounts).containsEntry("address", 3L);
    assertThat(conditionMatchCounts).containsEntry("phone", 3L);
    assertThat(conditionMatchCounts).containsEntry("email", 3L);
    Map<String, Long> conditionValidCounts =
        dataMatchResult.matchStatistics().validConditionChecks();
    assertThat(conditionValidCounts).hasSize(3);
    assertThat(conditionValidCounts).containsEntry("address", 3L);
    assertThat(conditionValidCounts).containsEntry("phone", 3L);
    assertThat(conditionValidCounts).containsEntry("email", 3L);
    Map<String, Long> datasource1Errors = dataMatchResult.matchStatistics().datasource1Errors();
    assertThat(datasource1Errors).isEmpty();
    Map<String, Long> datasource2ConditionMatchCounts =
        dataMatchResult.matchStatistics().datasource2ConditionMatches();
    assertThat(datasource2ConditionMatchCounts).hasSize(3);
    assertThat(datasource2ConditionMatchCounts).containsEntry("address", 3L);
    assertThat(datasource2ConditionMatchCounts).containsEntry("phone", 5L);
    assertThat(datasource2ConditionMatchCounts).containsEntry("email", 7L);
  }

  @Test
  public void match_joinModeWhenMultipleRepeatedGaia_validateMatchedValues() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record0 = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    List<String> testData1 =
        ImmutableList.of(
            /*email*/ "fake.email2@google.com",
            /*phone*/ "299-999-9999",
            /*first_name*/ "fake_first_name2",
            /*last_name*/ "fake_last_name2",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record1 = DEFAULT_FIELD_PARSER.getDataRecord(testData1);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(DEFAULT_SCHEMA)
            .setRecords(
                ImmutableList.of(dataSource1Record0, dataSource1Record0, dataSource1Record1))
            .build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var piiDataEmail0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email@google.com", /*encrypted_gaia_id*/ "e_0");
    var piiDataPhone0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "999-999-9999", /*encrypted_gaia_id*/ "p_0");
    var piiDataAddress0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
            /*encrypted_gaia_id*/ "a_0");
    var piiDataEmail1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email2@google.com", /*encrypted_gaia_id*/ "e_1");
    var piiDataPhone1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "299-999-9999", /*encrypted_gaia_id*/ "p_1");
    var piiDataAddress1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_name2fake_last_name2US99999"),
            /*encrypted_gaia_id*/ "a_1");
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(
                ImmutableList.of(
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0, // 6
                    piiDataEmail1,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0, // 4
                    piiDataPhone1,
                    piiDataAddress0,
                    piiDataAddress0, // 2
                    piiDataAddress1))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);

    assertThat(dataMatchResult.dataChunk().records()).hasSize(3);
    var result = dataMatchResult.dataChunk();
    DataRecord dataRecord = result.records().get(0);
    assertThat(dataRecord.getKeyValuesCount()).isEqualTo(7);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo("fake.email@google.com");
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo("999-999-9999");
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo("fake_first_name");
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo("fake_last_name");
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo("99999");
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo("US");
    assertThat(dataRecord.getKeyValues(6).getKey()).isEqualTo("row_status");
    assertThat(dataRecord.getKeyValues(6).getStringValue()).isEqualTo("SUCCESS");
    dataRecord = result.records().get(1);
    assertThat(dataRecord.getKeyValuesCount()).isEqualTo(7);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo("fake.email@google.com");
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo("999-999-9999");
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo("fake_first_name");
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo("fake_last_name");
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo("99999");
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo("US");
    assertThat(dataRecord.getKeyValues(6).getKey()).isEqualTo("row_status");
    assertThat(dataRecord.getKeyValues(6).getStringValue()).isEqualTo("SUCCESS");
    dataRecord = result.records().get(2);
    assertThat(dataRecord.getKeyValuesCount()).isEqualTo(7);
    assertThat(dataRecord.getKeyValues(0).getKey()).isEqualTo("email");
    assertThat(dataRecord.getKeyValues(0).getStringValue()).isEqualTo("fake.email2@google.com");
    assertThat(dataRecord.getKeyValues(1).getKey()).isEqualTo("phone");
    assertThat(dataRecord.getKeyValues(1).getStringValue()).isEqualTo("299-999-9999");
    assertThat(dataRecord.getKeyValues(2).getKey()).isEqualTo("first_name");
    assertThat(dataRecord.getKeyValues(2).getStringValue()).isEqualTo("fake_first_name2");
    assertThat(dataRecord.getKeyValues(3).getKey()).isEqualTo("last_name");
    assertThat(dataRecord.getKeyValues(3).getStringValue()).isEqualTo("fake_last_name2");
    assertThat(dataRecord.getKeyValues(4).getKey()).isEqualTo("zip_code");
    assertThat(dataRecord.getKeyValues(4).getStringValue()).isEqualTo("99999");
    assertThat(dataRecord.getKeyValues(5).getKey()).isEqualTo("country_code");
    assertThat(dataRecord.getKeyValues(5).getStringValue()).isEqualTo("US");
    assertThat(dataRecord.getKeyValues(6).getKey()).isEqualTo("row_status");
    assertThat(dataRecord.getKeyValues(6).getStringValue()).isEqualTo("SUCCESS");
  }

  @Test
  public void match_joinModeWhenMultipleRepeatedGaia_validateEmailJoinedField() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record0 = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    List<String> testData1 =
        ImmutableList.of(
            /*email*/ "fake.email2@google.com",
            /*phone*/ "299-999-9999",
            /*first_name*/ "fake_first_name2",
            /*last_name*/ "fake_last_name2",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record1 = DEFAULT_FIELD_PARSER.getDataRecord(testData1);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(DEFAULT_SCHEMA)
            .setRecords(
                ImmutableList.of(dataSource1Record0, dataSource1Record0, dataSource1Record1))
            .build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var piiDataEmail0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email@google.com", /*encrypted_gaia_id*/ "e_0");
    var piiDataPhone0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "999-999-9999", /*encrypted_gaia_id*/ "p_0");
    var piiDataAddress0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
            /*encrypted_gaia_id*/ "a_0");
    var piiDataEmail1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email2@google.com", /*encrypted_gaia_id*/ "e_1");
    var piiDataPhone1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "299-999-9999", /*encrypted_gaia_id*/ "p_1");
    var piiDataAddress1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_name2fake_last_name2US99999"),
            /*encrypted_gaia_id*/ "a_1");
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(
                ImmutableList.of(
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0, // 6
                    piiDataEmail1,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0, // 4
                    piiDataPhone1,
                    piiDataAddress0,
                    piiDataAddress0, // 2
                    piiDataAddress1))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);

    var result = dataMatchResult.dataChunk();
    assertThat(result.records()).hasSize(3);
    var singleFieldMatches =
        result.records().get(0).getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    // check email
    assertThat(singleFieldMatches).containsKey(0);
    var singleFieldOutputs =
        singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    var field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("e_0");
    // check email
    singleFieldMatches = result.records().get(1).getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    singleFieldOutputs =
        singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("e_0");
    // check email
    singleFieldMatches = result.records().get(2).getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    singleFieldOutputs =
        singleFieldMatches.get(0).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("e_1");
  }

  @Test
  public void match_joinModeWhenMultipleRepeatedGaia_validatePhoneJoinedField() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record0 = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    List<String> testData1 =
        ImmutableList.of(
            /*email*/ "fake.email2@google.com",
            /*phone*/ "299-999-9999",
            /*first_name*/ "fake_first_name2",
            /*last_name*/ "fake_last_name2",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record1 = DEFAULT_FIELD_PARSER.getDataRecord(testData1);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(DEFAULT_SCHEMA)
            .setRecords(
                ImmutableList.of(dataSource1Record0, dataSource1Record0, dataSource1Record1))
            .build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var piiDataEmail0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email@google.com", /*encrypted_gaia_id*/ "e_0");
    var piiDataPhone0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "999-999-9999", /*encrypted_gaia_id*/ "p_0");
    var piiDataAddress0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
            /*encrypted_gaia_id*/ "a_0");
    var piiDataEmail1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email2@google.com", /*encrypted_gaia_id*/ "e_1");
    var piiDataPhone1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "299-999-9999", /*encrypted_gaia_id*/ "p_1");
    var piiDataAddress1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_name2fake_last_name2US99999"),
            /*encrypted_gaia_id*/ "a_1");
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(
                ImmutableList.of(
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0, // 6
                    piiDataEmail1,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0, // 4
                    piiDataPhone1,
                    piiDataAddress0,
                    piiDataAddress0, // 2
                    piiDataAddress1))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    assertThat(result.records()).hasSize(3);
    // check phone
    var singleFieldMatches =
        result.records().get(0).getJoinFields().getSingleFieldRecordMatchesMap();
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    var singleFieldOutputs =
        singleFieldMatches.get(1).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    var field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("p_0");
    singleFieldMatches = result.records().get(1).getJoinFields().getSingleFieldRecordMatchesMap();
    // check phone
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    singleFieldOutputs =
        singleFieldMatches.get(1).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("p_0");
    singleFieldMatches = result.records().get(2).getJoinFields().getSingleFieldRecordMatchesMap();
    // check phone
    assertThat(singleFieldMatches.keySet()).containsExactlyElementsIn(List.of(0, 1));
    singleFieldOutputs =
        singleFieldMatches.get(1).getSingleFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(singleFieldOutputs).hasSize(1);
    assertThat(singleFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = singleFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("p_1");
  }

  @Test
  public void match_joinModeWhenMultipleRepeatedGaia_validateAddressJoinedField() {
    List<String> testData =
        ImmutableList.of(
            /*email*/ "fake.email@google.com",
            /*phone*/ "999-999-9999",
            /*first_name*/ "fake_first_name",
            /*last_name*/ "fake_last_name",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record0 = DEFAULT_FIELD_PARSER.getDataRecord(testData);
    List<String> testData1 =
        ImmutableList.of(
            /*email*/ "fake.email2@google.com",
            /*phone*/ "299-999-9999",
            /*first_name*/ "fake_first_name2",
            /*last_name*/ "fake_last_name2",
            /*zip_code*/ "99999",
            /*country_code*/ "US");
    var dataSource1Record1 = DEFAULT_FIELD_PARSER.getDataRecord(testData1);
    DataChunk dataChunk1 =
        DataChunk.builder()
            .setSchema(DEFAULT_SCHEMA)
            .setRecords(
                ImmutableList.of(dataSource1Record0, dataSource1Record0, dataSource1Record1))
            .build();
    List<String> dataSource2Fields = ImmutableList.of("pii_value", "encrypted_gaia_id");
    FieldParser dataSource2FieldParser = new FieldParser(dataSource2Fields);
    var piiDataEmail0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email@google.com", /*encrypted_gaia_id*/ "e_0");
    var piiDataPhone0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "999-999-9999", /*encrypted_gaia_id*/ "p_0");
    var piiDataAddress0 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_namefake_last_nameUS99999"),
            /*encrypted_gaia_id*/ "a_0");
    var piiDataEmail1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "fake.email2@google.com", /*encrypted_gaia_id*/ "e_1");
    var piiDataPhone1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ "299-999-9999", /*encrypted_gaia_id*/ "p_1");
    var piiDataAddress1 =
        dataSource2FieldParser.getDataRecord(
            /*pii_value*/ hashString("fake_first_name2fake_last_name2US99999"),
            /*encrypted_gaia_id*/ "a_1");
    DataChunk dataChunk2 =
        DataChunk.builder()
            .setSchema(getSchemaFromFields(dataSource2Fields))
            .setRecords(
                ImmutableList.of(
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0,
                    piiDataEmail0, // 6
                    piiDataEmail1,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0,
                    piiDataPhone0, // 4
                    piiDataPhone1,
                    piiDataAddress0,
                    piiDataAddress0, // 2
                    piiDataAddress1))
            .build();

    DataMatchResult dataMatchResult = dataMatcherWithJoinMode.match(dataChunk1, dataChunk2);

    DataChunk result = dataMatchResult.dataChunk();
    assertThat(result.records()).hasSize(3);
    // check address
    var compositeFieldMatches =
        result.records().get(0).getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches.keySet()).containsExactly(0);
    var compositeFieldOutputs =
        compositeFieldMatches.get(0).getCompositeFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(compositeFieldOutputs).hasSize(1);
    assertThat(compositeFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    var field = compositeFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("a_0");
    // check address
    compositeFieldMatches =
        result.records().get(1).getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches.keySet()).containsExactly(0);
    compositeFieldOutputs =
        compositeFieldMatches.get(0).getCompositeFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(compositeFieldOutputs).hasSize(1);
    assertThat(compositeFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = compositeFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("a_0");
    // check address
    compositeFieldMatches =
        result.records().get(2).getJoinFields().getCompositeFieldRecordMatchesMap();
    assertThat(compositeFieldMatches.keySet()).containsExactly(0);
    compositeFieldOutputs =
        compositeFieldMatches.get(0).getCompositeFieldMatchedOutput().getMatchedOutputFieldsList();
    assertThat(compositeFieldOutputs).hasSize(1);
    assertThat(compositeFieldOutputs.get(0).getIndividualFieldsCount()).isEqualTo(1);
    field = compositeFieldOutputs.get(0).getIndividualFields(0);
    assertThat(field.getKey()).isEqualTo("encrypted_gaia_id");
    assertThat(field.getValue()).isEqualTo("a_1");
  }

  private DataChunk buildDataChunk(
      String[][] piiDataEmail, String[][] piiDataPhone, String[][] piiDataAddress) {
    DataChunk.Builder result = DataChunk.builder();
    result.setSchema(getSchema(new String[][] {piiDataEmail[0]}));
    for (String[] keyValue : piiDataEmail) {
      result.addRecord(
          DataRecord.newBuilder()
              .addKeyValues(KeyValue.newBuilder().setKey(keyValue[0]).setStringValue(keyValue[2])));
    }
    for (String[] keyValue : piiDataPhone) {
      result.addRecord(
          DataRecord.newBuilder()
              .addKeyValues(KeyValue.newBuilder().setKey(keyValue[0]).setStringValue(keyValue[2])));
    }
    for (String[] keyValue : piiDataAddress) {
      result.addRecord(
          DataRecord.newBuilder()
              .addKeyValues(
                  KeyValue.newBuilder()
                      .setKey(keyValue[0])
                      .setStringValue(hashString(keyValue[2]))));
    }
    return result.build();
  }

  private Schema getSchema(String[][] keyValueQuads) {
    Schema.Builder builder = Schema.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addColumns(
          Column.newBuilder()
              .setColumnName(keyValue[0])
              .setColumnAlias(keyValue[1])
              .setColumnGroup(keyValue.length > 3 ? Integer.parseInt(keyValue[3]) : 0)
              .build());
    }
    return builder.build();
  }

  private DataMatcherImpl getDataMatcherWithPiiType() {
    MatchConfig micMatchConfig = MatchConfigProvider.getMatchConfig("mic");
    MatchConfig testMatchConfig =
        micMatchConfig.toBuilder()
            .setModeConfigs(
                micMatchConfig.getModeConfigs().toBuilder()
                    .setJoinModeConfig(
                        micMatchConfig.getModeConfigs().getJoinModeConfig().toBuilder()
                            .addJoinFields("pii_type")))
            .build();
    return new DataMatcherImpl(dataRecordTransformerFactory, testMatchConfig, JOIN_MODE_PARAMS);
  }

  private static Schema getDefaultSchemaFromDefaultFields() {
    return getSchemaFromFields(DEFAULT_FIELDS);
  }

  private static Schema getSchemaFromFields(List<String> fields) {
    Schema.Builder builder = Schema.newBuilder();
    for (String alias : fields) {
      builder.addColumns(Column.newBuilder().setColumnName(alias).setColumnAlias(alias).build());
    }
    return builder.build();
  }

  private DataRecord getDataRecord(String[][] keyValueQuads) {
    DataRecord.Builder builder = DataRecord.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addKeyValues(KeyValue.newBuilder().setKey(keyValue[0]).setStringValue(keyValue[2]));
    }
    return builder.build();
  }

  private DataRecord getDataRecordHashed(String[][] keyValueQuads) {
    DataRecord.Builder builder = DataRecord.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addKeyValues(
          KeyValue.newBuilder().setKey(keyValue[0]).setStringValue(hashString(keyValue[2])));
    }
    return builder.build();
  }

  /** Sort MatchedOutputField results by the value of the gaia Ids */
  private List<MatchedOutputField> sortFieldByGaia(List<MatchedOutputField> inputList) {
    String gaiaKey = "encrypted_gaia_id";
    List<MatchedOutputField> mutableList = new ArrayList<>(inputList);
    mutableList.sort(
        ((matchedOutputField0, matchedOutputField1) -> {
          var gaia0 =
              matchedOutputField0.getIndividualFieldsList().stream()
                  .filter(field -> field.getKey().equals(gaiaKey))
                  .findFirst();
          var gaia1 =
              matchedOutputField1.getIndividualFieldsList().stream()
                  .filter(field -> field.getKey().equals(gaiaKey))
                  .findFirst();
          String val0 = gaia0.map(Field::getValue).orElse("yyy");
          String val1 = gaia1.map(Field::getValue).orElse("zzz");
          return val0.compareTo(val1);
        }));
    return mutableList;
  }

  private String hashString(String s) {
    return BaseEncoding.base64().encode(sha256().hashBytes(s.getBytes(UTF_8)).asBytes());
  }

  private static class FieldParser {

    final List<String> fields;

    FieldParser(List<String> fields) {
      this.fields = fields;
    }

    DataRecord getDataRecord(List<String> values) {
      DataRecord.Builder builder = DataRecord.newBuilder();
      for (int i = 0; i < fields.size(); i++) {
        builder.addKeyValues(
            KeyValue.newBuilder().setKey(fields.get(i)).setStringValue(values.get(i)));
      }
      return builder.build();
    }

    DataRecord getDataRecord(String... values) {
      return getDataRecord(SUCCESS, values);
    }

    DataRecord getDataRecord(JobResultCode errorCode, String... values) {
      DataRecord.Builder builder = DataRecord.newBuilder();
      for (int i = 0; i < fields.size(); i++) {
        builder.addKeyValues(KeyValue.newBuilder().setKey(fields.get(i)).setStringValue(values[i]));
      }
      if (errorCode != SUCCESS) {
        builder.setErrorCode(errorCode);
      }
      return builder.build();
    }
  }
}
