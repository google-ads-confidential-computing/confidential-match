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

package com.google.cm.mrp.dataprocessor.transformations;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.TRANSFORMATION_CONFIG_ERROR;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.backend.DataRecordProto;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.CompositeColumn;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchCondition;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchTransformation;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.models.JobParameters;
import com.google.cm.mrp.models.JobParameters.OutputDataLocation;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.util.Objects;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataRecordTransformerImplTest {

  private static final JobParameters DEFAULT_PARAMS =
      JobParameters.builder()
          .setJobId("test")
          .setDataLocation(DataLocation.getDefaultInstance())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
          .build();
  private MatchConfig testMatchConfig;

  @Before
  public void setup() throws Exception {
    testMatchConfig =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(
                    getClass().getResource("testdata/transformation_match_config.json")),
                UTF_8),
            MatchConfig.class);
  }

  @Test
  public void transformDataRecord_success() {
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com"}, // uppercase
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "9999"}, // not five digits
      {"country_code", "country_code", "us"},
    };
    Schema schema = getSchema(testData);
    DataRecord dataRecord = getDataRecord(testData);
    DataRecordTransformerImpl transformer =
        new DataRecordTransformerImpl(testMatchConfig, schema, DEFAULT_PARAMS);

    DataRecord result = transformer.transform(dataRecord);

    assertThat(result)
        .isEqualTo(
            dataRecord.toBuilder()
                .setKeyValues(
                    0,
                    KeyValue.newBuilder().setKey("email").setStringValue("fake.email@google.com"))
                .setKeyValues(4, KeyValue.newBuilder().setKey("zip_code").setStringValue("09999"))
                .build());
  }

  @Test
  public void transformDataRecordWithConditionalColumns_success() {
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com"}, // uppercase
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "9999"}, // not five digits
      {"country_code", "country_code", "us"},
    };
    Schema schema = getSchema(testData);
    DataRecord dataRecord = getDataRecord(testData);
    JobParameters testParams =
        JobParameters.builder()
            .setJobId("test")
            .setDataLocation(DataLocation.getDefaultInstance())
            .setOutputDataLocation(OutputDataLocation.forNameAndPrefix("bucket", "test-path"))
            .setEncodingType(EncodingType.HEX)
            .build();
    DataRecordTransformerImpl transformer =
        new DataRecordTransformerImpl(testMatchConfig, schema, testParams);

    DataRecord result = transformer.transform(dataRecord);

    assertThat(result)
        .isEqualTo(
            dataRecord.toBuilder()
                .setKeyValues(
                    0,
                    KeyValue.newBuilder().setKey("email").setStringValue("fake.email@google.com"))
                .setKeyValues(
                    1,
                    // TODO(b/398109545): add after implementation
                    KeyValue.newBuilder().setKey("phone").setStringValue(""))
                .setKeyValues(4, KeyValue.newBuilder().setKey("zip_code").setStringValue("09999"))
                .build());
  }

  @Test
  public void transformDataRecordWithMultipleColumnGroups_success() {
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com", "0"}, // uppercase
      {"phone", "phone", "999-999-9999", "0"},
      {"first_name", "first_name", "fake_first_name", "0"},
      {"last_name", "last_name", "fake_last_name", "0"},
      {"zip_code", "zip_code", "9999", "0"}, // not five digits
      {"country_code", "country_code", "us", "0"},
      {"email", "email", "FAKE.uk@google.com", "1"}, // uppercase
      {"phone", "phone", "999-999-9999", "1"},
      {"first_name", "first_name", "fake_first_name", "1"},
      {"last_name", "last_name", "fake_last_name", "1"},
      {"zip_code", "zip_code", "999UK", "1"}, // not lowercase
      {"country_code", "country_code", "uk", "1"},
    };
    Schema schema = getSchema(testData);
    DataRecord dataRecord = getDataRecord(testData);
    DataRecordTransformerImpl transformer =
        new DataRecordTransformerImpl(testMatchConfig, schema, DEFAULT_PARAMS);

    DataRecord result = transformer.transform(dataRecord);

    assertThat(result)
        .isEqualTo(
            dataRecord.toBuilder()
                .setKeyValues(
                    0,
                    KeyValue.newBuilder().setKey("email").setStringValue("fake.email@google.com"))
                .setKeyValues(4, KeyValue.newBuilder().setKey("zip_code").setStringValue("09999"))
                .setKeyValues(
                    6, KeyValue.newBuilder().setKey("email").setStringValue("fake.uk@google.com"))
                .setKeyValues(10, KeyValue.newBuilder().setKey("zip_code").setStringValue("999uk"))
                .build());
  }

  @Test
  public void transformDataRecordWithInvalidMatchTransformation_failure() {
    MatchConfig configWithInvalidTransformations =
        testMatchConfig.toBuilder()
            .setMatchConditions(
                0,
                MatchCondition.newBuilder()
                    .setDataSource1Column(
                        CompositeColumn.newBuilder()
                            .setColumnAlias("email")
                            .addColumns(
                                MatchConfig.Column.newBuilder()
                                    .setColumnAlias("email")
                                    .addMatchTransformations(
                                        MatchTransformation.newBuilder()
                                            .setTransformationId("Invalid")))))
            .build();
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com"}, // uppercase
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "9999"}, // not five digits
      {"country_code", "country_code", "us"},
    };
    Schema schema = getSchema(testData);

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                new DataRecordTransformerImpl(
                    configWithInvalidTransformations, schema, DEFAULT_PARAMS));

    assertThat(ex.getErrorCode()).isEqualTo(TRANSFORMATION_CONFIG_ERROR);
    assertThat(ex.getMessage()).isEqualTo("Invalid transformation name in match config");
  }

  @Test
  public void transformDataRecordWithMissingDependentColumns_failure() {
    String[][] testData = {
      {"email", "email", "FAKE.email@google.com"}, // uppercase
      {"phone", "phone", "999-999-9999"},
      {"first_name", "first_name", "fake_first_name"},
      {"last_name", "last_name", "fake_last_name"},
      {"zip_code", "zip_code", "9999"}, // not five digits
    };
    Schema schema = getSchema(testData);
    DataRecord dataRecord = getDataRecord(testData);
    DataRecordTransformerImpl transformer =
        new DataRecordTransformerImpl(testMatchConfig, schema, DEFAULT_PARAMS);

    var ex = assertThrows(JobProcessorException.class, () -> transformer.transform(dataRecord));

    assertThat(ex.getErrorCode()).isEqualTo(INVALID_INPUT_FILE_ERROR);
    assertThat(ex.getMessage()).isEqualTo("Could not transform input KeyValue");
  }

  // TODO(b/329553462): Extract to helper methods where possible
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

  private Schema getSchema(String[][] keyValueQuads) {
    Schema.Builder builder = Schema.newBuilder();
    for (String[] keyValue : keyValueQuads) {
      builder.addColumns(
          Column.newBuilder()
              .setColumnName(keyValue[0])
              .setColumnAlias(keyValue[1])
              .setColumnGroup(keyValue.length > 3 ? Integer.parseInt(keyValue[3]) : 0)
              .setEncrypted(false)
              .build());
    }
    return builder.build();
  }
}
