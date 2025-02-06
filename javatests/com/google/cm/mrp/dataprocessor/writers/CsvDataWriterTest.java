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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class CsvDataWriterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Test
  public void write_writesDataChunkWithHeader() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    CsvDataWriter dataWriter =
        new CsvDataWriter(
            1000,
            testDataDestination,
            "test.csv",
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass().getResource("/com/google/cm/mrp/dataprocessor/testdata/schema_different_name_than_alias.json")),
                    UTF_8),
                Schema.class),
            // Set default headers different than schema
            List.of("email", "hone", "first_name", "last_name", "zip_code", "country_code"));

    dataWriter.write(getDataChunk());
    dataWriter.close();

    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("test_1.csv");
    assertThat(testDataDestination.fileLines).hasSize(2);
    assertThat(testDataDestination.fileLines.get(0))
        .isEqualTo("Email,Phone,FirstName,LastName,Zip,Country");
    assertThat(testDataDestination.fileLines.get(1))
        .isEqualTo("fake.email@google.com,999-999-9999,fake_first_name,fake_last_name,99999,US");
  }

  @Test
  public void write_writesDataChunkWithoutHeader() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination();
    CsvDataWriter dataWriter =
        new CsvDataWriter(
            1000,
            testDataDestination,
            "test.csv",
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass().getResource("/com/google/cm/mrp/dataprocessor/testdata/schema_no_header.json")),
                    UTF_8),
                Schema.class),
            // Set default headers different than schema
            List.of("email", "hone", "first_name", "last_name", "zip_code", "country_code"));

    dataWriter.write(getDataChunk());
    dataWriter.close();

    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
    assertThat(testDataDestination.name).isEqualTo("test_1.csv");
    assertThat(testDataDestination.fileLines).hasSize(1);
    assertThat(testDataDestination.fileLines.get(0))
        .isEqualTo("fake.email@google.com,999-999-9999,fake_first_name,fake_last_name,99999,US");
  }

  @Test
  public void write_whenWriteErrorDeleteFile() throws Exception {
    TestDataDestination testDataDestination = new TestDataDestination(/* throwException= */ true);
    CsvDataWriter dataWriter =
        new CsvDataWriter(
            1000,
            testDataDestination,
            "test",
            ProtoUtils.getProtoFromJson(
                Resources.toString(
                    Objects.requireNonNull(
                        getClass().getResource("/com/google/cm/mrp/dataprocessor/testdata/schema_no_header.json")),
                    UTF_8),
                Schema.class),
            List.of());

    dataWriter.write(getDataChunk());
    var ex = assertThrows(JobProcessorException.class, dataWriter::close);

    assertThat(ex.getMessage())
        .isEqualTo("CSV data writer threw an exception while uploading the file.");
    assertThat(testDataDestination.file.exists()).isFalse(); // the temp file should've been deleted
  }

  private DataChunk getDataChunk() {
    return DataChunk.builder()
        .addRecord(
            DataRecord.newBuilder()
                .addKeyValues(
                    KeyValue.newBuilder().setKey("email").setStringValue("fake.email@google.com"))
                .addKeyValues(KeyValue.newBuilder().setKey("phone").setStringValue("999-999-9999"))
                .addKeyValues(
                    KeyValue.newBuilder().setKey("first_name").setStringValue("fake_first_name"))
                .addKeyValues(
                    KeyValue.newBuilder().setKey("last_name").setStringValue("fake_last_name"))
                .addKeyValues(KeyValue.newBuilder().setKey("zip_code").setStringValue("99999"))
                .addKeyValues(KeyValue.newBuilder().setKey("country_code").setStringValue("US")))
        .setSchema(
            Schema.newBuilder()
                .addColumns(Column.newBuilder().setColumnName("email"))
                .addColumns(Column.newBuilder().setColumnName("phone"))
                .addColumns(Column.newBuilder().setColumnName("first_name"))
                .addColumns(Column.newBuilder().setColumnName("last_name"))
                .addColumns(Column.newBuilder().setColumnName("zip_code"))
                .addColumns(Column.newBuilder().setColumnName("country_code"))
                .build())
        .build();
  }

  private static class TestDataDestination implements DataDestination {

    private List<String> fileLines;
    private File file;
    private String name;

    private final boolean throwException;

    TestDataDestination() {
      throwException = false;
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
      fileLines = Files.readAllLines(file.toPath());
    }
  }
}
