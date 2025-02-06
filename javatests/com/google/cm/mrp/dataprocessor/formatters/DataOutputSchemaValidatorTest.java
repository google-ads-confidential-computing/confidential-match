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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataOutputSchemaValidatorTest {
  @Test
  public void dataOutputSchemaValidator_missingCondenseModeSchema() throws Exception {
    Schema schema = getSchema("testdata/invalid_output_schema_missing_condense_mode.json");

    var ex =
        Assert.assertThrows(
            JobProcessorException.class,
            () -> DataOutputSchemaValidator.validateOutputSchema(schema));
    assertThat(ex.getMessage())
        .isEqualTo("Invalid Schema. Incorrectly configured CondensedColumn.");
  }

  @Test
  public void dataOutputSchemaValidator_multipleCondenseColumnsSchema() throws Exception {
    Schema schema = getSchema("testdata/invalid_output_schema_multiple_condense_columns.json");

    var ex =
        Assert.assertThrows(
            JobProcessorException.class,
            () -> DataOutputSchemaValidator.validateOutputSchema(schema));
    assertThat(ex.getMessage())
        .isEqualTo("Invalid Schema. Incorrectly configured CondensedColumn.");
  }

  @Test
  public void dataOutputSchemaValidator_missingCondenseSubcolumns() throws Exception {
    Schema schema = getSchema("testdata/invalid_output_schema_missing_condense_subcolumns.json");

    var ex =
        Assert.assertThrows(
            JobProcessorException.class,
            () -> DataOutputSchemaValidator.validateOutputSchema(schema));
    assertThat(ex.getMessage())
        .isEqualTo("Invalid Schema. Incorrectly configured CondensedColumn.");
  }

  @Test
  public void dataOutputSchemaValidator_nestedSchemaRequiresCondensing() throws Exception {
    Schema schema = getSchema("testdata/invalid_output_schema_nesting_requires_condensing.json");

    var ex =
        Assert.assertThrows(
            JobProcessorException.class,
            () -> DataOutputSchemaValidator.validateOutputSchema(schema));
    assertThat(ex.getMessage())
        .isEqualTo(
            "Invalid Schema. All nested schema columns can only be output as part of a"
                + " CondensedColumn.");
  }

  @Test
  public void dataOutputSchemaValidator_duplicatedColumnNames() throws Exception {
    Schema schema = getSchema("testdata/invalid_output_schema_duplicate_column_names.json");

    var ex =
        Assert.assertThrows(
            JobProcessorException.class,
            () -> DataOutputSchemaValidator.validateOutputSchema(schema));
    assertThat(ex.getMessage())
        .isEqualTo("Invalid Schema. All output column and subcolumn names must be unique.");
  }

  @Test
  public void dataOutputSchemaValidator_nestedSchemaRequiresOutputColumns() throws Exception {
    Schema schema =
        getSchema("testdata/invalid_output_schema_nested_column_missing_output_columns.json");

    var ex =
        Assert.assertThrows(
            JobProcessorException.class,
            () -> DataOutputSchemaValidator.validateOutputSchema(schema));
    assertThat(ex.getMessage())
        .isEqualTo("Invalid Schema. Output column must be defined if a NestedSchema exists.");
  }

  @Test
  public void dataOutputSchemaValidator_correctOutputSchema() throws Exception {
    Schema schema = getSchema("testdata/nested_data_output_formatter_schema.json");
    DataOutputSchemaValidator.validateOutputSchema(schema);
  }

  private Schema getSchema(String path) throws Exception {
    return ProtoUtils.getProtoFromJson(
        Resources.toString(Objects.requireNonNull(getClass().getResource(path)), UTF_8),
        Schema.class);
  }
}
