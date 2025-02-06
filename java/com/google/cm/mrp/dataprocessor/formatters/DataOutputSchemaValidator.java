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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.SchemaProto.Schema.CondenseMode.CONDENSE_MODE_UNSPECIFIED;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.OutputColumn;
import com.google.cm.mrp.backend.SchemaProto.Schema.SubColumn;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataOutputSchemaValidator {
  /**
   * Validates {@link OutputColumn}s for a {@link Schema}. Throws and error for an incorrectly
   * configured schema.
   *
   * @param schema The schema to evaluate
   */
  public static void validateOutputSchema(Schema schema) {
    if (schema.getOutputColumnsList().isEmpty()
        && schema.getColumnsList().stream().noneMatch(Column::hasNestedSchema)) {
      return;
    }

    Logger logger = LoggerFactory.getLogger(DataOutputFormatter.class);
    if (!hasAtMostOneValidCondensedColumn(schema)) {
      String message = "Invalid Schema. Incorrectly configured CondensedColumn.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_SCHEMA_FILE_ERROR);
    }
    if (!allOutputColumnsAreUnique(schema)) {
      String message = "Invalid Schema. All output column and subcolumn names must be unique.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_SCHEMA_FILE_ERROR);
    }
    if (!hasOutputColumnsIfNestedSchemaExists(schema)) {
      String message = "Invalid Schema. Output column must be defined if a NestedSchema exists.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_SCHEMA_FILE_ERROR);
    }
    if (!nestedSubcolumnsMapToCondensedSubcolumn(schema)) {
      String message =
          "Invalid Schema. All nested schema columns can only be output as part of a"
              + " CondensedColumn.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_SCHEMA_FILE_ERROR);
    }
  }

  /** Verifies that if a CondensedColumn exists, it is configured correctly. */
  private static boolean hasAtMostOneValidCondensedColumn(Schema schema) {
    return schema.getOutputColumnsList().stream()
                .filter(OutputColumn::hasCondensedColumn)
                .limit(2)
                .count()
            <= 1
        && schema.getOutputColumnsList().stream()
            .filter(OutputColumn::hasCondensedColumn)
            .map(OutputColumn::getCondensedColumn)
            .noneMatch(
                condensedColumn -> {
                  return condensedColumn.getCondenseMode() == CONDENSE_MODE_UNSPECIFIED
                      || condensedColumn.getSubcolumnsList().isEmpty();
                });
  }

  /** Verifies that if a nested schema exists, output is defined */
  private static boolean hasOutputColumnsIfNestedSchemaExists(Schema schema) {
    return schema.getColumnsList().stream().anyMatch(Column::hasNestedSchema)
        && !schema.getOutputColumnsList().isEmpty();
  }

  /**
   * Verifies that any nested schema column names do not appear as a standard uncondensed output
   * column.
   */
  private static boolean nestedSubcolumnsMapToCondensedSubcolumn(Schema schema) {
    List<String> nestedSchemaNames =
        schema.getColumnsList().stream()
            .filter(Column::hasNestedSchema)
            .map(Column::getNestedSchema)
            .flatMap(nestedSchema -> nestedSchema.getColumnsList().stream())
            .map(Column::getColumnName)
            .collect(Collectors.toList());
    Set<String> uncondensedOutputColumns =
        schema.getOutputColumnsList().stream()
            .filter(OutputColumn::hasColumn)
            .map(OutputColumn::getColumn)
            .map(Column::getColumnName)
            .collect(Collectors.toSet());
    return nestedSchemaNames.stream().noneMatch(uncondensedOutputColumns::contains);
  }

  /**
   * Verifies that all output column, condensed column, subcolumn, and composite column names are
   * unique.
   */
  private static boolean allOutputColumnsAreUnique(Schema schema) {
    Set<String> uniqueOutputNames = new HashSet<>();
    return schema.getOutputColumnsList().stream()
        .flatMap(DataOutputSchemaValidator::extractColumnNames)
        .allMatch(uniqueOutputNames::add);
  }

  private static Stream<String> extractColumnNames(OutputColumn outputColumn) {
    switch (outputColumn.getColumnTypeCase()) {
      case COLUMN:
        return Stream.of(outputColumn.getColumn().getColumnName());
      case CONDENSED_COLUMN:
        return Stream.concat(
            Stream.of(outputColumn.getCondensedColumn().getColumnName()),
            outputColumn.getCondensedColumn().getSubcolumnsList().stream()
                .flatMap(DataOutputSchemaValidator::extractSubColumnNames));
    }
    return Stream.empty();
  }

  private static Stream<String> extractSubColumnNames(SubColumn subColumn) {
    switch (subColumn.getColumnTypeCase()) {
      case COLUMN:
        return Stream.of(subColumn.getColumn().getColumnName());
      case COMPOSITE_COLUMN:
        return Stream.concat(
            Stream.of(subColumn.getCompositeColumn().getColumnName()),
            subColumn.getCompositeColumn().getColumnsList().stream().map(Column::getColumnName));
    }
    return Stream.empty();
  }
}
