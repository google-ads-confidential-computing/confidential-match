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

package com.google.cm.mrp.dataprocessor.converters;

import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import java.util.Comparator;

/** Converts Schema-related items */
public final class SchemaConverter {

  /**
   * Converts a given schema item to an array that contains only the column names defined in the
   * schema
   */
  public static String[] convertToColumnNames(Schema schema) {
    return schema.getColumnsList().stream()
        .sorted(Comparator.comparing(Column::getColumnIndex))
        .map(Column::getColumnName)
        .toArray(String[]::new);
  }

  /**
   * Converts a given schema item to an array that contains only the column aliases defined in the
   * schema
   */
  public static String[] convertToColumnAliases(Schema schema) {
    return schema.getColumnsList().stream()
        .sorted(Comparator.comparing(Column::getColumnIndex))
        .map(Column::getColumnAlias)
        .toArray(String[]::new);
  }
}
