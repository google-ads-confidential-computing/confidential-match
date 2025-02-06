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

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.common.collect.ImmutableList;
import java.util.Optional;

/** Formats a {@link DataRecord} based on predefined {@link Schema} */
public interface DataSourceFormatter {

  /**
   * Formats a {@link DataRecord} based on the formatted {@link Schema} stored in this formatter.
   */
  ImmutableList<DataRecord> format(DataRecord dataRecord);

  /** Gets the formatted schema that the DataSourceFormatter formats the data record into. */
  Schema getFormattedSchema();

  /** Gets the {@link DataRecordEncryptionColumns} for the formatted data records */
  Optional<DataRecordEncryptionColumns> getDataRecordEncryptionColumns();
}
