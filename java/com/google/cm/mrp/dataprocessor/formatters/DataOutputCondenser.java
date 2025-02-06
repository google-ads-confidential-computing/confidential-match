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

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import java.util.List;

/**
 * Condenses multiple {@link KeyValue}s into a single KeyValue. A {@link Schema} should always be
 * validated with the DataOutputFormatter validateOutputSchema`method before constructing this
 * class.
 */
public interface DataOutputCondenser {

  /**
   * Condenses KeyValues according to an CondensedColumn specified in the {@link Schema}. A Schema
   * should always be validated with the DataOutputFormatter validateOutputSchema method before
   * constructing this class.
   *
   * @param dataRecords A list of DataRecords that contain KeyValues that should be condensed into a
   *     single KeyValue.
   * @param schema Indicates where KeyValues are located in the DataRecords.
   * @return {@link KeyValue}
   */
  KeyValue condense(List<DataRecord> dataRecords, Schema schema);
}
