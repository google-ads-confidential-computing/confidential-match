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

import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.CondenseMode;
import com.google.cm.mrp.backend.SchemaProto.Schema.OutputColumn;
import com.google.cm.mrp.dataprocessor.models.DataChunk;

/**
 * Formats records from a {@link DataChunk} to the expected output format. This may include
 * ordering, dropping, or condensing columns, as well as the fanning-in of multiple DataRecords that
 * have been previously fanned-out.
 */
public interface DataOutputFormatter {

  /**
   * If the input {@link DataChunk} has DataRecords that were fanned-out, they will be fanned-in. If
   * the MatchConfig specifies a {@link CondenseMode} condensing will be performed. Note: Fanned-out
   * records require condensing. All output will be ordered according to the order specified by the
   * {@link OutputColumn}.
   *
   * @param dataChunk The DataChunk that will be formatted.
   * @return A {@link DataChunk} that has been formatted according to a {@link Schema}.
   */
  DataChunk format(DataChunk dataChunk);

  /**
   * @return The {@link Schema} indicating how DataRecords will be formatted by this class.
   */
  Schema getOutputSchema();
}
