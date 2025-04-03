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

import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.writers.CsvDataWriter;
import com.google.cm.mrp.dataprocessor.writers.DataWriter;
import com.google.cm.mrp.dataprocessor.writers.SerializedProtoDataWriter;
import java.util.List;
import javax.inject.Named;

/** Factory interface for {@link DataWriter}. */
public interface DataWriterFactory {
  /** Factory method for constructing {@link CsvDataWriter} objects. */
  @Named("csv")
  DataWriter createCsvDataWriter(
      DataDestination dataDestination,
      String name,
      Schema schema,
      List<String> defaultOutputColumns);

  /** Factory method for constructing {@link SerializedProtoDataWriter} objects. */
  @Named("serializedProto")
  DataWriter createSerializedProtoDataWriter(
      DataDestination dataDestination, String name, Schema schema, MatchConfig matchConfig);
}
