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

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.readers.DataReader;

/**
 * Interface representing data sources that can be streamed and processed by the {@link
 * DataProcessor}. Streaming means loading and processing one {@link DataChunk} at a time.
 */
public interface StreamDataSource {
  /**
   * Returns the next streaming {@link DataReader}s associated with a blob in this data source in an
   * iterating fashion.
   *
   * @throws JobProcessorException - if errors are encountered reading the streaming {@link
   *     DataReader}s or if an invalid schema is encountered.
   */
  DataReader next();

  /** Returns true if this data source has more streaming {@link DataReader}s. */
  boolean hasNext();

  /** Returns the schema for this data source. */
  Schema getSchema();

  /** Returns the number of streaming {@link DataReader}s associated with this data source. */
  int size();
}
