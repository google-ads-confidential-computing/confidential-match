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

package com.google.cm.mrp.dataprocessor.readers;

import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import java.io.Closeable;
import java.util.Iterator;

/**
 * Reader interface for reading from various input sources (files/blobs). Allows iteration for
 * reading one {@link DataChunk} at a time.
 */
public interface DataReader extends Iterator<DataChunk>, Closeable {

  /** Accessor method for reading the name associated with this reader. */
  String getName();

  /** Return a schema definition */
  Schema getSchema();
}
