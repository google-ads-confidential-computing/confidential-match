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

package com.google.cm.mrp.dataprocessor.models;

import com.google.auto.value.AutoValue;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

/** Encapsulates a unit of data. */
@AutoValue
public abstract class DataChunk {

  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_DataChunk.Builder();
  }

  /** Records in this data chunk. */
  public abstract ImmutableList<DataRecord> records();

  /** Schema of the records in this data chunk. */
  public abstract Schema schema();

  /** Populated if encrypted records exist with {@link DataRecordEncryptionColumns}. */
  public abstract Optional<DataRecordEncryptionColumns> encryptionColumns();

  /** Orignal input schema. */
  public abstract Optional<Schema> inputSchema();

  /** Builder for {@link DataChunk}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Create a new {@link DataChunk} from the builder. */
    public abstract DataChunk build();

    /** Set the list of data records. */
    public abstract Builder setRecords(List<DataRecord> records);

    /** Builder for the list of data records. */
    protected abstract ImmutableList.Builder<DataRecord> recordsBuilder();

    /** Add a data record. */
    public Builder addRecord(DataRecord record) {
      recordsBuilder().add(record);
      return this;
    }

    /** Add a data record. */
    public Builder addRecord(DataRecord.Builder record) {
      return addRecord(record.build());
    }

    /** Set the schema. */
    public abstract Builder setSchema(Schema schema);

    /** Set the input schema. */
    public abstract Builder setInputSchema(Schema schema);

    /** Set the input schema. */
    public abstract Builder setInputSchema(Optional<Schema> schema);

    /** Set the {@link DataRecordEncryptionColumns} if dataRecords were originally encrypted. */
    public abstract Builder setEncryptionColumns(DataRecordEncryptionColumns encryptionColumns);

    /** Set the {@link DataRecordEncryptionColumns} if dataRecords were originally encrypted. */
    public abstract Builder setEncryptionColumns(
        Optional<DataRecordEncryptionColumns> encryptionColumns);
  }
}
