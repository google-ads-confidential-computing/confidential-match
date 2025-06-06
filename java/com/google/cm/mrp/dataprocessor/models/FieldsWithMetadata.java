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

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a map where the key is a {@link Field} within a {@link DataRecord}. The value of the
 * map is {@link FieldMetadata} corresponding to metadata associated with this Field.
 */
public class FieldsWithMetadata {

  /** Underlying map of Fields to metadata */
  private final Map<Field, FieldMetadata> fieldsMetadataMap;

  /** Creates new instance with an empty metadata map */
  public FieldsWithMetadata() {
    this.fieldsMetadataMap = new HashMap<>();
  }

  /**
   * Inserts or updates a field into the map. If field is brand-new, a new mapping is created. If
   * the field already exists, its count is updated.
   */
  public void upsertField(Field fieldKey) {
    fieldsMetadataMap.compute(
        fieldKey,
        (field, curMetadata) -> {
          // Create new FieldMetadata if none exists
          if (curMetadata == null) {
            var newResult = new FieldMetadata();
            newResult.incrementCount();
            return newResult;
          } else {
            // If results exist, then simply increment
            curMetadata.incrementCount();
            return curMetadata;
          }
        });
  }

  /** Gets the total count of a Field, if it exists within the map. Otherwise, returns 0 */
  public int getCountForField(Field fieldKey) {
    return !fieldsMetadataMap.containsKey(fieldKey)
        ? 0
        : fieldsMetadataMap.get(fieldKey).getCount();
  }

  /**
   * Encapsulates stats and associated data tied to a specific Field. Meant be mutable to allow
   * iterating through a list of DataRecords.
   */
  public static final class FieldMetadata {

    /** Defines the number of times the data record is found within a {@link DataChunk}. */
    private int count;

    /**
     * Associated data found with the given data record. Key is the field name of the data and Value
     * is the actual value. Only populated if {@link com.google.cm.mrp.backend.ModeProto.Mode} of
     * the job is JOIN.
     */
    private List<Field> associatedData;

    /** Creates new instance with a count of 0. */
    public FieldMetadata() {
      this.count = 0;
    }

    /** Creates new instance with associatedData. */
    public FieldMetadata(List<Field> associatedData) {
      this.count = 0;
      this.associatedData = associatedData;
    }

    /** Gets current count. */
    public Integer getCount() {
      return count;
    }

    /** Gets current associatedData, creating a new list if none exist. */
    public List<Field> getAssociatedData() {
      if (associatedData == null) {
        associatedData = new ArrayList<>();
      }
      return associatedData;
    }

    /** Increments current count by 1. */
    public void incrementCount() {
      this.count++;
    }

    /** Adds to current associatedData, creating a new list if none exist. */
    public void addAssociatedData(Field field) {
      if (associatedData == null) {
        associatedData = new ArrayList<>();
      }
      incrementCount();
      associatedData.add(field);
    }
  }
}
