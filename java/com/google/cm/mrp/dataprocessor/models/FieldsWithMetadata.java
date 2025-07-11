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
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            // If metadata exists, then simply increment
            curMetadata.incrementCount();
            return curMetadata;
          }
        });
  }

  /**
   * Inserts or updates a field into the map, along with associatedData. If field is brand-new, a
   * new mapping is created. If the field already exists, the associatedData is appended.
   */
  public void upsertFieldWithAssociatedData(Field fieldKey, GroupedField associatedData) {
    fieldsMetadataMap.compute(
        fieldKey,
        (field, curMetadata) -> {
          // Create new FieldMetadata if none exists
          if (curMetadata == null) {
            var newResult = new FieldMetadata();
            newResult.addAssociatedData(associatedData);
            return newResult;
          } else {
            // If metadata exists, then append only
            curMetadata.addAssociatedData(associatedData);
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
   * If it exists within the map, gets all associatedData of a GroupedField contained in an
   * AssociatedDataResult object. Otherwise, returns an empty object.
   */
  public AssociatedDataResult getAssociatedDataForField(Field fieldKey) {
    return !fieldsMetadataMap.containsKey(fieldKey)
        ? AssociatedDataResult.builder().build()
        : AssociatedDataResult.builder()
            .setGroupedFields(
                ImmutableSet.copyOf(fieldsMetadataMap.get(fieldKey).getAssociatedData()))
            .setCount(fieldsMetadataMap.get(fieldKey).getCount())
            .build();
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
    private Set<GroupedField> associatedData;

    /** Creates new instance with a count of 0. */
    public FieldMetadata() {
      this.count = 0;
    }

    /** Creates new instance with associatedData. */
    public FieldMetadata(Set<GroupedField> associatedData) {
      this.count = 0;
      this.associatedData = associatedData;
    }

    /** Gets current count. */
    public Integer getCount() {
      return count;
    }

    /** Gets current associatedData, creating a new list if none exist. */
    public Set<GroupedField> getAssociatedData() {
      if (associatedData == null) {
        associatedData = new HashSet<>();
      }
      return associatedData;
    }

    /** Increments current count by 1. */
    public void incrementCount() {
      this.count++;
    }

    /** Adds to current associatedData, creating a new list if none exist. */
    public void addAssociatedData(GroupedField groupedField) {
      if (associatedData == null) {
        associatedData = new HashSet<>();
      }
      incrementCount();
      // TODO(b/418070733): For phase 2, make sure that multiple keys in the group
      // are handled correctly when adding to this set
      associatedData.add(groupedField);
    }
  }

  /**
   * One group of fields corresponds to one row/record of the LookupServer results. Example of a
   * group: `gaia`: `123`, `type`: `E`
   */
  @AutoValue
  public abstract static class GroupedField {

    /** All individual Key-Value fields that make up this GroupedField */
    public abstract ImmutableList<Field> fields();

    /** Returns a new builder. */
    public static Builder builder() {
      return new AutoValue_FieldsWithMetadata_GroupedField.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      /** Create a new {@link GroupedField} from the builder. */
      public abstract GroupedField build();

      /** Set the list of fields. */
      public abstract Builder setFields(List<Field> fields);

      /** Builder for the list of fields. */
      protected abstract ImmutableList.Builder<Field> fieldsBuilder();

      /** Add a field. */
      public Builder addField(Field field) {
        fieldsBuilder().add(field);
        return this;
      }

      /** Add a field. */
      public Builder addField(Field.Builder field) {
        return addField(field.build());
      }
    }
  }

  /** Object containing all the GroupedFields for a given key as well as metadata for that key. */
  @AutoValue
  public abstract static class AssociatedDataResult {

    /**
     * Sum of all GroupedFields attempted to be added while building results. Does not necessarily
     * match the size of groupedFields list since that value does not contain duplicates.
     */
    public abstract Integer count();

    /** List of groupedFields results. Does not contain duplicates */
    public abstract ImmutableSet<GroupedField> groupedFields();

    /** Returns a new builder. */
    public static Builder builder() {
      return new AutoValue_FieldsWithMetadata_AssociatedDataResult.Builder().setCount(0);
    }

    @AutoValue.Builder
    public abstract static class Builder {

      /** Create a new {@link AssociatedDataResult} from the builder. */
      public abstract AssociatedDataResult build();

      /** Set the count of groupedFields encountered */
      public abstract Builder setCount(Integer count);

      /** Set the list of groupedFields. */
      public abstract Builder setGroupedFields(Set<GroupedField> groupedFields);

      /** Builder for the list of groupedFields. */
      protected abstract ImmutableSet.Builder<GroupedField> groupedFieldsBuilder();

      /** Add a groupedFields. */
      public Builder addGroupedField(GroupedField groupedField) {
        groupedFieldsBuilder().add(groupedField);
        return this;
      }

      /** Add a groupedFields. */
      public Builder addGroupedField(GroupedField.Builder groupedField) {
        return addGroupedField(groupedField.build());
      }
    }
  }
}
