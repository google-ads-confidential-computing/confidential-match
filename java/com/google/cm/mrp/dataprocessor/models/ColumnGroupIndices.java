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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Holds a map that maps the column group number to the indices of all columns in that column group
 * that have the columns aliases specified in a multicolumn match condition. Datasources may have
 * multiple columns with the same alias, so column groups are used to indicate which columns should
 * be grouped together.
 */
@AutoValue
public abstract class ColumnGroupIndices {

  // Creates a ColumnGroupIndices object, preserving the insertion order of the given ListMultimap.
  public static ColumnGroupIndices create(
      ListMultimap<Integer, Integer> groupToIndicesListMultiap) {
    return new AutoValue_ColumnGroupIndices(
        ImmutableListMultimap.copyOf(groupToIndicesListMultiap));
  }

  public abstract ImmutableListMultimap<Integer, Integer> columnGroupIndicesMultimap();
}
