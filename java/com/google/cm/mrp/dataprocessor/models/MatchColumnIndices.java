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

import com.google.auto.value.AutoOneOf;

/** Holds all the columns indices needed from a datasource in a match condition */
@AutoOneOf(MatchColumnIndices.Kind.class)
public abstract class MatchColumnIndices {
  public enum Kind {
    SINGLE_COLUMN_INDICES,
    COLUMN_GROUP_INDICES
  }

  public abstract Kind getKind();

  public abstract SingleColumnIndices singleColumnIndices();

  public abstract ColumnGroupIndices columnGroupIndices();

  public static MatchColumnIndices ofSingleColumnIndices(SingleColumnIndices singleColumnIndices) {
    return AutoOneOf_MatchColumnIndices.singleColumnIndices(singleColumnIndices);
  }

  public static MatchColumnIndices ofColumnGroupIndices(ColumnGroupIndices columnGroupIndices) {
    return AutoOneOf_MatchColumnIndices.columnGroupIndices(columnGroupIndices);
  }
}
