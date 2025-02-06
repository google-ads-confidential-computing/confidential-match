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
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Holds a list of indices of all columns in a datasource that have the column alias specified in a
 * single column match condition.
 */
@AutoValue
public abstract class SingleColumnIndices {
  public static SingleColumnIndices create(List<Integer> singleColumns) {
    return new AutoValue_SingleColumnIndices(ImmutableList.copyOf(singleColumns));
  }

  public abstract ImmutableList<Integer> indicesList();
}
