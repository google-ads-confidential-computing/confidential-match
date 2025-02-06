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

import static java.util.Collections.emptyMap;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Simple data object to hold match statistics. */
@AutoValue
public abstract class MatchStatistics {

  private static final MatchStatistics EMPTY =
      create(0, 0L, 0L, emptyMap(), emptyMap(), emptyMap(), emptyMap());

  /** Creates a new instance. */
  public static MatchStatistics create(
      int numFiles,
      long numDataRecords,
      long numDataRecordsWithMatch,
      Map<String, Long> conditionMatches,
      Map<String, Long> validConditionChecks,
      Map<String, Long> datasource1Errors,
      Map<String, Long> datasource2ConditionMatches) {
    return new AutoValue_MatchStatistics(
        numFiles,
        numDataRecords,
        numDataRecordsWithMatch,
        ImmutableMap.copyOf(conditionMatches),
        ImmutableMap.copyOf(validConditionChecks),
        ImmutableMap.copyOf(datasource1Errors),
        ImmutableMap.copyOf(datasource2ConditionMatches));
  }

  /** Creates a default instance with no values set. Maps are empty and non-null. */
  public static MatchStatistics emptyInstance() {
    return EMPTY;
  }

  /** Number of files. */
  public abstract int numFiles();

  /** Number of data records. */
  public abstract long numDataRecords();

  /** Number of data records that have at least one match. */
  public abstract long numDataRecordsWithMatch();

  /** Datasource1 match conditions mapping of column name to valid match count. */
  public abstract ImmutableMap<String, Long> conditionMatches();

  /** Datasource1 match conditions mapping of column name to times matching criteria was met. */
  public abstract ImmutableMap<String, Long> validConditionChecks();

  /** Datasource1 errors mapping errorCode to record count. */
  public abstract ImmutableMap<String, Long> datasource1Errors();

  /** Datasource2 match conditions mapping of column name to valid match count. */
  public abstract ImmutableMap<String, Long> datasource2ConditionMatches();
}
