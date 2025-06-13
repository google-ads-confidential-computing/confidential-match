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

import static com.google.cm.mrp.dataprocessor.models.MatchColumnIndices.Kind.COLUMN_GROUP_INDICES;
import static com.google.cm.mrp.dataprocessor.models.MatchColumnIndices.Kind.SINGLE_COLUMN_INDICES;

import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.Column;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchCondition;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.models.ColumnGroupIndices;
import com.google.cm.mrp.dataprocessor.models.MatchColumnIndices;
import com.google.cm.mrp.dataprocessor.models.SingleColumnIndices;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Wrapper class holding utility and creation methods for a MatchColumnIndices list */
public class MatchColumnsList {
  private ImmutableList<MatchColumnIndices> matchColumnsList;

  MatchColumnsList(ImmutableList<MatchColumnIndices> matchColumnsList) {
    this.matchColumnsList = matchColumnsList;
  }

  public ImmutableList<MatchColumnIndices> getList() {
    return matchColumnsList;
  }

  /*
   * Checks if the given column is one of the match columns.
   */
  public boolean isMatchColumn(int columnIndex) {
    for (MatchColumnIndices matchColumns : matchColumnsList) {
      if (matchColumns.getKind().equals(SINGLE_COLUMN_INDICES)) {
        if (matchColumns.singleColumnIndices().indicesList().contains(columnIndex)) {
          return true;
        }
      } else {
        if (matchColumns
            .columnGroupIndices()
            .columnGroupIndicesMultimap()
            .values()
            .contains(columnIndex)) {
          return true;
        }
      }
    }
    return false;
  }

  /*
   * Checks if the given index is part of a multi match condition.
   */
  public boolean isMultiMatchCondition(int columnIndex) {
    for (MatchColumnIndices matchColumns : matchColumnsList) {
      if (matchColumns.getKind().equals(COLUMN_GROUP_INDICES)) {
        if (matchColumns
            .columnGroupIndices()
            .columnGroupIndicesMultimap()
            .values()
            .contains(columnIndex)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Count the expected number of PIIs for each row, based on single or multi column indices */
  public int countPiis() {
    // Need atomic to work inside a lambda
    AtomicInteger piisPerRow = new AtomicInteger();
    matchColumnsList.forEach(
        matchColumnIndices -> {
          if (matchColumnIndices.getKind().equals(SINGLE_COLUMN_INDICES)) {
            piisPerRow.addAndGet(matchColumnIndices.singleColumnIndices().indicesList().size());
          } else {
            piisPerRow.addAndGet(
                matchColumnIndices
                    .columnGroupIndices()
                    .columnGroupIndicesMultimap()
                    .values()
                    .size());
          }
        });
    return piisPerRow.get();
  }

  /**
   * Creates a list of MatchColumnIndices, where the nth element holds the column indices of all the
   * columns with aliases specified in datasource1 of the nth match condition of the match config
   */
  public static MatchColumnsList generateMatchColumnsListForDataSource1(
      Schema schema, MatchConfig matchConfig) {
    ImmutableList.Builder<MatchColumnIndices> columnMatchList = ImmutableList.builder();
    for (MatchCondition matchCondition : matchConfig.getMatchConditionsList()) {
      if (matchCondition.getDataSource1Column().getColumnsCount() == 1) {
        SingleColumnIndices singleColumnIndices =
            getMatchColumnsForSingleColumnMatchCondition(
                schema, matchCondition.getDataSource1Column().getColumnsList());
        columnMatchList.add(MatchColumnIndices.ofSingleColumnIndices(singleColumnIndices));
      } else {
        ColumnGroupIndices columnGroupIndices =
            getMatchColumnsForMultiColumnMatchCondition(
                schema, matchCondition.getDataSource1Column().getColumnsList());
        columnMatchList.add(MatchColumnIndices.ofColumnGroupIndices(columnGroupIndices));
      }
    }
    return new MatchColumnsList(columnMatchList.build());
  }

  /**
   * Returns a SingleColumnIndices with a list of the indices of the columns (in schema) that are
   * listed in the match condition.
   */
  public static SingleColumnIndices getMatchColumnsForSingleColumnMatchCondition(
      Schema schema, List<Column> matchConditionColumnsList) {
    ImmutableList.Builder<Integer> matchColumnIndices = ImmutableList.builder();
    for (Column column : matchConditionColumnsList) {
      for (int i = 0; i < schema.getColumnsCount(); i++) {
        if (schema.getColumns(i).getColumnAlias().equalsIgnoreCase(column.getColumnAlias())) {
          matchColumnIndices.add(i);
        }
      }
    }
    return SingleColumnIndices.create(matchColumnIndices.build());
  }

  /**
   * Creates a ColumnGroupIndices with a map whose key is the column group and value is the list of
   * indices of the columns (from schema) in that column group. Indices list will be sorted by
   * column order.
   */
  public static ColumnGroupIndices getMatchColumnsForMultiColumnMatchCondition(
      Schema schema, List<Column> matchConditioncolumnsList) {
    ListMultimap<Integer, Integer> columnGroupIndices = ArrayListMultimap.create();
    List<Column> matchConditioncolumnsListSorted =
        matchConditioncolumnsList.stream()
            .sorted(Comparator.comparing(Column::getOrder))
            .collect(Collectors.toCollection(ArrayList::new));
    for (Column column : matchConditioncolumnsListSorted) {
      for (int i = 0; i < schema.getColumnsCount(); i++) {
        if (column.getColumnAlias().equalsIgnoreCase(schema.getColumns(i).getColumnAlias())) {
          columnGroupIndices.put(schema.getColumns(i).getColumnGroup(), i);
        }
      }
    }
    return ColumnGroupIndices.create(columnGroupIndices);
  }
}
