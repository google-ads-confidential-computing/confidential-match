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

package com.google.cm.mrp.dataprocessor.formatters;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DATA_OUTPUT_CONDENSER_ERROR;
import static com.google.cm.mrp.backend.SchemaProto.Schema.CondenseMode.CONDENSE_COLUMNS_PROTO;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.CondensedResponseColumnProto.CondensedResponseColumn;
import com.google.cm.mrp.backend.CondensedResponseColumnProto.CondensedResponseColumn.ColumnGroup;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.CondensedColumn;
import com.google.cm.mrp.backend.SchemaProto.Schema.OutputColumn;
import com.google.cm.mrp.backend.SchemaProto.Schema.SubColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.scp.shared.util.Base64Util;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Condenses multiple {@link KeyValue}s into single KeyValue with a value of a serialized and Base64
 * encoded {@link CondensedResponseColumn} proto.
 */
public final class ProtoDataOutputCondenser implements DataOutputCondenser {
  private static final Logger logger = LoggerFactory.getLogger(ProtoDataOutputCondenser.class);
  private final ImmutableMap<String, String> compositeColumnMap;
  private final ImmutableSet<String> condensedSubcolumnNames;
  private final String condensedResponseColumnName;
  private volatile ImmutableMap<Schema, ImmutableList<Integer>> schemaCache;

  /** Constructs a ProtoDataOutputCondenser for condensing fields of DataRecords to a proto. */
  @AssistedInject
  public ProtoDataOutputCondenser(@Assisted Schema schema) {
    // Builds mappings of the condensed column.
    CondensedColumn condensedColumn = getCondensedColumn(schema);
    if (condensedColumn.getCondenseMode() != CONDENSE_COLUMNS_PROTO) {
      String message =
          String.format(
              "Condenser Error: Expected condense mode %s but found %s.",
              CONDENSE_COLUMNS_PROTO, condensedColumn.getCondenseMode());
      logger.error(message);
      throw new JobProcessorException(message, DATA_OUTPUT_CONDENSER_ERROR);
    }
    condensedResponseColumnName = condensedColumn.getColumnName();
    condensedSubcolumnNames = getCondensedSubcolumnNames(condensedColumn);
    compositeColumnMap = getCompositeColumnMap(condensedColumn);
    schemaCache = ImmutableMap.of();
  }

  /**
   * Condenses KeyValues according to an CondensedColumn specified in the Schema. The value of the
   * returned KeyValue is a serialized and Base64 encoded {@link CondensedResponseColumn} proto.
   *
   * @param dataRecords A list of DataRecords that contain KeyValues that should be condensed into a
   *     single KeyValue.
   * @param schema Indicates where KeyValues are located in the DataRecords.
   * @return {@link KeyValue}
   */
  public KeyValue condense(List<DataRecord> dataRecords, Schema schema) {
    ImmutableList<Integer> indices = getCondensedSubcolumnIndices(schema);
    CondensedResponseColumn.Builder condensedResponseColumn = CondensedResponseColumn.newBuilder();
    dataRecords.forEach(
        dataRecord -> {
          // Gathers all ungrouped columns and maps CompositeColumns for an individual record.
          Map<String, List<KeyValue>> compositeColumnGroups = new HashMap<>();
          indices.forEach(
              index -> {
                if (index < 0 || index >= dataRecord.getKeyValuesList().size()) {
                  String message = "Invalid record index caught when condensing.";
                  logger.error(message);
                  throw new JobProcessorException(message, DATA_OUTPUT_CONDENSER_ERROR);
                }
                KeyValue keyValue = dataRecord.getKeyValues(index);
                if (!keyValue.getStringValue().isEmpty()) {
                  // Adds ungrouped columns from the record to the CondensedResponseColumn proto.
                  if (!compositeColumnMap.containsKey(keyValue.getKey())) {
                    condensedResponseColumn.addColumns(
                        CondensedResponseColumn.Column.newBuilder()
                            .setColumn(
                                CondensedResponseColumn.KeyValue.newBuilder()
                                    .setKey(keyValue.getKey())
                                    .setValue(keyValue.getStringValue())
                                    .build())
                            .build());
                  } else {
                    // Maps a CompositeColumn name to the columns it contains.
                    compositeColumnGroups
                        .computeIfAbsent(
                            compositeColumnMap.get(keyValue.getKey()), k -> new ArrayList<>())
                        .add(keyValue);
                  }
                }
              });
          // Adds the mapped CompositeColumn groups from the record to the CondensedResponseColumn
          // proto
          compositeColumnGroups.forEach(
              (groupName, keyValues) -> {
                ColumnGroup.Builder columnGroup = ColumnGroup.newBuilder();
                columnGroup.setName(groupName);
                keyValues.forEach(
                    keyValue ->
                        columnGroup.addSubcolumns(
                            CondensedResponseColumn.KeyValue.newBuilder()
                                .setKey(keyValue.getKey())
                                .setValue(keyValue.getStringValue())
                                .build()));
                condensedResponseColumn.addColumns(
                    CondensedResponseColumn.Column.newBuilder()
                        .setColumnGroup(columnGroup.build())
                        .build());
              });
        });

    return KeyValue.newBuilder()
        .setKey(condensedResponseColumnName)
        .setStringValue(base64Encode(condensedResponseColumn.build()))
        .build();
  }

  /**
   * Manages the schema cache and if outdated, updates the indices cache which contains the location
   * of columns for condensing.
   */
  private ImmutableList<Integer> getCondensedSubcolumnIndices(Schema schema) {
    if (schemaCache.containsKey(schema)) {
      return schemaCache.get(schema);
    }
    Map<String, Integer> keyValueLocationMap =
        IntStream.range(0, schema.getColumnsList().size())
            .boxed()
            .collect(
                Collectors.toMap(
                    index -> schema.getColumnsList().get(index).getColumnName(), index -> index));

    ImmutableList<Integer> indices =
        condensedSubcolumnNames.stream()
            .filter(keyValueLocationMap::containsKey)
            .map(keyValueLocationMap::get)
            .collect(ImmutableList.toImmutableList());

    try {
      schemaCache =
          ImmutableMap.<Schema, ImmutableList<Integer>>builder()
              .put(schema, indices)
              .buildOrThrow();
    } catch (IllegalStateException e) {
      String message = "Condenser Error. Unable to update internal cache.";
      logger.error(message, e);
      throw new JobProcessorException(message, e, DATA_OUTPUT_CONDENSER_ERROR);
    }

    return indices;
  }

  /** Returns a set of all column names to be condensed. */
  private ImmutableSet<String> getCondensedSubcolumnNames(CondensedColumn condensedColumn) {
    return condensedColumn.getSubcolumnsList().stream()
        .flatMap(
            subColumn ->
                subColumn.hasColumn()
                    ? Stream.of(subColumn.getColumn().getColumnName())
                    : subColumn.getCompositeColumn().getColumnsList().stream()
                        .map(Column::getColumnName))
        .collect(ImmutableSet.toImmutableSet());
  }

  /** Locates the output {@link CondensedColumn}. */
  private CondensedColumn getCondensedColumn(Schema schema) {
    for (OutputColumn outputColumn : schema.getOutputColumnsList()) {
      if (outputColumn.hasCondensedColumn()) {
        return outputColumn.getCondensedColumn();
      }
    }
    String message = "Condenser Error: CondensedColumn not found.";
    logger.error(message);
    throw new JobProcessorException(message, DATA_OUTPUT_CONDENSER_ERROR);
  }

  /**
   * Constructs a mapping of column names to the CompositeColumn they belong to. Ungrouped columns
   * are omitted.
   */
  private ImmutableMap<String, String> getCompositeColumnMap(CondensedColumn condensedColumn) {
    return condensedColumn.getSubcolumnsList().stream()
        .filter(SubColumn::hasCompositeColumn)
        .flatMap(
            subColumn ->
                subColumn.getCompositeColumn().getColumnsList().stream()
                    .map(
                        column ->
                            new AbstractMap.SimpleEntry<>(
                                column.getColumnName(),
                                subColumn.getCompositeColumn().getColumnName())))
        .collect(
            ImmutableMap.toImmutableMap(
                AbstractMap.SimpleEntry::getKey,
                AbstractMap.SimpleEntry::getValue,
                (oldValue, newValue) -> oldValue));
  }

  /** Returns a serialized and base64 encoded {@link CondensedResponseColumn}. */
  private static String base64Encode(CondensedResponseColumn condensedResponseColumn) {
    try {
      return Base64Util.toBase64String(condensedResponseColumn.toByteString());
    } catch (Exception e) {
      String message = "Unexpected error during Base64 encoding of CondensedResponseColumn proto.";
      logger.error(message, e);
      throw new JobProcessorException(message, e, DATA_OUTPUT_CONDENSER_ERROR);
    }
  }
}
