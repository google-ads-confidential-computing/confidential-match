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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARTIAL_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.PartialSuccessAttributes;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.CondenseMode;
import com.google.cm.mrp.backend.SchemaProto.Schema.CondensedColumn;
import com.google.cm.mrp.backend.SchemaProto.Schema.OutputColumn;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formats records from a {@link DataChunk} to the expected output format. This may include
 * ordering, dropping, or condensing columns, as well as the fanning-in of multiple DataRecords that
 * have been previously fanned-out.
 */
public final class DataOutputFormatterImpl implements DataOutputFormatter {
  private static final Logger logger = LoggerFactory.getLogger(DataOutputFormatterImpl.class);
  // The final outputSchema constructed from the original job Schema.
  private final Schema outputSchema;
  // A map of where KeyValues should be placed in ordered DataRecords
  private final ImmutableList<String> outputColumns;
  // The name of the CondensedColumn. Serves as the key for the KeyValue of a returned DataRecord.
  private final String condensedResponseColumnName;
  private final Optional<String> recordStatusFieldName;
  private final Optional<DataOutputCondenser> dataOutputCondenser;
  private Optional<ImmutableMap<String, Integer>> columnToIndexMap;

  /**
   * Constructs a DataOutputFormatterImpl for formatting DataChunks needing fan-in or condensing.
   *
   * @param schema The original and unmodified input {@link Schema}.
   * @param dataOutputCondenser An optional {@link DataOutputCondenser} for condensing if required.
   */
  @AssistedInject
  public DataOutputFormatterImpl(
      @Assisted MatchConfig matchConfig,
      @Assisted Schema schema,
      @Assisted Optional<DataOutputCondenser> dataOutputCondenser) {
    // Maps and creates the output schema.
    columnToIndexMap = Optional.empty();
    outputSchema = generateOutputSchema(schema);
    outputColumns = getOutputColumns(outputSchema);
    // Sets up the condenser.
    this.dataOutputCondenser = dataOutputCondenser;
    Optional<CondensedColumn> condensedColumn = findCondensedColumn(schema);
    condensedResponseColumnName = condensedColumn.map(CondensedColumn::getColumnName).orElse("");
    // Captures the record status field name for propagating errors.
    recordStatusFieldName = getRecordStatusFieldName(matchConfig);
  }

  /**
   * @return The {@link Schema} indicating how DataRecords will be formatted by this class.
   */
  public Schema getOutputSchema() {
    return outputSchema;
  }

  /**
   * If the input {@link DataChunk} has DataRecords that were fanned-out, they will be fanned-in. If
   * the Schema specifies a {@link CondenseMode} condensing will be performed. Note: Fanned-out
   * records require condensing. All output will be ordered according to the order specified by the
   * {@link OutputColumn}.
   *
   * @param dataChunk The DataChunk that will be formatted.
   * @return A {@link DataChunk} that has been formatted according to a {@link Schema}.
   */
  public DataChunk format(DataChunk dataChunk) {
    // Group together all fanned-out DataRecords. For DataRecords that were not fanned out, they
    // will exist as a single DataRecord within their grouping.
    List<List<DataRecord>> dataRecordGroups = groupFannedOutDataRecords(dataChunk);

    // Build out the final formatted and ordered DataChunk by fanning in DataRecords when needed,
    // condensing KeyValues into a single KeyValue when needed, and ordering all KeyValues for final
    // output.
    DataChunk.Builder dataChunkBuilder = DataChunk.builder();
    for (List<DataRecord> dataRecordGroup : dataRecordGroups) {
      DataRecord.Builder dataRecordBuilder = DataRecord.newBuilder();
      dataRecordBuilder.setErrorCode(dataRecordGroup.get(0).getErrorCode());
      for (String keyValueName : outputColumns) {
        if (keyValueName.equals(condensedResponseColumnName) && dataOutputCondenser.isPresent()) {
          // Condenses KeyValues from a dataRecordGroup into a single KeyValue if condensing is
          // needed.
          dataRecordBuilder.addKeyValues(
              dataOutputCondenser.get().condense(dataRecordGroup, dataChunk.schema()));
        } else {
          // No condensing needed - Specifically picks the KeyValues specified for output. This is
          // where fan-in occurs if the dataRecordGroup contains multiple records.
          dataRecordBuilder.addKeyValues(getKeyValueByKeyName(dataRecordGroup, keyValueName));
        }
      }
      dataChunkBuilder.addRecord(dataRecordBuilder.build());
    }
    dataChunkBuilder.setInputSchema(dataChunk.inputSchema().orElse(dataChunk.schema()));
    return dataChunkBuilder.setSchema(outputSchema).build();
  }

  /** Returns the final output schema to attach to the returned DataChunk */
  private Schema generateOutputSchema(Schema schema) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schema.getOutputColumnsList().stream()
        .forEachOrdered(
            outputColumn ->
                schemaBuilder.addColumns(
                    Column.newBuilder()
                        .setColumnName(
                            outputColumn.hasColumn()
                                ? outputColumn.getColumn().getColumnName()
                                : outputColumn.getCondensedColumn().getColumnName())));
    return schemaBuilder
        .setDataFormat(schema.getDataFormat())
        .setSkipHeaderRecord(schema.getSkipHeaderRecord())
        .build();
  }

  /**
   * Gets the MatchConfig's recordStatusFieldName specified in the {@link MatchConfig} if it exists.
   */
  private Optional<String> getRecordStatusFieldName(MatchConfig matchConfig) {
    return Optional.of(matchConfig.getSuccessConfig())
        .map(SuccessConfig::getPartialSuccessAttributes)
        .map(PartialSuccessAttributes::getRecordStatusFieldName);
  }

  /** Returns an ordered list of the output column's names. */
  private ImmutableList<String> getOutputColumns(Schema schema) {
    return schema.getColumnsList().stream()
        .map(Column::getColumnName)
        .collect(ImmutableList.toImmutableList());
  }

  /** Returns a map of column names to their index within a schema. */
  private ImmutableMap<String, Integer> getColumnToIndexMap(Schema schema) {
    return IntStream.range(0, schema.getColumnsList().size())
        .boxed()
        .collect(
            Collectors.collectingAndThen(
                Collectors.toMap(
                    index -> schema.getColumnsList().get(index).getColumnName(), index -> index),
                ImmutableMap::copyOf));
  }

  /** Locates the Schema's {@link CondensedColumn}. */
  private Optional<CondensedColumn> findCondensedColumn(Schema schema) {
    for (OutputColumn outputColumn : schema.getOutputColumnsList()) {
      if (outputColumn.hasCondensedColumn()) {
        return Optional.of(outputColumn.getCondensedColumn());
      }
    }
    return Optional.empty();
  }

  /**
   * Flattens a KeyValue from a list of DataRecords by preferring the first KeyValue without an
   * empty value found. A list with only one DataRecord will always return the one KeyValue found.
   * For recordStatusField, prefers to return the errorKeyValue.
   */
  private KeyValue getKeyValueByKeyName(List<DataRecord> dataRecordGroup, String keyValueName) {
    Integer keyValueLocation = columnToIndexMap.get().get(keyValueName);
    boolean isStatusKeyValue =
        recordStatusFieldName.isPresent() && keyValueName.equals(recordStatusFieldName.get());
    for (DataRecord record : dataRecordGroup) {
      KeyValue tempKeyValue = record.getKeyValues(keyValueLocation);
      if (!tempKeyValue.getStringValue().isEmpty()) {
        if (!isStatusKeyValue) {
          return tempKeyValue;
        }
        // Accepts and propagates the first found error.
        try {
          if (SUCCESS != JobResultCode.valueOf(tempKeyValue.getStringValue())) {
            return tempKeyValue;
          }
        } catch (IllegalArgumentException e) {
          String message =
              String.format(
                  "DataOutputFormatter Error: Unknown JobResultCode %s found.",
                  tempKeyValue.getStringValue());
          logger.error(message, e);
          throw new JobProcessorException(message, e, INVALID_PARTIAL_ERROR);
        }
      }
    }
    return KeyValue.newBuilder()
        .setKey(keyValueName)
        .setStringValue(isStatusKeyValue ? SUCCESS.name() : "")
        .build();
  }

  /**
   * Groups together all fanned-out DataRecords by their `ROW_MARKER_COLUMN_NAME`. If DataRecords
   * were never fanned-out, each group will only contain one individual DataRecord.
   */
  private List<List<DataRecord>> groupFannedOutDataRecords(DataChunk dataChunk) {
    if (columnToIndexMap.isEmpty()) {
      columnToIndexMap = Optional.of(getColumnToIndexMap(dataChunk.schema()));
    }

    // Collects DataRecords into dataRecordGroups, either as groups or individual DataRecords.
    List<DataRecord> dataRecords = dataChunk.records();
    List<List<DataRecord>> dataRecordGroups = new ArrayList<>();
    if (columnToIndexMap.get().containsKey(ROW_MARKER_COLUMN_NAME)) {
      // Groups fanned-out records together by row ID
      Map<String, List<DataRecord>> dataRecordGroupsMap = new HashMap<>();
      for (DataRecord dataRecord : dataRecords) {
        List<DataRecord> dataRecordGroup =
            dataRecordGroupsMap.computeIfAbsent(
                dataRecord
                    .getKeyValues(columnToIndexMap.get().get(ROW_MARKER_COLUMN_NAME))
                    .getStringValue(),
                k -> new ArrayList<>());
        dataRecordGroup.add(dataRecord);
      }
      dataRecordGroups.addAll(dataRecordGroupsMap.values());
    } else {
      // Records were not fanned-out so each group will contain only a single DataRecord
      for (DataRecord dataRecord : dataRecords) {
        dataRecordGroups.add(Collections.singletonList(dataRecord));
      }
    }
    return dataRecordGroups;
  }
}
