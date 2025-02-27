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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.Mode.REDACT;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.Column;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.CompositeColumn;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchCondition;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.DataMatchResult;
import com.google.cm.mrp.dataprocessor.models.KeyValuePair;
import com.google.cm.mrp.dataprocessor.models.MatchColumnIndices;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.dataprocessor.models.SingleColumnIndices;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformer;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete Matcher class for comparing two {@link DataChunk}s and producing match results in a
 * resulting {@link DataChunk}.
 */
public final class DataMatcherImpl implements DataMatcher {

  private static final Logger logger = LoggerFactory.getLogger(DataMatcherImpl.class);

  private final DataRecordTransformerFactory dataRecordTransformerFactory;

  private final MatchConfig matchConfig;
  private final SuccessConfig successConfig;

  /** Constructor for {@link DataMatcherImpl}. */
  @Inject
  public DataMatcherImpl(
      DataRecordTransformerFactory dataRecordTransformerFactory,
      @Assisted MatchConfig matchConfig) {
    this.dataRecordTransformerFactory = dataRecordTransformerFactory;
    this.successConfig = matchConfig.getSuccessConfig();
    if (shouldAppendRecordStatus() && !successConfig.hasPartialSuccessAttributes()) {
      String message =
          "SUCCESS_MODE is ALLOW_PARTIAL_SUCCESS, but partial_success_attributes empty";
      logger.error(message);
      throw new JobProcessorException(message, PARTIAL_SUCCESS_CONFIG_ERROR);
    }
    MatchConfig.Builder matchConfigBuilder = MatchConfig.newBuilder(matchConfig);
    matchConfigBuilder.clearMatchConditions();
    // Need to sort the columns by column order for consistent hashing in the future.
    for (MatchCondition matchCondition : matchConfig.getMatchConditionsList()) {
      List<Column> columnList1 =
          matchCondition.getDataSource1Column().getColumnsList().stream()
              .sorted(Comparator.comparing(Column::getOrder))
              .collect(Collectors.toCollection(ArrayList::new));
      CompositeColumn.Builder compositeColumnBuilder1 =
          CompositeColumn.newBuilder(matchCondition.getDataSource1Column());
      compositeColumnBuilder1.clearColumns();
      compositeColumnBuilder1.addAllColumns(columnList1);

      List<Column> columnList2 =
          matchCondition.getDataSource2Column().getColumnsList().stream()
              .sorted(Comparator.comparing(Column::getOrder))
              .collect(Collectors.toCollection(ArrayList::new));
      CompositeColumn.Builder compositeColumnBuilder2 =
          CompositeColumn.newBuilder(matchCondition.getDataSource2Column());
      compositeColumnBuilder2.clearColumns();
      compositeColumnBuilder2.addAllColumns(columnList2);
      matchConfigBuilder.addMatchConditions(
          MatchCondition.newBuilder(matchCondition)
              .setDataSource1Column(compositeColumnBuilder1)
              .setDataSource2Column(compositeColumnBuilder2));
    }
    this.matchConfig = matchConfigBuilder.build();
  }

  /**
   * This method compares two {@link DataChunk}s using a given configuration and produces match
   * results in a {@link DataChunk}.
   */
  @Override
  @SuppressWarnings("UnstableApiUsage")
  public DataMatchResult match(
      DataChunk dataChunkFromDataSource1, DataChunk dataChunkFromDataSource2) {

    // this size is accurate for REDACT mode logic. For TRANSFORM mode, we will have to change it.
    final ImmutableList.Builder<DataRecord> records =
        ImmutableList.builderWithExpectedSize(dataChunkFromDataSource1.records().size());

    // We can build a HashMap once, and then match against it multiple times for performance.
    final Map<KeyValuePair, Integer> keyValuePairCount =
        buildKeyValuePairCountForDataSource2(dataChunkFromDataSource2);

    MatchColumnsList dataSource1MatchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(
            dataChunkFromDataSource1.schema(), matchConfig);

    DataRecordTransformer transformer =
        dataRecordTransformerFactory.create(matchConfig, dataChunkFromDataSource1.schema());

    // TODO(b/309462161): Add logic to honor output columns as specified in the matchConfig

    // Counts the number of records that contain at least one match
    long numDataRecordsWithMatch = 0L;
    // Counts the number of conditions checked where the record has all required fields present
    var validConditionCheckCounts = new HashMap<String, Long>();
    // Counts the number of conditions that were matched
    var conditionMatchCounts = new HashMap<String, Long>();
    // Counts the number of errored data records from datasource1, grouped by errorCode
    var datasource1ErrorCounts = new HashMap<String, Long>();
    // Counts the number of conditions that were matched in datasource 2
    var datasource2ConditionMatches = new HashMap<String, Long>();

    if (REDACT.equals(matchConfig.getMode())) {
      // Set of KeyValuePairs that we have already processed.  This is used to avoid over counting
      // datasource2ConditionMatches when there are duplicate kvPairs in datasource1.  The values in
      // keyValuePairCount should already include the sum of all the datasource2 matches for a
      // given keyValuePair.
      HashSet<KeyValuePair> processedKvPairs = new HashSet<>();
      for (DataRecord dataRecord : dataChunkFromDataSource1.records()) {
        DataRecord outputRecord;

        if (shouldAppendRecordStatus() && dataRecord.hasErrorCode()) {
          String errorCode = dataRecord.getErrorCode().name();
          outputRecord =
              redact(
                  dataRecord,
                  successConfig.getPartialSuccessAttributes().getRedactErrorWith(),
                  /* redactPredicate= */ dataSource1MatchColumnsList::isMatchColumn,
                  /* recordStatus= */ Optional.of(errorCode));
          datasource1ErrorCounts.merge(errorCode, 1L, Long::sum);
        } else {
          final List<Integer> matchedColumns =
              getMatchedColumns(
                  /* dataRecord= */ transformer.transform(dataRecord),
                  keyValuePairCount,
                  dataSource1MatchColumnsList,
                  conditionMatchCounts,
                  validConditionCheckCounts,
                  datasource2ConditionMatches,
                  processedKvPairs);

          numDataRecordsWithMatch += matchedColumns.isEmpty() ? 0 : 1;

          outputRecord =
              redact(
                  dataRecord,
                  matchConfig.getRedactUnmatchedWith(),
                  /* redactPredicate= */ i ->
                      dataSource1MatchColumnsList.isMatchColumn(i) && !matchedColumns.contains(i),
                  /* recordStatus= */ shouldAppendRecordStatus()
                      ? Optional.of(SUCCESS.name())
                      : Optional.empty());
        }

        if (!isEmpty(outputRecord)) {
          records.add(outputRecord);
        }
      }
    } else {
      // TODO(b/309462754): Add data matching logic for TRANSFORM mode
      throw new UnsupportedOperationException(
          "TRANSFORM match configuration is not supported yet.");
    }
    // TODO: Change the schema in the returned dataChunk for TRANSFORM mode.  The DataSource1 schema
    // can be used for REDACT mode, but this is not the case for TRANSFORM mode. This will require
    // changes in the DataWriter since the schema is not read from this DataChunk
    return DataMatchResult.create(
        DataChunk.builder()
            .setSchema(getUpdatedSchema(dataChunkFromDataSource1.schema()))
            .setRecords(records.build())
            .build(),
        MatchStatistics.create(
            0,
            dataChunkFromDataSource1.records().size(),
            numDataRecordsWithMatch,
            conditionMatchCounts,
            validConditionCheckCounts,
            datasource1ErrorCounts,
            datasource2ConditionMatches));
  }

  // Returns a map where the key is the key value pair (column name, column value) and the value is
  // the number of instances of this key value pair in the data chunk.  The number of instances of
  // each key value pair represents the number of times that data record was matched in datasource
  // 2.
  private Map<KeyValuePair, Integer> buildKeyValuePairCountForDataSource2(
      DataChunk dataChunkFromDataSource2) {
    HashMap<KeyValuePair, Integer> keyValuePairCount = new HashMap<>();
    // HashSet of datasource2 columns (single column match condition) that have already been
    // processed
    HashSet<String> recordedDataSource2ColumnSingle = new HashSet<>();
    // HashSet of datasource2 columns (multi column match condition) that have already been
    // processed
    HashSet<String> recordedDataSource2ColumnMulti = new HashSet<>();
    for (MatchCondition matchCondition : matchConfig.getMatchConditionsList()) {
      if (matchCondition.getDataSource2Column().getColumnsCount() == 1) {
        String columnAlias = matchCondition.getDataSource2Column().getColumns(0).getColumnAlias();
        if (recordedDataSource2ColumnSingle.contains(columnAlias)) {
          continue;
        }
        recordedDataSource2ColumnSingle.add(columnAlias);
        updateKeyValuePairCountForSingleColumnMatchCondition(
            matchCondition, keyValuePairCount, dataChunkFromDataSource2);
      } else {
        String columnAlias = matchCondition.getDataSource2Column().getColumnAlias();
        if (recordedDataSource2ColumnMulti.contains(columnAlias)) {
          continue;
        }
        recordedDataSource2ColumnMulti.add(columnAlias);
        updateKeyValuePairCountForMultiColumnMatchCondition(
            matchCondition, keyValuePairCount, dataChunkFromDataSource2);
      }
    }
    return keyValuePairCount;
  }

  private void updateKeyValuePairCountForSingleColumnMatchCondition(
      MatchCondition singleColumnMatchCondition,
      Map<KeyValuePair, Integer> keyValuePairCount,
      DataChunk dataChunkFromDataSource2) {
    Schema schema = dataChunkFromDataSource2.schema();
    SingleColumnIndices columnIndices =
        MatchColumnsList.getMatchColumnsForSingleColumnMatchCondition(
            schema, singleColumnMatchCondition.getDataSource2Column().getColumnsList());
    for (DataRecord dataRecord : dataChunkFromDataSource2.records()) {
      for (Integer columnIndex : columnIndices.indicesList()) {
        keyValuePairCount.merge(
            KeyValuePair.builder()
                .setKey(
                    singleColumnMatchCondition
                        .getDataSource2Column()
                        .getColumns(0)
                        .getColumnAlias())
                .setValue(dataRecord.getKeyValues(columnIndex).getStringValue())
                .build(),
            1,
            Integer::sum);
      }
    }
  }

  private void updateKeyValuePairCountForMultiColumnMatchCondition(
      MatchCondition multiColumnMatchCondition,
      Map<KeyValuePair, Integer> keyValuePairCount,
      DataChunk dataChunkFromDataSource2) {
    Schema schema = dataChunkFromDataSource2.schema();
    // Create a map whose key is the column group and value is the list of indices of columns
    // (in schema) in that column group.
    ListMultimap<Integer, Integer> columnGroups =
        MatchColumnsList.getMatchColumnsForMultiColumnMatchCondition(
                schema, multiColumnMatchCondition.getDataSource2Column().getColumnsList())
            .columnGroupIndicesMultimap();
    for (DataRecord dataRecord : dataChunkFromDataSource2.records()) {
      for (Collection<Integer> columnIndices : columnGroups.asMap().values()) {
        String combinedValues =
            columnIndices.stream()
                .map(dataRecord::getKeyValues)
                .map(KeyValue::getStringValue)
                .collect(Collectors.joining());
        keyValuePairCount.merge(
            KeyValuePair.builder()
                .setKey(multiColumnMatchCondition.getDataSource2Column().getColumnAlias())
                .setValue(hashString(combinedValues))
                .build(),
            1,
            Integer::sum);
      }
    }
  }

  // Gets all the keys that have a match in keyValuePairSet. Updates matched conditions map.
  private List<Integer> getMatchedColumns(
      DataRecord dataRecord,
      Map<KeyValuePair, Integer> keyValuePairCount,
      MatchColumnsList matchColumnsList,
      Map<String, Long> matchedConditions,
      Map<String, Long> validConditions,
      Map<String, Long> datasource2ConditionMatches,
      HashSet<KeyValuePair> processedKvPairs) {
    List<Integer> matchedColumns = new ArrayList<>();
    for (int i = 0; i < matchConfig.getMatchConditionsCount(); ++i) {
      MatchCondition matchCondition = matchConfig.getMatchConditions(i);
      CompositeColumn dataSource1Column = matchCondition.getDataSource1Column();
      MatchColumnIndices matchColumns = matchColumnsList.getList().get(i);
      List<Integer> matches =
          dataSource1Column.getColumnsCount() == 1
              ? getMatchedColumnsForSingleColumnMatchCondition(
                  matchCondition,
                  dataRecord,
                  keyValuePairCount,
                  matchColumns.singleColumnIndices().indicesList(),
                  validConditions,
                  matchedConditions,
                  datasource2ConditionMatches,
                  processedKvPairs)
              : getMatchedColumnsForMultiColumnMatchCondition(
                  matchCondition,
                  dataRecord,
                  keyValuePairCount,
                  matchColumns.columnGroupIndices().columnGroupIndicesMultimap(),
                  validConditions,
                  matchedConditions,
                  datasource2ConditionMatches,
                  processedKvPairs);
      matchedColumns.addAll(matches);
    }
    return matchedColumns;
  }

  /**
   * Finds and returns all columns that matched using a single column MatchCondition.
   *
   * @param singleColumnMatchCondition a MatchCondition whose DataSource1Column only has one column
   * @param dataRecord the DataRecord to be matched against
   * @param keyValuePairCount the map representing the data records in DataSource2 that the
   *     dataRecord will be matched against
   * @param columnIndices the indices of the columns in the DataChunk schema that correspond to the
   *     column in the DataSource1Column of the singleColumnMatchCondition
   */
  private ImmutableList<Integer> getMatchedColumnsForSingleColumnMatchCondition(
      MatchCondition singleColumnMatchCondition,
      DataRecord dataRecord,
      Map<KeyValuePair, Integer> keyValuePairCount,
      List<Integer> columnIndices,
      Map<String, Long> validConditions,
      Map<String, Long> matchedConditions,
      Map<String, Long> datasource2ConditionMatches,
      HashSet<KeyValuePair> processedKvPairs) {
    ImmutableList.Builder<Integer> matchedColumns = ImmutableList.builder();
    String conditionName = singleColumnMatchCondition.getDataSource1Column().getColumnAlias();
    long numValidMatchAttempts = 0L;
    for (Integer i : columnIndices) {
      KeyValue kv = dataRecord.getKeyValues(i);
      String stringValue = kv.getStringValue();
      numValidMatchAttempts += kv.getStringValue().isEmpty() ? 0 : 1;
      KeyValuePair kvPair =
          KeyValuePair.builder()
              .setKey(singleColumnMatchCondition.getDataSource2Column().getColumnAlias())
              .setValue(stringValue)
              .build();
      int count = keyValuePairCount.getOrDefault(kvPair, 0);
      if (count > 0) {
        matchedColumns.add(i);
        matchedConditions.merge(conditionName, 1L, Long::sum);
        if (processedKvPairs.add(kvPair)) {
          datasource2ConditionMatches.merge(conditionName, (long) count, Long::sum);
        }
      }
    }
    validConditions.merge(conditionName, numValidMatchAttempts, Long::sum);
    return matchedColumns.build();
  }

  /**
   * Finds and returns all columns that matched using a multicolumn MatchCondition.
   *
   * @param multiColumnMatchCondition a MatchCondition whose DataSource1Column has multiple columns
   * @param dataRecord the DataRecord to be matched against
   * @param keyValuePairCount the set representing the data records in DataSource2 that the
   *     dataRecord will be matched against
   * @param columnGroups a map whose values are the list of indices of the columns that have the
   *     same column group. The columns in each group correspond to the columns in the
   *     DataSource1Column of the multiColumnMatchCondition
   */
  private ImmutableList<Integer> getMatchedColumnsForMultiColumnMatchCondition(
      MatchCondition multiColumnMatchCondition,
      DataRecord dataRecord,
      Map<KeyValuePair, Integer> keyValuePairCount,
      ListMultimap<Integer, Integer> columnGroups,
      Map<String, Long> validConditions,
      Map<String, Long> matchedConditions,
      Map<String, Long> datasource2ConditionMatches,
      HashSet<KeyValuePair> processedKvPairs) {
    ImmutableList.Builder<Integer> matchedColumns = ImmutableList.builder();
    String conditionName = multiColumnMatchCondition.getDataSource1Column().getColumnAlias();
    long numValidMatchAttempts = 0L;
    // For every group, append all the values together, hash the result, and look for a match in the
    // keyValuePairSet.  Maps returned by ListMultimap.asMap() have List values with insert ordering
    // preserved, although the method signature doesn't explicitly say so.
    for (Collection<Integer> columnGroupIndices : columnGroups.asMap().values()) {
      // The logic here relies on the fact that the column lists are all already sorted by column
      // order, to ensure the resulting hash value is consistent every time.
      String combinedValues =
          columnGroupIndices.stream()
              .map(dataRecord::getKeyValues)
              .map(KeyValue::getStringValue)
              .collect(Collectors.joining());

      // The corresponding kvPair that would be in DataSource2 (keyValuePairSet) if there was a
      // match.
      KeyValuePair kvPair =
          KeyValuePair.builder()
              .setKey(multiColumnMatchCondition.getDataSource2Column().getColumnAlias())
              .setValue(hashString(combinedValues))
              .build();

      numValidMatchAttempts +=
          columnGroupIndices.stream()
                  .map(dataRecord::getKeyValues)
                  .map(KeyValue::getStringValue)
                  .allMatch(Predicate.not(String::isEmpty))
              ? 1
              : 0;

      int count = keyValuePairCount.getOrDefault(kvPair, 0);
      if (count > 0) {
        // Found a match.
        for (Integer columnIndex : columnGroupIndices) {
          matchedColumns.add(columnIndex);
        }
        matchedConditions.merge(conditionName, 1L, Long::sum);
        if (processedKvPairs.add(kvPair)) {
          datasource2ConditionMatches.merge(conditionName, (long) count, Long::sum);
        }
      }
    }
    validConditions.merge(conditionName, numValidMatchAttempts, Long::sum);
    return matchedColumns.build();
  }

  /**
   * Redacts match columns in a data record that match the predicate.
   *
   * @param dataRecord the data record to be redacted
   * @param redactWith the string to replace redacted fields
   * @param redactPredicate accepts column indices and returns true for redacted columns
   * @param recordStatus an Optional status string to populate the status field
   * @return a new data record with redacted values in correct columns
   */
  private DataRecord redact(
      DataRecord dataRecord,
      String redactWith,
      Predicate<Integer> redactPredicate,
      Optional<String> recordStatus) {
    DataRecord.Builder result = DataRecord.newBuilder();

    for (int i = 0; i < dataRecord.getKeyValuesCount(); i++) {
      KeyValue keyValue = dataRecord.getKeyValues(i);
      String value;
      if (!keyValue.hasStringValue() || keyValue.getStringValue().isBlank()) {
        value = "";
      } else if (redactPredicate.test(i)) {
        value = redactWith;
      } else {
        value = keyValue.getStringValue();
      }
      result.addKeyValues(KeyValue.newBuilder(keyValue).setStringValue(value));
    }
    recordStatus.ifPresent(
        status ->
            result.addKeyValues(
                KeyValue.newBuilder()
                    .setKey(successConfig.getPartialSuccessAttributes().getRecordStatusFieldName())
                    .setStringValue(status)));
    if (dataRecord.hasProcessingMetadata()) {
      result.setProcessingMetadata(dataRecord.getProcessingMetadata());
    }
    return result.build();
  }

  private boolean shouldAppendRecordStatus() {
    return successConfig.getSuccessMode() == SuccessMode.ALLOW_PARTIAL_SUCCESS;
  }

  /*
   * Returns true if all KeyValues in a data record contain blank values (null or "").
   * This is needed to avoid appending blank rows to the MRP output.
   */
  private boolean isEmpty(DataRecord dataRecord) {
    return dataRecord.getKeyValuesList().stream()
        .map(KeyValue::getStringValue)
        .allMatch(String::isBlank);
  }

  private String hashString(String s) {
    return BaseEncoding.base64().encode(sha256().hashBytes(s.getBytes(UTF_8)).asBytes());
  }

  /*
   * Appends record status column to schema if partial success enabled.
   * Otherwise, return original schema.
   */
  private Schema getUpdatedSchema(Schema currentSchema) {
    if (shouldAppendRecordStatus()) {
      String recordStatusColumnName =
          successConfig.getPartialSuccessAttributes().getRecordStatusFieldName();
      Schema.Column recordStatusColumn =
          Schema.Column.newBuilder()
              .setColumnName(recordStatusColumnName)
              .setColumnAlias(recordStatusColumnName)
              .setColumnType(ColumnType.STRING)
              .build();
      return currentSchema.toBuilder().addColumns(recordStatusColumn).build();
    }
    return currentSchema;
  }
}
