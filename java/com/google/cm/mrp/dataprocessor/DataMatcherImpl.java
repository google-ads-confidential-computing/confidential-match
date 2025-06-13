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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DATA_MATCHER_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_DATA_SOURCE_JOIN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_MODE_ERROR;
import static com.google.cm.mrp.backend.ModeProto.Mode.JOIN;
import static com.google.cm.mrp.backend.ModeProto.Mode.REDACT;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.FieldMatches;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.CompositeFieldMatchedOutput;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.MatchedOutputField;
import com.google.cm.mrp.backend.FieldMatchProto.FieldMatch.SingleFieldMatchedOutput;
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
import com.google.cm.mrp.dataprocessor.models.Field;
import com.google.cm.mrp.dataprocessor.models.FieldsWithMetadata;
import com.google.cm.mrp.dataprocessor.models.MatchColumnIndices;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.dataprocessor.models.SingleColumnIndices;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformer;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerFactory;
import com.google.cm.mrp.models.JobParameters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
  private final JobParameters jobParameters;
  private final SuccessConfig successConfig;

  /** Constructor for {@link DataMatcherImpl}. */
  @Inject
  public DataMatcherImpl(
      DataRecordTransformerFactory dataRecordTransformerFactory,
      @Assisted MatchConfig matchConfig,
      @Assisted JobParameters jobParameters) {
    this.dataRecordTransformerFactory = dataRecordTransformerFactory;
    this.jobParameters = jobParameters;
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
    MatchColumnsList dataSource1MatchColumnsList =
        MatchColumnsList.generateMatchColumnsListForDataSource1(
            dataChunkFromDataSource1.schema(), matchConfig);
    DataRecordTransformer transformer =
        dataRecordTransformerFactory.create(
            matchConfig, dataChunkFromDataSource1.schema(), jobParameters);

    // For JOIN mode. Get the index where to find the associated data in dataSource2
    SingleColumnIndices dataSource2AssociatedDataIndices =
        getFieldIndicesForAliases(dataChunkFromDataSource2.schema(), getAssociatedDataFieldNames());

    // Collects all fields from dataSource2 at once, so they can be looked easily
    // and matched multiple times for performance.
    final FieldsWithMetadata dataSource2Fields =
        buildDataSource2FieldsWithMetadata(
            dataChunkFromDataSource2, dataSource2AssociatedDataIndices);

    // For JOIN mode. Map to lookup the column group of a field by its index
    ImmutableMap<Integer, Integer> dataSource1IndexToGroupNumberMap =
        getIndexToGroupNumberMap(dataChunkFromDataSource1.schema(), dataSource1MatchColumnsList);

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

    final ImmutableList.Builder<DataRecord> outputRecords =
        ImmutableList.builderWithExpectedSize(dataChunkFromDataSource1.records().size());
    // Set of fields that we have already processed. This is used to avoid over counting
    // datasource2ConditionMatches when there are duplicate fields in datasource1.  The values in
    // dataSource2Fields should already include the sum of all the datasource2
    // matches for a given fields.
    HashSet<Field> processedFields = new HashSet<>();
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
        final List<FieldMatch> matchedFields =
            getMatchedFields(
                /* dataRecord= */ transformer.transform(dataRecord),
                dataSource2Fields,
                dataSource1MatchColumnsList,
                conditionMatchCounts,
                validConditionCheckCounts,
                datasource2ConditionMatches,
                processedFields);

        numDataRecordsWithMatch += matchedFields.isEmpty() ? 0 : 1;

        if (REDACT == jobParameters.mode()) {
          outputRecord =
              redact(
                  dataRecord,
                  matchConfig.getModeConfigs().getRedactModeConfig().getRedactUnmatchedWith(),
                  /* redactPredicate= */ i ->
                      dataSource1MatchColumnsList.isMatchColumn(i)
                          && matchedFields.stream()
                              .noneMatch(field -> field.hasIndex() && field.getIndex() == i),
                  /* recordStatus= */ shouldAppendRecordStatus()
                      ? Optional.of(SUCCESS.name())
                      : Optional.empty());
        } else if (JOIN == jobParameters.mode()) {
          outputRecord =
              redactAndJoin(
                  dataRecord,
                  matchConfig.getModeConfigs().getJoinModeConfig().getRedactUnmatchedWith(),
                  dataSource1MatchColumnsList,
                  dataSource1IndexToGroupNumberMap,
                  matchedFields,
                  /* recordStatus= */ shouldAppendRecordStatus()
                      ? Optional.of(SUCCESS.name())
                      : Optional.empty());
        } else {
          throw new JobProcessorException(
              "Unknown mode: " + jobParameters.mode(), UNSUPPORTED_MODE_ERROR);
        }
      }

      if (!isEmpty(outputRecord)) {
        outputRecords.add(outputRecord);
      }
    }
    return DataMatchResult.create(
        DataChunk.builder()
            .setSchema(getUpdatedSchema(dataChunkFromDataSource1.schema()))
            .setRecords(outputRecords.build())
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

  /**
   * Iterates through all the {@link DataRecord}s in DataSource2, extracts each field (or
   * compositeField) result as well its relevant metadata (such as the count of a field within the
   * DataSource). Returns a {@link FieldsWithMetadata} mapping each field to its metadata.
   */
  private FieldsWithMetadata buildDataSource2FieldsWithMetadata(
      DataChunk dataChunkFromDataSource2, SingleColumnIndices associatedDataIndices) {
    FieldsWithMetadata fieldsWithMetadata = new FieldsWithMetadata();
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
        addDataSource2SingleFieldMatchConditionResults(
            fieldsWithMetadata, matchCondition, dataChunkFromDataSource2, associatedDataIndices);
      } else {
        String columnAlias = matchCondition.getDataSource2Column().getColumnAlias();
        if (recordedDataSource2ColumnMulti.contains(columnAlias)) {
          continue;
        }
        recordedDataSource2ColumnMulti.add(columnAlias);
        addDataSource2MultiColumnMatchConditionResults(
            fieldsWithMetadata, matchCondition, dataChunkFromDataSource2, associatedDataIndices);
      }
    }
    return fieldsWithMetadata;
  }

  /**
   * Extracts all the singleColumnMatchCondition fields from dataSource2 and adds them to
   * fieldsWithMetadata. SingleColumnMatchCondition (defined at the match config) fields are those
   * that consist of only 1 dataSource field to find a match. For REDACT mode, simply adds the
   * key-value pair that makes up the field to the fieldsWithMetadata result. For JOIN mode, adds
   * this pair as well as any associatedData found in the data record.
   *
   * @param fieldsWithMetadata Stores all extracted fields from dataSource2
   * @param singleColumnMatchCondition MatchCondition definitions from matchConfig that only contain
   *     singleColumn MatchConditions
   * @param dataChunkFromDataSource2 DataChunk with all the records from dataSource2
   * @param associatedDataIndices Indices where to find associatedData for each dataRecord in
   *     dataSource2
   */
  private void addDataSource2SingleFieldMatchConditionResults(
      FieldsWithMetadata fieldsWithMetadata,
      MatchCondition singleColumnMatchCondition,
      DataChunk dataChunkFromDataSource2,
      SingleColumnIndices associatedDataIndices) {
    Schema schema = dataChunkFromDataSource2.schema();
    SingleColumnIndices columnIndices =
        MatchColumnsList.getMatchColumnsForSingleColumnMatchCondition(
            schema, singleColumnMatchCondition.getDataSource2Column().getColumnsList());
    if (REDACT == jobParameters.mode()) {
      for (DataRecord dataRecord : dataChunkFromDataSource2.records()) {
        for (Integer columnIndex : columnIndices.indicesList()) {
          fieldsWithMetadata.upsertField(
              Field.builder()
                  .setKey(
                      singleColumnMatchCondition
                          .getDataSource2Column()
                          .getColumns(0)
                          .getColumnAlias())
                  .setValue(dataRecord.getKeyValues(columnIndex).getStringValue())
                  .build());
        }
      }
    } else if (JOIN == jobParameters.mode()) {
      // Validate matchCondition only has 1 column.  The current design does not allow to have
      // multiple match fields (ie multiple pii) per record, since that makes it ambiguous which
      // associated
      // data belongs to which field. If there is only one pii column, then it is simple to say
      // that all the associatedData in the record map it.
      if (columnIndices.indicesList().size() != 1) {
        String msg =
            "Join mode requires DataSource2 to contain one match condition column per record";
        throw new JobProcessorException(msg, INVALID_DATA_SOURCE_JOIN);
      }
      Integer piiTypeIndex = columnIndices.indicesList().get(0);
      // Iterate through each dataRecord. Get the matched field first
      for (DataRecord dataRecord : dataChunkFromDataSource2.records()) {
        // represents the field that will be used to match (ie the PII type)
        var fieldKey =
            Field.builder()
                .setKey(
                    singleColumnMatchCondition
                        .getDataSource2Column()
                        .getColumns(0)
                        .getColumnAlias())
                .setValue(dataRecord.getKeyValues(piiTypeIndex).getStringValue())
                .build();
        // Go through all the associatedData and add it to the Fields to be returned
        for (Integer associatedDataIndex : associatedDataIndices.indicesList()) {
          fieldsWithMetadata.upsertFieldWithAssociatedData(
              fieldKey,
              Field.builder()
                  .setKey((dataRecord.getKeyValues(associatedDataIndex).getKey()))
                  .setValue(dataRecord.getKeyValues(associatedDataIndex).getStringValue())
                  .build());
        }
      }
    } else {
      throw new JobProcessorException(
          "Unknown mode: " + jobParameters.mode(), UNSUPPORTED_MODE_ERROR);
    }
  }

  /**
   * Extracts all the multiColumnMatchCondition fields from dataSource2 and adds them to
   * fieldsWithMetadata. MultiColumnMatchCondition (defined at the match config) fields are those
   * that consist more than 1 dataSource field to find a match. For REDACT mode, concats and hashes
   * all the field values and adds the result to the fieldsWithMetadata result. For JOIN mode,
   * performs the same operation and also adds any associatedData found in the data record.
   *
   * @param fieldsWithMetadata Stores all extracted fields from dataSource2
   * @param multiColumnMatchCondition MatchCondition definitions from matchConfig that only contain
   *     multiColum MatchConditions
   * @param dataChunkFromDataSource2 DataChunk with all the records from dataSource2
   * @param associatedDataIndices Indices where to find associatedData for each dataRecord in
   *     dataSource2
   */
  private void addDataSource2MultiColumnMatchConditionResults(
      FieldsWithMetadata fieldsWithMetadata,
      MatchCondition multiColumnMatchCondition,
      DataChunk dataChunkFromDataSource2,
      SingleColumnIndices associatedDataIndices) {
    Schema schema = dataChunkFromDataSource2.schema();
    // Create a map whose key is the column group and value is the list of indices of columns
    // (in schema) in that column group.
    ListMultimap<Integer, Integer> columnGroups =
        MatchColumnsList.getMatchColumnsForMultiColumnMatchCondition(
                schema, multiColumnMatchCondition.getDataSource2Column().getColumnsList())
            .columnGroupIndicesMultimap();
    if (REDACT == jobParameters.mode()) {
      for (DataRecord dataRecord : dataChunkFromDataSource2.records()) {
        for (Collection<Integer> columnIndices : columnGroups.asMap().values()) {
          String combinedValues =
              columnIndices.stream()
                  .map(dataRecord::getKeyValues)
                  .map(KeyValue::getStringValue)
                  .collect(Collectors.joining());
          fieldsWithMetadata.upsertField(
              Field.builder()
                  .setKey(multiColumnMatchCondition.getDataSource2Column().getColumnAlias())
                  .setValue(hashString(combinedValues))
                  .build());
        }
      }
    } else if (JOIN == jobParameters.mode()) {
      if (columnGroups.keySet().size() != 1) {
        String msg =
            "Join mode requires DataSource2 to contain only one match condition columnGroup per"
                + " record";
        throw new JobProcessorException(msg, INVALID_DATA_SOURCE_JOIN);
      }
      Integer columnGroup =
          columnGroups.keySet().stream()
              .findAny()
              .orElseThrow(
                  () -> {
                    String msg = "Invalid dataSource2 column group";
                    logger.error(msg);
                    throw new JobProcessorException(msg, DATA_MATCHER_CONFIG_ERROR);
                  });
      // Iterate through each dataRecord. Get the matched field first
      for (DataRecord dataRecord : dataChunkFromDataSource2.records()) {
        String combinedValues =
            columnGroups.get(columnGroup).stream()
                .map(dataRecord::getKeyValues)
                .map(KeyValue::getStringValue)
                .collect(Collectors.joining());
        // represents the field that will be used to match (ie the PII type)
        var fieldKey =
            Field.builder()
                .setKey(multiColumnMatchCondition.getDataSource2Column().getColumnAlias())
                .setValue(hashString(combinedValues))
                .build();
        // Go through all the associatedData and add it to the Fields to be returned
        for (Integer associatedDataIndex : associatedDataIndices.indicesList()) {
          fieldsWithMetadata.upsertFieldWithAssociatedData(
              fieldKey,
              Field.builder()
                  .setKey((dataRecord.getKeyValues(associatedDataIndex).getKey()))
                  .setValue(dataRecord.getKeyValues(associatedDataIndex).getStringValue())
                  .build());
        }
      }
    } else {
      throw new JobProcessorException(
          "Unknown mode: " + jobParameters.mode(), UNSUPPORTED_MODE_ERROR);
    }
  }

  // Gets all the keys that have a match in dataSource2Fields. Updates matched conditions map.
  private List<FieldMatch> getMatchedFields(
      DataRecord dataRecord,
      FieldsWithMetadata dataSource2fields,
      MatchColumnsList matchColumnsList,
      Map<String, Long> matchedConditions,
      Map<String, Long> validConditions,
      Map<String, Long> datasource2ConditionMatches,
      HashSet<Field> processedFields) {
    List<FieldMatch> matchedFields = new ArrayList<>();
    for (int i = 0; i < matchConfig.getMatchConditionsCount(); ++i) {
      MatchCondition matchCondition = matchConfig.getMatchConditions(i);
      CompositeColumn dataSource1Column = matchCondition.getDataSource1Column();
      MatchColumnIndices matchColumns = matchColumnsList.getList().get(i);
      List<FieldMatch> matches =
          dataSource1Column.getColumnsCount() == 1
              ? getMatchedFieldsForSingleColumnMatchCondition(
                  matchCondition,
                  dataRecord,
                  dataSource2fields,
                  matchColumns.singleColumnIndices().indicesList(),
                  validConditions,
                  matchedConditions,
                  datasource2ConditionMatches,
                  processedFields)
              : getMatchedColumnsForMultiColumnMatchCondition(
                  matchCondition,
                  dataRecord,
                  dataSource2fields,
                  matchColumns.columnGroupIndices().columnGroupIndicesMultimap(),
                  validConditions,
                  matchedConditions,
                  datasource2ConditionMatches,
                  processedFields);
      matchedFields.addAll(matches);
    }
    return matchedFields;
  }

  /**
   * Finds and returns all fields that matched using a single column MatchCondition.
   *
   * @param singleColumnMatchCondition a MatchCondition whose DataSource1Column only has one column
   * @param dataRecord the DataRecord to be matched against
   * @param dataSource2Fields all the fields from DataSource2 that the dataRecord will be matched
   *     against
   * @param dataSource1columnIndices the indices of the columns in the DataChunk schema that
   *     correspond to the column in the DataSource1Column of the singleColumnMatchCondition
   */
  private ImmutableList<FieldMatch> getMatchedFieldsForSingleColumnMatchCondition(
      MatchCondition singleColumnMatchCondition,
      DataRecord dataRecord,
      FieldsWithMetadata dataSource2Fields,
      List<Integer> dataSource1columnIndices,
      Map<String, Long> validConditions,
      Map<String, Long> matchedConditions,
      Map<String, Long> datasource2ConditionMatches,
      HashSet<Field> processedFields) {
    ImmutableList.Builder<FieldMatch> matchedFields = ImmutableList.builder();
    String conditionName = singleColumnMatchCondition.getDataSource1Column().getColumnAlias();
    long numValidMatchAttempts = 0L;
    for (Integer i : dataSource1columnIndices) {
      KeyValue kv = dataRecord.getKeyValues(i);
      String stringValue = kv.getStringValue();
      numValidMatchAttempts += kv.getStringValue().isEmpty() ? 0 : 1;
      Field fieldKey =
          Field.builder()
              .setKey(singleColumnMatchCondition.getDataSource2Column().getColumnAlias())
              .setValue(stringValue)
              .build();
      if (REDACT == jobParameters.mode()) {
        int matchesCount = dataSource2Fields.getCountForField(fieldKey);
        if (matchesCount > 0) {
          matchedFields.add(FieldMatch.newBuilder().setIndex(i).build());
          matchedConditions.merge(conditionName, 1L, Long::sum);
          if (processedFields.add(fieldKey)) {
            datasource2ConditionMatches.merge(conditionName, (long) matchesCount, Long::sum);
          }
        }
      } else if (JOIN == jobParameters.mode()) {
        // Get all associatedData found for the field
        ImmutableList<Field> associatedData = dataSource2Fields.getAssociatedDataForField(fieldKey);
        if (!associatedData.isEmpty()) {
          // Convert to MatchedOutputField
          ImmutableList<MatchedOutputField> matchedOutputFields =
              associatedData.stream()
                  .map(
                      data ->
                          MatchedOutputField.newBuilder()
                              .setKey(data.key())
                              .setValue(data.value())
                              .build())
                  .collect(ImmutableList.toImmutableList());
          // Save to output list with its index within the data record as the key
          matchedFields.add(
              FieldMatch.newBuilder()
                  .setSingleFieldMatchedOutput(
                      SingleFieldMatchedOutput.newBuilder()
                          .setIndex(i)
                          .addAllMatchedOutputFields(matchedOutputFields))
                  .build());
          matchedConditions.merge(conditionName, 1L, Long::sum);
          if (processedFields.add(fieldKey)) {
            datasource2ConditionMatches.merge(
                conditionName, (long) associatedData.size(), Long::sum);
          }
        }
      } else {
        throw new JobProcessorException(
            "Unknown mode: " + jobParameters.mode(), UNSUPPORTED_MODE_ERROR);
      }
    }
    validConditions.merge(conditionName, numValidMatchAttempts, Long::sum);
    return matchedFields.build();
  }

  /**
   * Finds and returns all fields that matched using a multicolumn MatchCondition.
   *
   * @param multiColumnMatchCondition a MatchCondition whose DataSource1Column has multiple columns
   * @param dataRecord the DataRecord to be matched against
   * @param dataSource2Fields all the fields from DataSource2 that the dataRecord will be matched
   *     against
   * @param dataSource1columnGroups a map whose values are the list of indices of the columns that
   *     have the same column group. The columns in each group correspond to the columns in the
   *     DataSource1Column of the multiColumnMatchCondition
   */
  private ImmutableList<FieldMatch> getMatchedColumnsForMultiColumnMatchCondition(
      MatchCondition multiColumnMatchCondition,
      DataRecord dataRecord,
      FieldsWithMetadata dataSource2Fields,
      ListMultimap<Integer, Integer> dataSource1columnGroups,
      Map<String, Long> validConditions,
      Map<String, Long> matchedConditions,
      Map<String, Long> datasource2ConditionMatches,
      HashSet<Field> processedFields) {
    ImmutableList.Builder<FieldMatch> matchedFields = ImmutableList.builder();
    String conditionName = multiColumnMatchCondition.getDataSource1Column().getColumnAlias();
    long numValidMatchAttempts = 0L;
    // For every group, append all the values together, hash the result, and look for a match in the
    // dataSource2Fields. Maps returned by ListMultimap.asMap() have List values with insert
    // ordering preserved, although the method signature doesn't explicitly say so.
    // Entry key is the column group number, entry value are all the indices within that column
    // group
    for (Entry<Integer, Collection<Integer>> columnGroupIndexEntry :
        dataSource1columnGroups.asMap().entrySet()) {
      // The logic here relies on the fact that the column lists are all already sorted by column
      // order, to ensure the resulting hash value is consistent every time.
      String combinedValues =
          columnGroupIndexEntry.getValue().stream()
              .map(dataRecord::getKeyValues)
              .map(KeyValue::getStringValue)
              .collect(Collectors.joining());

      // The corresponding field that would be in DataSource2 if there was a match.
      Field fieldKey =
          Field.builder()
              .setKey(multiColumnMatchCondition.getDataSource2Column().getColumnAlias())
              .setValue(hashString(combinedValues))
              .build();
      if (REDACT == jobParameters.mode()) {
        int matchesCount = dataSource2Fields.getCountForField(fieldKey);

        // This variable counts the number of matching attempts that occur and are considered
        // valid.
        // An invalid match attempt is one where every column is empty, but it is still possible
        // to match when only some columns are empty. The combined values variable will be empty
        // when every column value is empty, so match attempt validity is checked using this. An
        // attempt is also considered valid if there was a match, which acts as a failsafe to avoid
        // reporting more matches than attempts.
        numValidMatchAttempts += matchesCount == 0 && combinedValues.isEmpty() ? 0 : 1;

        if (matchesCount > 0) {
          // Found a match.
          for (Integer columnIndex : columnGroupIndexEntry.getValue()) {
            matchedFields.add(FieldMatch.newBuilder().setIndex(columnIndex).build());
          }
          matchedConditions.merge(conditionName, 1L, Long::sum);
          if (processedFields.add(fieldKey)) {
            datasource2ConditionMatches.merge(conditionName, (long) matchesCount, Long::sum);
          }
        }
      } else if (JOIN == jobParameters.mode()) {
        // get all associatedData found for this field
        ImmutableList<Field> associatedData = dataSource2Fields.getAssociatedDataForField(fieldKey);
        numValidMatchAttempts += associatedData.isEmpty() && combinedValues.isEmpty() ? 0 : 1;

        // Found a match.
        if (!associatedData.isEmpty()) {
          // Convert associatedData to MatchedOutputField
          ImmutableList<MatchedOutputField> matchedOutputFields =
              associatedData.stream()
                  .map(
                      data ->
                          MatchedOutputField.newBuilder()
                              .setKey(data.key())
                              .setValue(data.value())
                              .build())
                  .collect(ImmutableList.toImmutableList());
          // Save to output list where key is the colum group number
          matchedFields.add(
              FieldMatch.newBuilder()
                  .setCompositeFieldMatchedOutput(
                      CompositeFieldMatchedOutput.newBuilder()
                          .setColumnGroup(columnGroupIndexEntry.getKey())
                          .addAllMatchedOutputFields(matchedOutputFields))
                  .build());
          matchedConditions.merge(conditionName, 1L, Long::sum);
          if (processedFields.add(fieldKey)) {
            datasource2ConditionMatches.merge(
                conditionName, (long) associatedData.size(), Long::sum);
          }
        }
      } else {
        throw new JobProcessorException(
            "Unknown mode: " + jobParameters.mode(), UNSUPPORTED_MODE_ERROR);
      }
    }
    validConditions.merge(conditionName, numValidMatchAttempts, Long::sum);
    return matchedFields.build();
  }

  /**
   * Redacts match fields in a data record that match the predicate.
   *
   * @param dataRecord the data record to be redacted
   * @param redactWith the string to replace redacted fields
   * @param redactPredicate accepts column indices and returns true for redacted fields
   * @param recordStatus an Optional status string to populate the status field
   * @return a new data record with redacted values
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

  /**
   * Redacts match fields and includes matchedOutputFields in a data record
   *
   * @param dataRecord the data record to be processed
   * @param redactWith the string to replace redacted fields
   * @param dataSource1MatchColumnsList list of match conditions from dataSource1
   * @param dataSource1CompositeIndexToGroupNumberMap map of composite column groups where the key
   *     is the index of a compositeField, and the value is the column group number of that field
   * @param dataSource2MatchedFields fields that were matched from dataSource2
   * @param recordStatus an Optional status string to populate the status field
   * @return a new data record with redacted values in correct columns
   */
  private DataRecord redactAndJoin(
      DataRecord dataRecord,
      String redactWith,
      MatchColumnsList dataSource1MatchColumnsList,
      ImmutableMap<Integer, Integer> dataSource1CompositeIndexToGroupNumberMap,
      List<FieldMatch> dataSource2MatchedFields,
      Optional<String> recordStatus) {
    DataRecord.Builder result = DataRecord.newBuilder();
    // Mapping of dataSource1 field index -> FieldMatch
    Map<Integer, FieldMatch> matchedSingleFieldMap = new HashMap<>();
    // Mapping of dataSource1 1 field group number -> FieldMatch
    Map<Integer, FieldMatch> matchedCompositeFieldMap = new HashMap<>();

    for (int idx = 0; idx < dataRecord.getKeyValuesCount(); idx++) {
      KeyValue dataSource1Field = dataRecord.getKeyValues(idx);
      Optional<FieldMatch> match = Optional.empty();
      // holds value to output
      String value;
      // Make idx final so it can be used in lambdas
      int dataSource1FieldIndex = idx;
      // Get the groupNumber for this field (if it exists)
      Optional<Integer> dataSource1GroupNumber =
          Optional.ofNullable(dataSource1CompositeIndexToGroupNumberMap.get(dataSource1FieldIndex));

      // If blank, value is also blank
      if (!dataSource1Field.hasStringValue() || dataSource1Field.getStringValue().isBlank()) {
        value = "";
        // If it's not a match condition field, then return original value
      } else if (!dataSource1MatchColumnsList.isMatchColumn(idx)) {
        value = dataSource1Field.getStringValue();
      } else {
        // iterate through all dataSource2 field to look for a match
        match =
            dataSource2MatchedFields.stream()
                .filter(
                    field -> {
                      // if field is a single field, then simply check its index matches that of
                      // dataSource1
                      if (field.hasSingleFieldMatchedOutput()) {
                        return field.getSingleFieldMatchedOutput().getIndex()
                            == dataSource1FieldIndex;
                      }
                      // if field is composite, then check if the group number of the potential
                      // match matches the groupNumber from dataSource1
                      else if (field.hasCompositeFieldMatchedOutput()) {
                        int dataSource2GroupNumber =
                            field.getCompositeFieldMatchedOutput().getColumnGroup();
                        return dataSource1GroupNumber.isPresent()
                            && dataSource1GroupNumber.get() == dataSource2GroupNumber;
                      } else {
                        String msg = "FieldMatch missing joinMode field during redactAndJoin";
                        logger.error(msg);
                        throw new JobProcessorException(msg, DATA_MATCHER_CONFIG_ERROR);
                      }
                    })
                .findAny();

        value = match.isPresent() ? dataSource1Field.getStringValue() : redactWith;
      }

      result.addKeyValues(KeyValue.newBuilder(dataSource1Field).setStringValue(value));
      match.ifPresent(
          matchedField -> {
            // For single fields, simply add to map using the index of dataSource1 field as the key
            if (matchedField.hasSingleFieldMatchedOutput()) {
              matchedSingleFieldMap.put(dataSource1FieldIndex, matchedField);
            }
            // For composite fields, add to map using the groupNumber of dataSource1 composite field
            // as the key
            else if (matchedField.hasCompositeFieldMatchedOutput()
                && dataSource1GroupNumber.isPresent()) {
              matchedCompositeFieldMap.putIfAbsent(dataSource1GroupNumber.get(), matchedField);
            }
          });
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
    // Add fields to be joined, if any
    var joinFieldsBuilder = FieldMatches.newBuilder();
    if (!matchedSingleFieldMap.isEmpty()) {
      joinFieldsBuilder.putAllSingleFieldRecordMatches(matchedSingleFieldMap);
    }
    if (!matchedCompositeFieldMap.isEmpty()) {
      joinFieldsBuilder.putAllCompositeFieldRecordMatches(matchedCompositeFieldMap);
    }
    result.setJoinFields(joinFieldsBuilder.build());
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

  /* Get associatedData field names to search for in a data source. Only for Join mode */
  private List<String> getAssociatedDataFieldNames() {
    if (jobParameters.mode() != JOIN) {
      return ImmutableList.of();
    } else {
      if (!matchConfig.getModeConfigs().hasJoinModeConfig()) {
        String message =
            String.format(
                "Application %s does have Join mode configurations",
                matchConfig.getApplicationId());
        logger.warn(message);
        throw new JobProcessorException(message, UNSUPPORTED_MODE_ERROR);
      }
      return matchConfig.getModeConfigs().getJoinModeConfig().getJoinFieldsList();
    }
  }

  /*
   * Returns a SingleColumnIndices with the indices of the fields (in the schema) that match the aliasList parameter
   */
  public static SingleColumnIndices getFieldIndicesForAliases(
      Schema schema, List<String> aliasList) {
    ImmutableList.Builder<Integer> indices = ImmutableList.builder();
    for (String alias : aliasList) {
      for (int i = 0; i < schema.getColumnsCount(); i++) {
        if (schema.getColumns(i).getColumnAlias().equalsIgnoreCase(alias)) {
          indices.add(i);
        }
      }
    }
    return SingleColumnIndices.create(indices.build());
  }

  /**
   * Builds a map of column index to column group number for all CompositeColumns found in the
   * schema. The column index and column groups are taken from the schema while the match config is
   * used to know if the column is part of CompositeColumns
   */
  private ImmutableMap<Integer, Integer> getIndexToGroupNumberMap(
      Schema schema, MatchColumnsList matchColumnsList) {
    return IntStream.range(0, schema.getColumnsCount())
        .boxed()
        .filter(matchColumnsList::isMultiMatchCondition)
        .collect(
            toImmutableMap(Function.identity(), idx -> schema.getColumns(idx).getColumnGroup()));
  }
}
