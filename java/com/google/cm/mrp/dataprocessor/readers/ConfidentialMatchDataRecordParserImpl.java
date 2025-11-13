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

package com.google.cm.mrp.dataprocessor.readers;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.COORDINATOR_KEY_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DATA_READER_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_DUPLICATE_METADATA_KEY;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_MATCH_KEY_MISSING_FIELD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_METADATA_CONTAINING_RESTRICTED_ALIAS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PROTO_MISSING_MATCH_KEYS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ROLE_ARN_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_MISSING_IN_RECORD;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl.AliasType.COORDINATOR_KEY;
import static com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl.AliasType.ENCRYPTED_DEK;
import static com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl.AliasType.KEK_URI;
import static com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl.AliasType.ROLE_ARN;
import static com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl.AliasType.WIP_PROVIDER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchKey;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProcessingMetadata;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProtoEncryptionLevel;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns.WrappedKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchCondition;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parser to convert proto data format ConfidentialMatchDataRecord to list DataRecord. */
public final class ConfidentialMatchDataRecordParserImpl
    implements ConfidentialMatchDataRecordParser {
  enum AliasType {
    COORDINATOR_KEY,
    ENCRYPTED_DEK,
    KEK_URI,
    WIP_PROVIDER,
    ROLE_ARN;
  }

  private static final Logger logger =
      LoggerFactory.getLogger(ConfidentialMatchDataRecordParserImpl.class);
  private final ImmutableList<Column> columnList;
  private final ImmutableSet<String> columnNameSet;
  private final ImmutableSet<String> singleMatchAliasSet;
  private final ImmutableMap<String, Integer> columnNameToGroupNumber;
  private final ImmutableMap<String, String> matchAliasToCompositeField;
  private final ImmutableMap<String, AliasType> encryptionKeyAliasesToType;
  private final ImmutableMap<String, ColumnType> columnNameToValueTypeMap;
  private final ImmutableSet<String> metadataNameSet;
  private final SuccessMode successMode;
  private final Optional<WrappedKeyInfo> requestWrappedKeyInfo;

  /**
   * Constructs a ConfidentialMatchDataRecordParser without encryption metadata for parsing {@link
   * ConfidentialMatchDataRecord} into a list of {@link DataRecord}.
   */
  @AssistedInject
  public ConfidentialMatchDataRecordParserImpl(
      @Assisted MatchConfig matchConfig,
      @Assisted Schema schema,
      @Assisted SuccessMode successMode) {
    this.columnList = getColumnList(schema);
    this.columnNameSet = getColumnNameSet(schema);
    this.columnNameToGroupNumber = getColumnNameToGroupNumberMap(schema);
    this.singleMatchAliasSet = getMatchConfigSingleFieldSet(matchConfig);
    this.matchAliasToCompositeField = getMatchConfigCompositeFieldAliasMap(matchConfig);
    this.columnNameToValueTypeMap = getColumnNameToValueTypeMap(schema);
    this.encryptionKeyAliasesToType = ImmutableMap.of();
    this.metadataNameSet = getMetadataNameSet(schema);
    this.successMode = successMode;
    this.requestWrappedKeyInfo = Optional.empty();
  }

  /**
   * Constructs a ConfidentialMatchDataRecordParser with encryption metadata for parsing {@link
   * ConfidentialMatchDataRecord} into a list of {@link DataRecord}.
   */
  @AssistedInject
  public ConfidentialMatchDataRecordParserImpl(
      @Assisted MatchConfig matchConfig,
      @Assisted Schema schema,
      @Assisted SuccessMode successMode,
      @Assisted EncryptionMetadata encryptionMetadata) {
    this.columnList = getColumnList(schema);
    this.columnNameSet = getColumnNameSet(schema);
    this.columnNameToGroupNumber = getColumnNameToGroupNumberMap(schema);
    this.singleMatchAliasSet = getMatchConfigSingleFieldSet(matchConfig);
    this.matchAliasToCompositeField = getMatchConfigCompositeFieldAliasMap(matchConfig);
    this.columnNameToValueTypeMap = getColumnNameToValueTypeMap(schema);
    this.encryptionKeyAliasesToType =
        getEncryptionKeyAliasesToTypeMap(matchConfig.getEncryptionKeyColumns(), encryptionMetadata);
    this.metadataNameSet = getMetadataNameSet(schema);
    this.successMode = successMode;
    this.requestWrappedKeyInfo =
        encryptionMetadata.getEncryptionKeyInfo().hasWrappedKeyInfo()
            ? Optional.of(encryptionMetadata.getEncryptionKeyInfo().getWrappedKeyInfo())
            : Optional.empty();
  }

  /**
   * Parse ConfidentialMatchDataRecord and convert to list of internal DataRecord to be used in the
   * serialized proto reader.
   */
  @Override
  public List<DataRecord> parse(ConfidentialMatchDataRecord cfmDataRecord) {
    String rowId = UUID.randomUUID().toString();
    try {
      return internalParse(cfmDataRecord, rowId);
    } catch (JobProcessorException e) {
      if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
        logger.info(e.getMessage());
        return ImmutableList.of(generateErrorDataRecord(e.getErrorCode(), rowId));
      }
      throw e;
    }
  }

  private List<DataRecord> internalParse(ConfidentialMatchDataRecord cfmDataRecord, String rowId) {
    if (cfmDataRecord.getMatchKeysList().isEmpty()) {
      throw new JobProcessorException(
          "CFM DataRecord does not contain match keys.", PROTO_MISSING_MATCH_KEYS);
    }

    Map<EncryptionKey, Map<String, List<MatchKey>>> singleFieldMatchKeysMap = new HashMap<>();
    Map<EncryptionKey, Map<Integer, List<MatchKey>>> compositeFieldMatchKeysMap = new HashMap<>();

    for (MatchKey matchKey : cfmDataRecord.getMatchKeysList()) {
      EncryptionKey encryptionKey = getEncryptionKey(matchKey, cfmDataRecord);
      if (matchKey.hasField()) {
        populateSingleFieldMap(matchKey, encryptionKey, singleFieldMatchKeysMap);
      } else if (matchKey.hasCompositeField()) {
        populateCompositeFieldMap(matchKey, encryptionKey, compositeFieldMatchKeysMap);
      } else {
        throw new JobProcessorException(
            "CFM DataRecord match key does not contain field or composite field.",
            PROTO_MATCH_KEY_MISSING_FIELD);
      }
    }
    Map<String, KeyValue> metadataMap = validateAndExtractMetadata(cfmDataRecord);
    return buildInternalRecords(
        cfmDataRecord, rowId, singleFieldMatchKeysMap, compositeFieldMatchKeysMap, metadataMap);
  }

  private EncryptionKey getEncryptionKey(
      MatchKey matchKey, ConfidentialMatchDataRecord cfmDataRecord) {
    if (matchKey.hasEncryptionKey() && cfmDataRecord.hasEncryptionKey()) {
      throw new JobProcessorException(
          "Invalid ConfidentialMatchDataRecord with encryption key specified at MatchKey and Row"
              + " level.",
          INVALID_ENCRYPTION_COLUMN);
    }
    return matchKey.hasEncryptionKey()
        ? matchKey.getEncryptionKey()
        : cfmDataRecord.getEncryptionKey();
  }

  private void populateSingleFieldMap(
      MatchKey matchKey,
      EncryptionKey encryptionKey,
      Map<EncryptionKey, Map<String, List<MatchKey>>> singleFieldMatchKeysMap) {
    singleFieldMatchKeysMap
        .computeIfAbsent(encryptionKey, key -> new HashMap<>())
        .computeIfAbsent(matchKey.getField().getKeyValue().getKey(), k -> new ArrayList<>())
        .add(matchKey);
  }

  private void populateCompositeFieldMap(
      MatchKey matchKey,
      EncryptionKey encryptionKey,
      Map<EncryptionKey, Map<Integer, List<MatchKey>>> compositeFieldMatchKeysMap) {
    if (matchKey.getCompositeField().getChildFieldsCount() < 1) {
      throw new JobProcessorException(
          "CFM DataRecord match key composite field does not have any child fields.",
          PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
    }
    getCompositeFieldGroupNumber(matchKey.getCompositeField())
        .ifPresentOrElse(
            groupNumber -> {
              compositeFieldMatchKeysMap
                  .computeIfAbsent(encryptionKey, k -> new HashMap<>())
                  .computeIfAbsent(groupNumber, k -> new ArrayList<>())
                  .add(matchKey);
            },
            () -> {
              throw new JobProcessorException(
                  "CFM DataRecord match key composite field has incorrectly grouped child fields.",
                  PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
            });
  }

  private Map<String, KeyValue> validateAndExtractMetadata(
      ConfidentialMatchDataRecord cfmDataRecord) {
    Map<String, KeyValue> metadataMap;
    try {
      metadataMap =
          cfmDataRecord.getMetadataList().stream()
              .filter(keyValue -> columnNameSet.contains(keyValue.getKey()))
              .collect(Collectors.toMap(KeyValue::getKey, Function.identity()));
    } catch (IllegalStateException e) {
      throw new JobProcessorException(
          "Invalid ConfidentialMatchDataRecord with duplicate metadata key.",
          PROTO_DUPLICATE_METADATA_KEY);
    }

    for (String metadataName : metadataMap.keySet()) {
      if (singleMatchAliasSet.contains(metadataName)
          || matchAliasToCompositeField.containsKey(metadataName)
          || encryptionKeyAliasesToType.containsKey(metadataName)) {
        throw new JobProcessorException(
            String.format("CFM DataRecord metadata contains restricted name: %s", metadataName),
            PROTO_METADATA_CONTAINING_RESTRICTED_ALIAS);
      }
    }
    return metadataMap;
  }

  private List<DataRecord> buildInternalRecords(
      ConfidentialMatchDataRecord cfmDataRecord,
      String rowId,
      Map<EncryptionKey, Map<String, List<MatchKey>>> singleFieldMatchKeysMap,
      Map<EncryptionKey, Map<Integer, List<MatchKey>>> compositeFieldMatchKeysMap,
      Map<String, KeyValue> metadataMap) {

    List<DataRecord> internalRecords = new ArrayList<>();
    HashSet<EncryptionKey> encryptionKeySet =
        getAllEncryptionKeys(singleFieldMatchKeysMap, compositeFieldMatchKeysMap);
    boolean firstDataRecord = true;

    ProtoEncryptionLevel encryptionLevel = getEncryptionLevel(cfmDataRecord);

    for (EncryptionKey encryptionKey : encryptionKeySet) {
      // map of column name to list of match keys containing single fields
      Map<String, List<MatchKey>> columnToSingleFieldMatchKeys =
          singleFieldMatchKeysMap.getOrDefault(encryptionKey, Collections.emptyMap());
      // map of column group number to list of match keys containing composite fields
      Map<Integer, List<MatchKey>> groupNumToCompositeFieldMatchKeys =
          compositeFieldMatchKeysMap.getOrDefault(encryptionKey, Collections.emptyMap());

      int maxIndex =
          Integer.max(
              columnToSingleFieldMatchKeys.values().stream().mapToInt(List::size).max().orElse(0),
              groupNumToCompositeFieldMatchKeys.values().stream()
                  .mapToInt(List::size)
                  .max()
                  .orElse(0));

      for (int fieldIndex = 0; fieldIndex < maxIndex; fieldIndex++) {
        DataRecord dataRecord =
            buildDataRecord(
                encryptionKey,
                rowId,
                columnToSingleFieldMatchKeys,
                groupNumToCompositeFieldMatchKeys,
                metadataMap,
                fieldIndex,
                firstDataRecord,
                encryptionLevel);

        internalRecords.add(dataRecord);
        firstDataRecord = false;
      }
    }
    return internalRecords;
  }

  private DataRecord buildDataRecord(
      EncryptionKey encryptionKey,
      String rowId,
      Map<String, List<MatchKey>> columnToSingleFieldMatchKeys,
      Map<Integer, List<MatchKey>> groupNumToCompositeFieldMatchKeys,
      Map<String, KeyValue> metadataMap,
      int fieldIndex,
      boolean firstDataRecord,
      ProtoEncryptionLevel encryptionLevel) {

    // Map of index of single field in the column list to its metadata.
    Map<Integer, DataRecord.Metadata> singleFieldMetadataMap = new HashMap<>();
    // Map of composite field group number to its metadata.
    Map<Integer, DataRecord.Metadata> compositeFieldMetadataMap = new HashMap<>();

    List<DataRecord.KeyValue> keyValues = new ArrayList<>();
    Optional<JobResultCode> rowLevelErrorCode = Optional.empty();

    for (Column column : columnList) {
      try {
        DataRecord.KeyValue keyValue =
            buildKeyValue(
                column,
                encryptionKey,
                columnToSingleFieldMatchKeys,
                groupNumToCompositeFieldMatchKeys,
                metadataMap,
                fieldIndex,
                rowId,
                firstDataRecord);
        keyValues.add(keyValue);

        // if column corresponds to a match key containing a single field
        if (singleMatchAliasSet.contains(column.getColumnAlias())) {
          populateSingleFieldMetadataMap(
              column, fieldIndex, columnToSingleFieldMatchKeys, singleFieldMetadataMap);
        } else if (matchAliasToCompositeField.containsKey(column.getColumnAlias())) {
          // if column corresponds to a match key containing a composite field
          populateCompositeFieldMetadataMap(
              column, fieldIndex, groupNumToCompositeFieldMatchKeys, compositeFieldMetadataMap);
        }
      } catch (JobProcessorException e) {
        if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS
            && isMissingEncryptionKeyColumnError(e.getErrorCode())) {
          DataRecord.KeyValue defaultKeyValue =
              DataRecord.KeyValue.newBuilder()
                  .setKey(column.getColumnName())
                  .setStringValue("")
                  .build();
          keyValues.add(defaultKeyValue);
          rowLevelErrorCode = Optional.of(e.getErrorCode());
        } else {
          throw e;
        }
      }
    }
    return createDataRecord(
        keyValues,
        rowLevelErrorCode,
        encryptionLevel,
        singleFieldMetadataMap,
        compositeFieldMetadataMap);
  }

  private void populateSingleFieldMetadataMap(
      Column column,
      int fieldIndex,
      Map<String, List<MatchKey>> columnToSingleFieldMatchKeys,
      Map<Integer, DataRecord.Metadata> singleFieldMetadataMap) {

    String columnName = column.getColumnName();

    if (columnToSingleFieldMatchKeys.containsKey(columnName)
        && fieldIndex < columnToSingleFieldMatchKeys.get(columnName).size()) {

      MatchKey matchKey = columnToSingleFieldMatchKeys.get(columnName).get(fieldIndex);

      // If the match key has metadata, add it to the single field metadata map.
      if (matchKey.getMetadataCount() > 0) {
        Integer columnIndex = columnList.indexOf(column);
        if (columnIndex == -1) {
          throw new JobProcessorException(
              String.format("Unknown column index for column: %s", columnName),
              INVALID_INPUT_FILE_ERROR);
        }
        singleFieldMetadataMap.put(
            columnIndex,
            DataRecord.Metadata.newBuilder()
                .addAllMetadata(
                    matchKey.getMetadataList().stream()
                        .map(keyValue -> toDataRecordKeyValue(keyValue))
                        .collect(toImmutableList()))
                .build());
      }
    }
  }

  private void populateCompositeFieldMetadataMap(
      Column column,
      int fieldIndex,
      Map<Integer, List<MatchKey>> groupNumToCompositeFieldMatchKeys,
      Map<Integer, DataRecord.Metadata> compositeFieldMetadataMap) {

    String columnName = column.getColumnName();

    if (columnNameToGroupNumber.containsKey(columnName)
        // We need to add the composite field metadata to the map only once per
        // composite field i.e. once per column group. So check whether the
        // column group number corresponding to the column is already present in
        // the compositeFieldMetadataMap before adding it.
        && !compositeFieldMetadataMap.containsKey(columnNameToGroupNumber.get(columnName))) {
      Integer groupNumber = columnNameToGroupNumber.get(columnName);

      if (groupNumToCompositeFieldMatchKeys.containsKey(groupNumber)
          && fieldIndex < groupNumToCompositeFieldMatchKeys.get(groupNumber).size()) {

        MatchKey matchKey = groupNumToCompositeFieldMatchKeys.get(groupNumber).get(fieldIndex);

        // If the match key has metadata, add it to the composite field metadata map.
        if (matchKey.getMetadataCount() > 0) {
          compositeFieldMetadataMap.put(
              groupNumber,
              DataRecord.Metadata.newBuilder()
                  .addAllMetadata(
                      matchKey.getMetadataList().stream()
                          .map(keyValue -> toDataRecordKeyValue(keyValue))
                          .collect(toImmutableList()))
                  .build());
        }
      }
    }
  }

  private ProtoEncryptionLevel getEncryptionLevel(ConfidentialMatchDataRecord cfmDataRecord) {

    if (cfmDataRecord.getMatchKeysList().stream()
        .anyMatch(matchKey -> matchKey.hasEncryptionKey())) {
      return ProtoEncryptionLevel.MATCH_KEY_LEVEL;
    } else if (cfmDataRecord.hasEncryptionKey()) {
      return ProtoEncryptionLevel.ROW_LEVEL;
    }
    // TODO(b/455659908): Add and use a new enum value for unencrypted data.
    return ProtoEncryptionLevel.UNSPECIFIED_ENCRYPTION_LEVEL;
  }

  private DataRecord.KeyValue buildKeyValue(
      Column column,
      EncryptionKey encryptionKey,
      Map<String, List<MatchKey>> columnToSingleFieldMatchKeys,
      Map<Integer, List<MatchKey>> groupNumToCompositeFieldMatchKeys,
      Map<String, KeyValue> metadataMap,
      int fieldIndex,
      String rowId,
      boolean firstDataRecord) {

    String columnName = column.getColumnName();
    Optional<String> columnAlias =
        column.hasColumnAlias() ? Optional.of(column.getColumnAlias()) : Optional.empty();

    if (metadataNameSet.contains(columnName)) {
      return handleMetadata(columnName, metadataMap, firstDataRecord);
    } else if (columnName.equals(ROW_MARKER_COLUMN_NAME)) {
      return DataRecord.KeyValue.newBuilder()
          .setKey(ROW_MARKER_COLUMN_NAME)
          .setStringValue(rowId)
          .build();
    } else if (columnAlias.isPresent()) {
      if (encryptionKeyAliasesToType.containsKey(columnAlias.get())) {
        return getEncryptionKeyColumn(columnName, encryptionKey, columnAlias.get());
      } else if (singleMatchAliasSet.contains(columnAlias.get())) {
        return handleSingleField(columnName, columnToSingleFieldMatchKeys, fieldIndex);
      } else if (matchAliasToCompositeField.containsKey(columnAlias.get())) {
        return handleCompositeField(columnName, groupNumToCompositeFieldMatchKeys, fieldIndex);
      }
    }
    return DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue("").build();
  }

  private DataRecord.KeyValue handleMetadata(
      String columnName, Map<String, KeyValue> metadataMap, boolean firstDataRecord) {
    if (metadataMap.containsKey(columnName) && firstDataRecord) {
      return convertSchemaColumnKeyValue(metadataMap.get(columnName));
    }
    return DataRecord.KeyValue.newBuilder().setKey(columnName).build();
  }

  private DataRecord.KeyValue handleSingleField(
      String columnName, Map<String, List<MatchKey>> columnToSingleFieldMatchKeys, int fieldIndex) {
    if (columnToSingleFieldMatchKeys.containsKey(columnName)
        && fieldIndex < columnToSingleFieldMatchKeys.get(columnName).size()) {
      return convertSchemaColumnKeyValue(
          columnToSingleFieldMatchKeys.get(columnName).get(fieldIndex).getField().getKeyValue());
    }
    return DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue("").build();
  }

  private DataRecord.KeyValue handleCompositeField(
      String columnName,
      Map<Integer, List<MatchKey>> groupNumToCompositeFieldMatchKeys,
      int fieldIndex) {
    DataRecord.KeyValue defaultKeyValue =
        DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue("").build();
    if (columnNameToGroupNumber.containsKey(columnName)
        && groupNumToCompositeFieldMatchKeys.containsKey(columnNameToGroupNumber.get(columnName))) {
      List<MatchKey> matchKeyList =
          groupNumToCompositeFieldMatchKeys.get(columnNameToGroupNumber.get(columnName));
      return fieldIndex < matchKeyList.size()
          ? matchKeyList.get(fieldIndex).getCompositeField().getChildFieldsList().stream()
              .filter(field -> columnName.equals(field.getKeyValue().getKey()))
              .map(filteredField -> convertSchemaColumnKeyValue(filteredField.getKeyValue()))
              .findAny()
              .orElse(defaultKeyValue)
          : defaultKeyValue;
    }
    return defaultKeyValue;
  }

  private DataRecord createDataRecord(
      List<DataRecord.KeyValue> keyValues,
      Optional<JobResultCode> rowLevelErrorCode,
      ProtoEncryptionLevel encryptionLevel,
      Map<Integer, DataRecord.Metadata> singleFieldMetadataMap,
      Map<Integer, DataRecord.Metadata> compositeFieldMetadataMap) {
    ProcessingMetadata.Builder processingMetadata = ProcessingMetadata.newBuilder();
    if (!encryptionKeyAliasesToType.isEmpty()
        && encryptionLevel != ProtoEncryptionLevel.UNSPECIFIED_ENCRYPTION_LEVEL) {
      processingMetadata.setProtoEncryptionLevel(encryptionLevel);
    }
    DataRecord.Builder dataRecordBuilder =
        DataRecord.newBuilder()
            .addAllKeyValues(keyValues)
            .setProcessingMetadata(processingMetadata.build());
    if (!singleFieldMetadataMap.isEmpty() || !compositeFieldMetadataMap.isEmpty()) {
      DataRecord.FieldLevelMetadata.Builder fieldLevelMetadata =
          DataRecord.FieldLevelMetadata.newBuilder();
      if (!singleFieldMetadataMap.isEmpty()) {
        fieldLevelMetadata.putAllSingleFieldMetadata(singleFieldMetadataMap);
      }
      if (!compositeFieldMetadataMap.isEmpty()) {
        fieldLevelMetadata.putAllCompositeFieldMetadata(compositeFieldMetadataMap);
      }
      dataRecordBuilder.setFieldLevelMetadata(fieldLevelMetadata.build());
    }
    if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS && rowLevelErrorCode.isPresent()) {
      dataRecordBuilder.setErrorCode(rowLevelErrorCode.get());
    }
    return dataRecordBuilder.build();
  }

  private HashSet<EncryptionKey> getAllEncryptionKeys(
      Map<EncryptionKey, Map<String, List<MatchKey>>> singleFieldMatchKeysMap,
      Map<EncryptionKey, Map<Integer, List<MatchKey>>> compositeFieldMatchKeysMap) {
    HashSet<EncryptionKey> allKeys =
        new HashSet<>(singleFieldMatchKeysMap.keySet()); // Start with keys from map1
    allKeys.addAll(compositeFieldMatchKeysMap.keySet()); // Add keys from map2
    return allKeys;
  }

  private ImmutableList<Column> getColumnList(Schema schema) {
    return ImmutableList.copyOf(schema.getColumnsList());
  }

  private ImmutableSet<String> getColumnNameSet(Schema schema) {
    return schema.getColumnsList().stream().map(Column::getColumnName).collect(toImmutableSet());
  }

  ImmutableMap<String, Integer> getColumnNameToGroupNumberMap(Schema schema) {
    return schema.getColumnsList().stream()
        .filter(Column::hasColumnGroup)
        .map(column -> Map.entry(column.getColumnName(), column.getColumnGroup()))
        .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
  }

  private Optional<Integer> getCompositeFieldGroupNumber(CompositeField compositeField) {
    ImmutableSet<Integer> groupNumbers =
        compositeField.getChildFieldsList().stream()
            .map(CompositeChildField::getKeyValue)
            .map(KeyValue::getKey)
            .filter(key -> !key.isEmpty())
            .filter(columnNameToGroupNumber::containsKey)
            .map(columnNameToGroupNumber::get)
            .collect(ImmutableSet.toImmutableSet());
    return groupNumbers.size() == 1 ? groupNumbers.stream().findFirst() : Optional.empty();
  }

  private ImmutableMap<String, ColumnType> getColumnNameToValueTypeMap(Schema schema) {
    return schema.getColumnsList().stream()
        .collect(ImmutableMap.toImmutableMap(Column::getColumnName, Column::getColumnType));
  }

  private ImmutableSet<String> getMatchConfigSingleFieldSet(MatchConfig matchConfig) {
    ImmutableSet.Builder<String> aliasSetBuilder = ImmutableSet.builder();
    for (MatchCondition matchCondition : matchConfig.getMatchConditionsList()) {
      String matchAlias = matchCondition.getDataSource1Column().getColumnAlias();
      if (matchCondition.getDataSource1Column().getColumnsCount() == 1) {
        aliasSetBuilder.add(matchAlias);
      }
    }
    return aliasSetBuilder.build();
  }

  /** Creates a mapping of a field's alias to the alias of its parent composite field. */
  private ImmutableMap<String, String> getMatchConfigCompositeFieldAliasMap(
      MatchConfig matchConfig) {
    ImmutableMap.Builder<String, String> matchAliasToCompositeFieldMap = ImmutableMap.builder();
    for (MatchCondition matchCondition : matchConfig.getMatchConditionsList()) {
      if (matchCondition.getDataSource1Column().getColumnsCount() > 1) {
        for (MatchConfig.Column column : matchCondition.getDataSource1Column().getColumnsList()) {
          matchAliasToCompositeFieldMap.put(
              column.getColumnAlias(), matchCondition.getDataSource1Column().getColumnAlias());
        }
      }
    }
    return matchAliasToCompositeFieldMap.build();
  }

  private ImmutableMap<String, AliasType> getEncryptionKeyAliasesToTypeMap(
      EncryptionKeyColumns encryptionKeyColumns, EncryptionMetadata encryptionMetadata) {
    ImmutableMap.Builder<String, AliasType> encryptionKeyAliasesToTypeMap = ImmutableMap.builder();
    EncryptionKeyInfo encryptionKeyInfo = encryptionMetadata.getEncryptionKeyInfo();
    switch (encryptionKeyInfo.getKeyInfoCase()) {
      case COORDINATOR_KEY_INFO ->
          encryptionKeyAliasesToTypeMap.put(
              encryptionKeyColumns.getCoordinatorKeyColumn().getCoordinatorKeyColumnAlias(),
              COORDINATOR_KEY);
      case WRAPPED_KEY_INFO -> {
        WrappedKeyColumns wrappedKeyColumns = encryptionKeyColumns.getWrappedKeyColumns();
        encryptionKeyAliasesToTypeMap.put(
            wrappedKeyColumns.getEncryptedDekColumnAlias(), ENCRYPTED_DEK);
        encryptionKeyAliasesToTypeMap.put(wrappedKeyColumns.getKekUriColumnAlias(), KEK_URI);
        if (encryptionKeyInfo.getWrappedKeyInfo().hasGcpWrappedKeyInfo()) {
          // Check WIP alias exists in matchConfig
          if (!wrappedKeyColumns.hasGcpWrappedKeyColumns()) {
            String msg = "WIP missing in match config.";
            logger.error(msg);
            throw new JobProcessorException(msg, ENCRYPTION_COLUMNS_CONFIG_ERROR);
          }
          encryptionKeyAliasesToTypeMap.put(
              encryptionKeyColumns
                  .getWrappedKeyColumns()
                  .getGcpWrappedKeyColumns()
                  .getWipProviderAlias(),
              WIP_PROVIDER);
        } else if (encryptionKeyInfo.getWrappedKeyInfo().hasAwsWrappedKeyInfo()) {
          // Check Role ARN alias exists in matchConfig
          if (!wrappedKeyColumns.hasAwsWrappedKeyColumns()) {
            String msg = "Role ARN missing in match config.";
            logger.error(msg);
            throw new JobProcessorException(msg, ENCRYPTION_COLUMNS_CONFIG_ERROR);
          }
          encryptionKeyAliasesToTypeMap.put(
              encryptionKeyColumns
                  .getWrappedKeyColumns()
                  .getAwsWrappedKeyColumns()
                  .getRoleArnAlias(),
              ROLE_ARN);
        }
      }
      default ->
          throw new JobProcessorException(
              "Invalid encryption metadata encryption key info.", INVALID_PARAMETERS);
    }
    return encryptionKeyAliasesToTypeMap.build();
  }

  private DataRecord.KeyValue getEncryptionKeyColumn(
      String columnName, EncryptionKey encryptionKey, String columnAlias) {

    AliasType aliasType = encryptionKeyAliasesToType.get(columnAlias);

    // Handle simplest case
    if (aliasType == COORDINATOR_KEY) {
      return getKeyValueOrThrow(
          columnName,
          /* encryptionKeyValue= */ encryptionKey.getCoordinatorKey().getKeyId(),
          /* errorCode= */ COORDINATOR_KEY_MISSING_IN_RECORD);
    }
    // First check config
    if (requestWrappedKeyInfo.isEmpty()) {
      String msg = "WrappedKeyInfo when reading encryptionKey columns from proto.";
      logger.error(msg);
      throw new JobProcessorException(msg, DATA_READER_CONFIGURATION_ERROR);
    }
    WrappedKeyInfo wrappedKeyInfo = requestWrappedKeyInfo.get();
    return switch (aliasType) {
      case ENCRYPTED_DEK ->
          getKeyValueOrThrow(
              columnName, getEncryptedDek(encryptionKey, wrappedKeyInfo), DEK_MISSING_IN_RECORD);
      case KEK_URI ->
          getKeyValueOrThrow(
              columnName, getKekUri(encryptionKey, wrappedKeyInfo), KEK_MISSING_IN_RECORD);
      case WIP_PROVIDER ->
          getKeyValueOrThrow(
              columnName, encryptionKey.getWrappedKey().getWip(), WIP_MISSING_IN_RECORD);
      case ROLE_ARN ->
          getKeyValueOrThrow(
              columnName,
              encryptionKey.getAwsWrappedKey().getRoleArn(),
              ROLE_ARN_MISSING_IN_RECORD);
      default -> {
        String message = String.format("Invalid key column type: %s", aliasType);
        logger.info(message);
        throw new JobProcessorException(message, INVALID_ENCRYPTION_COLUMN);
      }
    };
  }

  private String getEncryptedDek(EncryptionKey encryptionKey, WrappedKeyInfo wrappedKeyInfo) {
    if (wrappedKeyInfo.hasGcpWrappedKeyInfo()) {
      return encryptionKey.getWrappedKey().getEncryptedDek();
    } else if (wrappedKeyInfo.hasAwsWrappedKeyInfo()) {
      return encryptionKey.getAwsWrappedKey().getEncryptedDek();
    }
    return "";
  }

  private String getKekUri(EncryptionKey encryptionKey, WrappedKeyInfo wrappedKeyInfo) {
    if (wrappedKeyInfo.hasGcpWrappedKeyInfo()) {
      return encryptionKey.getWrappedKey().getKekUri();
    } else if (wrappedKeyInfo.hasAwsWrappedKeyInfo()) {
      return encryptionKey.getAwsWrappedKey().getKekUri();
    }
    return "";
  }

  private DataRecord.KeyValue getKeyValueOrThrow(
      String columnName, String encryptionKeyValue, JobResultCode errorCode) {
    if (encryptionKeyValue.isEmpty()) {
      String msg = "DataRecord missing encryptionKey: " + columnName;
      logger.warn(msg);
      throw new JobProcessorException(msg, errorCode);
    }
    return DataRecord.KeyValue.newBuilder()
        .setKey(columnName)
        .setStringValue(encryptionKeyValue)
        .build();
  }

  private ImmutableSet<String> getMetadataNameSet(Schema schema) {
    return schema.getColumnsList().stream()
        .filter(
            column ->
                !column.hasColumnAlias()
                    || (!singleMatchAliasSet.contains(column.getColumnAlias())
                        && !encryptionKeyAliasesToType.containsKey(column.getColumnAlias())
                        && !matchAliasToCompositeField.containsKey(column.getColumnAlias())
                        && !column.getColumnName().equals(ROW_MARKER_COLUMN_NAME)))
        .map(Column::getColumnName)
        .collect(toImmutableSet());
  }

  private DataRecord.KeyValue convertSchemaColumnKeyValue(KeyValue keyValue) {
    String key = keyValue.getKey();
    DataRecord.KeyValue.Builder keyValueBuilder = DataRecord.KeyValue.newBuilder().setKey(key);
    switch (columnNameToValueTypeMap.get(key)) {
      case INT:
        keyValueBuilder.setIntValue(keyValue.getIntValue());
        break;
      case DOUBLE:
        keyValueBuilder.setDoubleValue(keyValue.getDoubleValue());
        break;
      case BOOL:
        keyValueBuilder.setBoolValue(keyValue.getBoolValue());
        break;
      case STRING:
        keyValueBuilder.setStringValue(keyValue.getStringValue());
        break;
      default:
        throw new JobProcessorException("Invalid key value type.", INVALID_INPUT_FILE_ERROR);
    }
    return keyValueBuilder.build();
  }

  private static DataRecord.KeyValue toDataRecordKeyValue(KeyValue cfmKeyValue) {
    DataRecord.KeyValue.Builder dataRecordKeyValueBuilder =
        DataRecord.KeyValue.newBuilder().setKey(cfmKeyValue.getKey());

    switch (cfmKeyValue.getValueCase()) {
      case STRING_VALUE:
        dataRecordKeyValueBuilder.setStringValue(cfmKeyValue.getStringValue());
        break;
      case INT_VALUE:
        dataRecordKeyValueBuilder.setIntValue(cfmKeyValue.getIntValue());
        break;
      case DOUBLE_VALUE:
        dataRecordKeyValueBuilder.setDoubleValue(cfmKeyValue.getDoubleValue());
        break;
      case BOOL_VALUE:
        dataRecordKeyValueBuilder.setBoolValue(cfmKeyValue.getBoolValue());
        break;
      case VALUE_NOT_SET:
        throw new JobProcessorException("Value not set in key value.", INVALID_INPUT_FILE_ERROR);
    }
    return dataRecordKeyValueBuilder.build();
  }

  private boolean isMissingEncryptionKeyColumnError(JobResultCode jobResultCode) {
    switch (jobResultCode) {
      case COORDINATOR_KEY_MISSING_IN_RECORD:
      case DEK_MISSING_IN_RECORD:
      case KEK_MISSING_IN_RECORD:
      case WIP_MISSING_IN_RECORD:
      case ROLE_ARN_MISSING_IN_RECORD:
        return true;
      default:
        return false;
    }
  }

  private DataRecord generateErrorDataRecord(JobResultCode errorCode, String rowId) {
    DataRecord.Builder errorRecordBuilder = DataRecord.newBuilder().setErrorCode(errorCode);
    for (Column column : columnList) {
      String columnName = column.getColumnName();
      if (columnName.equals(ROW_MARKER_COLUMN_NAME)) {
        errorRecordBuilder.addKeyValues(
            DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue(rowId).build());
      } else {
        errorRecordBuilder.addKeyValues(
            DataRecord.KeyValue.newBuilder().setKey(columnName).build());
      }
    }
    return errorRecordBuilder.build();
  }
}
