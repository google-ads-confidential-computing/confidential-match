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

    Map<EncryptionKey, Map<String, List<Field>>> singleFieldMap = new HashMap<>();
    Map<EncryptionKey, Map<Integer, List<CompositeField>>> compositeFieldMap = new HashMap<>();

    for (MatchKey matchKey : cfmDataRecord.getMatchKeysList()) {
      EncryptionKey encryptionKey = getEncryptionKey(matchKey, cfmDataRecord);
      populateFieldMaps(matchKey, encryptionKey, singleFieldMap, compositeFieldMap);
    }
    Map<String, KeyValue> metadataMap = validateAndExtractMetadata(cfmDataRecord);
    return buildInternalRecords(
        cfmDataRecord, rowId, singleFieldMap, compositeFieldMap, metadataMap);
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

  private void populateFieldMaps(
      MatchKey matchKey,
      EncryptionKey encryptionKey,
      Map<EncryptionKey, Map<String, List<Field>>> singleFieldMap,
      Map<EncryptionKey, Map<Integer, List<CompositeField>>> compositeFieldMap) {
    if (matchKey.hasField()) {
      singleFieldMap
          .computeIfAbsent(encryptionKey, key -> new HashMap<>())
          .computeIfAbsent(matchKey.getField().getKeyValue().getKey(), k -> new ArrayList<>())
          .add(matchKey.getField());
    } else if (matchKey.hasCompositeField()) {
      processCompositeField(matchKey, encryptionKey, compositeFieldMap);
    } else {
      throw new JobProcessorException(
          "CFM DataRecord match key does not contain field or composite field.",
          PROTO_MATCH_KEY_MISSING_FIELD);
    }
  }

  private void processCompositeField(
      MatchKey matchKey,
      EncryptionKey encryptionKey,
      Map<EncryptionKey, Map<Integer, List<CompositeField>>> compositeFieldMap) {
    if (matchKey.getCompositeField().getChildFieldsCount() < 1) {
      throw new JobProcessorException(
          "CFM DataRecord match key composite field does not have any child fields.",
          PROTO_MATCH_KEY_HAS_BAD_CHILD_FIELDS);
    }
    getCompositeFieldGroupNumber(matchKey.getCompositeField())
        .ifPresentOrElse(
            groupNumber -> {
              compositeFieldMap
                  .computeIfAbsent(encryptionKey, k -> new HashMap<>())
                  .computeIfAbsent(groupNumber, k -> new ArrayList<>())
                  .add(matchKey.getCompositeField());
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
      Map<EncryptionKey, Map<String, List<Field>>> singleFieldMap,
      Map<EncryptionKey, Map<Integer, List<CompositeField>>> compositeFieldMap,
      Map<String, KeyValue> metadataMap) {

    List<DataRecord> internalRecords = new ArrayList<>();
    HashSet<EncryptionKey> encryptionKeySet =
        getAllEncryptionKeys(singleFieldMap, compositeFieldMap);
    boolean firstDataRecord = true;

    ProtoEncryptionLevel encryptionLevel = getEncryptionLevel(cfmDataRecord);

    for (EncryptionKey encryptionKey : encryptionKeySet) {
      Map<String, List<Field>> protoSingleFields =
          singleFieldMap.getOrDefault(encryptionKey, Collections.emptyMap());
      Map<Integer, List<CompositeField>> protoCompositeFields =
          compositeFieldMap.getOrDefault(encryptionKey, Collections.emptyMap());

      int maxIndex =
          Integer.max(
              protoSingleFields.values().stream().mapToInt(List::size).max().orElse(0),
              protoCompositeFields.values().stream().mapToInt(List::size).max().orElse(0));

      for (int fieldIndex = 0; fieldIndex < maxIndex; fieldIndex++) {
        List<DataRecord.KeyValue> keyValues = new ArrayList<>();
        Optional<JobResultCode> rowLevelErrorCode = Optional.empty();

        for (Column column : columnList) {
          try {
            DataRecord.KeyValue keyValue =
                buildKeyValue(
                    column,
                    encryptionKey,
                    protoSingleFields,
                    protoCompositeFields,
                    metadataMap,
                    fieldIndex,
                    rowId,
                    firstDataRecord);
            keyValues.add(keyValue);
          } catch (JobProcessorException e) {
            if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS
                && isMissingEncryptionKeyColumnError(e.getErrorCode())) {
              keyValues.add(
                  DataRecord.KeyValue.newBuilder()
                      .setKey(column.getColumnName())
                      .setStringValue("")
                      .build());
              rowLevelErrorCode = Optional.of(e.getErrorCode());
            } else {
              throw e;
            }
          }
        }
        internalRecords.add(createDataRecord(keyValues, rowLevelErrorCode, encryptionLevel));
        firstDataRecord = false;
      }
    }
    return internalRecords;
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
      Map<String, List<Field>> protoSingleFields,
      Map<Integer, List<CompositeField>> protoCompositeFields,
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
        return handleSingleField(columnName, protoSingleFields, fieldIndex);
      } else if (matchAliasToCompositeField.containsKey(columnAlias.get())) {
        return handleCompositeField(columnName, protoCompositeFields, fieldIndex);
      }
    }
    return DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue("").build();
  }

  private DataRecord.KeyValue handleMetadata(
      String columnName, Map<String, KeyValue> metadataMap, boolean firstDataRecord) {
    if (metadataMap.containsKey(columnName) && firstDataRecord) {
      return convertKeyValue(metadataMap.get(columnName));
    }
    return DataRecord.KeyValue.newBuilder().setKey(columnName).build();
  }

  private DataRecord.KeyValue handleSingleField(
      String columnName, Map<String, List<Field>> protoSingleFields, int fieldIndex) {
    if (protoSingleFields.containsKey(columnName)
        && fieldIndex < protoSingleFields.get(columnName).size()) {
      return convertKeyValue(protoSingleFields.get(columnName).get(fieldIndex).getKeyValue());
    }
    return DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue("").build();
  }

  private DataRecord.KeyValue handleCompositeField(
      String columnName, Map<Integer, List<CompositeField>> protoCompositeFields, int fieldIndex) {
    DataRecord.KeyValue defaultKeyValue =
        DataRecord.KeyValue.newBuilder().setKey(columnName).setStringValue("").build();
    if (columnNameToGroupNumber.containsKey(columnName)
        && protoCompositeFields.containsKey(columnNameToGroupNumber.get(columnName))) {
      List<CompositeField> compositeFieldList =
          protoCompositeFields.get(columnNameToGroupNumber.get(columnName));
      return fieldIndex < compositeFieldList.size()
          ? compositeFieldList.get(fieldIndex).getChildFieldsList().stream()
              .filter(field -> columnName.equals(field.getKeyValue().getKey()))
              .map(filteredField -> convertKeyValue(filteredField.getKeyValue()))
              .findAny()
              .orElse(defaultKeyValue)
          : defaultKeyValue;
    }
    return defaultKeyValue;
  }

  private DataRecord createDataRecord(
      List<DataRecord.KeyValue> keyValues,
      Optional<JobResultCode> rowLevelErrorCode,
      ProtoEncryptionLevel encryptionLevel) {
    ProcessingMetadata.Builder processingMetadata = ProcessingMetadata.newBuilder();
    if (!encryptionKeyAliasesToType.isEmpty()
        && encryptionLevel != ProtoEncryptionLevel.UNSPECIFIED_ENCRYPTION_LEVEL) {
      processingMetadata.setProtoEncryptionLevel(encryptionLevel);
    }
    DataRecord.Builder dataRecordBuilder =
        DataRecord.newBuilder()
            .addAllKeyValues(keyValues)
            .setProcessingMetadata(processingMetadata.build());
    if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS && rowLevelErrorCode.isPresent()) {
      dataRecordBuilder.setErrorCode(rowLevelErrorCode.get());
    }
    return dataRecordBuilder.build();
  }

  private HashSet<EncryptionKey> getAllEncryptionKeys(
      Map<EncryptionKey, Map<String, List<Field>>> singleFieldMap,
      Map<EncryptionKey, Map<Integer, List<CompositeField>>> compositeFieldMap) {
    HashSet<EncryptionKey> allKeys =
        new HashSet<>(singleFieldMap.keySet()); // Start with keys from map1
    allKeys.addAll(compositeFieldMap.keySet()); // Add keys from map2
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

  private DataRecord.KeyValue convertKeyValue(KeyValue keyValue) {
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
