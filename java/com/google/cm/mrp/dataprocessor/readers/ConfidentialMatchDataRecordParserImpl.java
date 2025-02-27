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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_MISSING_IN_RECORD;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;

import com.google.cm.mrp.JobProcessorException;
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
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parser to convert proto data format ConfidentialMatchDataRecord to list DataRecord. */
public final class ConfidentialMatchDataRecordParserImpl
    implements ConfidentialMatchDataRecordParser {

  private static final Logger logger =
      LoggerFactory.getLogger(ConfidentialMatchDataRecordParserImpl.class);
  private final ImmutableList<String> columnList;
  private final ImmutableSet<String> singleMatchAliasSet;
  private final ImmutableMap<String, String> matchAliasToCompositeField;
  private final ImmutableMap<String, String> encryptionKeyAliasesToType;
  private final ImmutableMap<String, ColumnType> columnListToValueTypeMap;
  private final ImmutableSet<String> metadataAliasSet;
  private final SuccessMode successMode;
  private static final String COORDINATOR_KEY = "COORDINATOR_KEY";
  private static final String ENCRYPTED_DEK = "ENCRYPTED_DEK";
  private static final String KEK_URI = "KEK_URI";
  private static final String WIP_PROVIDER = "WIP_PROVIDER";

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
    this.singleMatchAliasSet = getMatchConfigSingleFieldSet(matchConfig);
    this.matchAliasToCompositeField = getMatchConfigCompositeFieldAliasMap(matchConfig);
    this.columnListToValueTypeMap = getColumnListToValueTypeMap(schema);
    this.encryptionKeyAliasesToType = ImmutableMap.of();
    this.metadataAliasSet = getMetadataAliasSet();
    this.successMode = successMode;
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
    this.singleMatchAliasSet = getMatchConfigSingleFieldSet(matchConfig);
    this.matchAliasToCompositeField = getMatchConfigCompositeFieldAliasMap(matchConfig);
    this.columnListToValueTypeMap = getColumnListToValueTypeMap(schema);
    this.encryptionKeyAliasesToType =
        getEncryptionKeyAliasesToTypeMap(matchConfig.getEncryptionKeyColumns(), encryptionMetadata);
    this.metadataAliasSet = getMetadataAliasSet();
    this.successMode = successMode;
  }

  /**
   * Parse ConfidentialMatchDataRecord and convert to list of internal DataRecord to be used in the
   * serialized proto reader.
   */
  @Override
  public List<DataRecord> parse(ConfidentialMatchDataRecord cfmDataRecord) {
    String rowId = UUID.randomUUID().toString();

    Map<EncryptionKey, Map<String, List<Field>>> singleFieldMap = new HashMap<>();
    Map<EncryptionKey, Map<String, List<CompositeField>>> compositeFieldMap = new HashMap<>();
    EncryptionKey rowLevelEncryptionKey = cfmDataRecord.getEncryptionKey();
    List<DataRecord> internalRecords = new ArrayList<>();
    ProtoEncryptionLevel encryptionLevel = ProtoEncryptionLevel.UNSPECIFIED_ENCRYPTION_LEVEL;

    // Handle invalid CFM DataRecord without match keys.
    if (cfmDataRecord.getMatchKeysList().isEmpty()) {
      if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
        logger.info("CFM DataRecord does not contain match keys.");
        internalRecords.add(generateErrorDataRecord(JobResultCode.PROTO_MISSING_MATCH_KEYS, rowId));
        return internalRecords;
      }
      throw new JobProcessorException(
          "CFM DataRecord does not contain match keys.", JobResultCode.PROTO_MISSING_MATCH_KEYS);
    }

    // Put match keys in corresponding map for field or composite field.
    for (MatchKey matchKey : cfmDataRecord.getMatchKeysList()) {
      if (matchKey.hasEncryptionKey() && cfmDataRecord.hasEncryptionKey()) {
        String message =
            "Invalid ConfidentialMatchDataRecord with encryption key specified at MatchKey and Row"
                + " level.";
        if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
          logger.info(message);
          return ImmutableList.of(
                  generateErrorDataRecord(JobResultCode.INVALID_ENCRYPTION_COLUMN, rowId));
        }
        throw new JobProcessorException(message, JobResultCode.INVALID_ENCRYPTION_COLUMN);
      }

      EncryptionKey encryptionKey = rowLevelEncryptionKey;
      if (matchKey.hasEncryptionKey()) {
        encryptionLevel = ProtoEncryptionLevel.MATCH_KEY_LEVEL;
        encryptionKey = matchKey.getEncryptionKey();
      } else if (cfmDataRecord.hasEncryptionKey()) {
        encryptionLevel = ProtoEncryptionLevel.ROW_LEVEL;
      }

      if (matchKey.hasField()) {
        singleFieldMap
            .computeIfAbsent(encryptionKey, key -> new HashMap<>())
            .computeIfAbsent(matchKey.getField().getKeyValue().getKey(), k -> new ArrayList<>())
            .add(matchKey.getField());
      } else if (matchKey.hasCompositeField()) {
        compositeFieldMap
            .computeIfAbsent(encryptionKey, key -> new HashMap<>())
            .computeIfAbsent(matchKey.getCompositeField().getKey(), k -> new ArrayList<>())
            .add(matchKey.getCompositeField());
      } else {
        String message = "CFM DataRecord match key does not contain field or composite field.";
        if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
          logger.info(message);
          internalRecords.add(
              generateErrorDataRecord(JobResultCode.PROTO_MATCH_KEY_MISSING_FIELD, rowId));
          return internalRecords;
        }
        throw new JobProcessorException(message, JobResultCode.PROTO_MATCH_KEY_MISSING_FIELD);
      }
    }

    // Validate metadata key values to not contain duplicates
    Map<String, KeyValue> metadataMap;
    try {
      metadataMap =
          cfmDataRecord.getMetadataList().stream()
              .filter(keyValue -> columnList.contains(keyValue.getKey()))
              .collect(Collectors.toMap(KeyValue::getKey, Function.identity()));
    } catch (IllegalStateException e) {
      String message = "Invalid ConfidentialMatchDataRecord with duplicate metadata key.";
      if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
        logger.info(message);
        return ImmutableList.of(
                generateErrorDataRecord(JobResultCode.PROTO_DUPLICATE_METADATA_KEY, rowId));
      }
      throw new JobProcessorException(message, JobResultCode.PROTO_DUPLICATE_METADATA_KEY);
    }

    // Handle invalid metadata containing match key alias
    for (String metadataAlias : metadataMap.keySet()) {
      if (singleMatchAliasSet.contains(metadataAlias)
          || matchAliasToCompositeField.containsKey(metadataAlias)
          || encryptionKeyAliasesToType.containsKey(metadataAlias)) {
        String message =
            String.format("CFM DataRecord metadata contains restricted alias: %s", metadataAlias);
        if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
          logger.info(message);
          return ImmutableList.of(
                  generateErrorDataRecord(
                      JobResultCode.PROTO_METADATA_CONTAINING_RESTRICTED_ALIAS, rowId));
        }
        throw new JobProcessorException(
            message, JobResultCode.PROTO_METADATA_CONTAINING_RESTRICTED_ALIAS);
      }
    }

    // Generate map and process single/composite/metadata field maps for each encryption key
    HashSet<EncryptionKey> encryptionKeySet =
        getAllEncryptionKeys(singleFieldMap, compositeFieldMap);
    boolean firstDataRecord = true;
    for (EncryptionKey encryptionKey : encryptionKeySet) {
      Map<String, List<Field>> protoSingleFields =
          singleFieldMap.containsKey(encryptionKey)
              ? singleFieldMap.get(encryptionKey)
              : new HashMap<>();
      Map<String, List<CompositeField>> protoCompositeFields =
          compositeFieldMap.containsKey(encryptionKey)
              ? compositeFieldMap.get(encryptionKey)
              : new HashMap<>();

      int maxIndex =
          Integer.max(
              protoSingleFields.values().stream().mapToInt(List::size).max().orElse(0),
              protoCompositeFields.values().stream().mapToInt(List::size).max().orElse(0));
      for (int fieldIndex = 0; fieldIndex < maxIndex; fieldIndex++) {
        List<DataRecord.KeyValue> keyValues = new ArrayList<>();
        Optional<JobResultCode> rowLevelErrorCode = Optional.empty();
        for (String column : columnList) {
          DataRecord.KeyValue defaultKeyValue =
              DataRecord.KeyValue.newBuilder().setKey(column).setStringValue("").build();
          if (metadataAliasSet.contains(column)) {
            // only add metadata to first data record
            if (metadataMap.containsKey(column) && firstDataRecord) {
              keyValues.add(convertKeyValue(metadataMap.get(column)));
            } else {
              keyValues.add(DataRecord.KeyValue.newBuilder().setKey(column).build());
            }
          } else if (column.equals(ROW_MARKER_COLUMN_NAME)) {
            keyValues.add(
                DataRecord.KeyValue.newBuilder()
                    .setKey(ROW_MARKER_COLUMN_NAME)
                    .setStringValue(rowId)
                    .build());
          } else if (encryptionKeyAliasesToType.containsKey(column)) {
            try {
              keyValues.add(getEncryptionKeyColumn(column, encryptionKey));
            } catch (JobProcessorException e) {
              if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS
                  && isMissingEncryptionKeyColumnError(e.getErrorCode())) {
                keyValues.add(
                    DataRecord.KeyValue.newBuilder().setKey(column).setStringValue("").build());
                rowLevelErrorCode = Optional.of(e.getErrorCode());
              } else {
                throw e;
              }
            }
          } else if (singleMatchAliasSet.contains(column)
              && protoSingleFields.containsKey(column)
              && fieldIndex < protoSingleFields.get(column).size()) {
            DataRecord.KeyValue keyValue =
                convertKeyValue(protoSingleFields.get(column).get(fieldIndex).getKeyValue());
            keyValues.add(keyValue);
          } else if (matchAliasToCompositeField.containsKey(column)
              && protoCompositeFields.containsKey(matchAliasToCompositeField.get(column))) {
            List<CompositeField> compositeFieldList =
                protoCompositeFields.get(matchAliasToCompositeField.get(column));
            DataRecord.KeyValue keyValue =
                fieldIndex < compositeFieldList.size()
                    ? compositeFieldList.get(fieldIndex).getFieldList().stream()
                        .filter(field -> column.equals(field.getKeyValue().getKey()))
                        .map(filteredField -> convertKeyValue(filteredField.getKeyValue()))
                        .findAny()
                        .orElse(defaultKeyValue)
                    : defaultKeyValue;
            keyValues.add(keyValue);
          } else {
            keyValues.add(
                DataRecord.KeyValue.newBuilder().setKey(column).setStringValue("").build());
          }
        }
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
        internalRecords.add(dataRecordBuilder.build());
        firstDataRecord = false;
      }
    }
    return internalRecords;
  }

  private HashSet<EncryptionKey> getAllEncryptionKeys(
      Map<EncryptionKey, Map<String, List<Field>>> singleFieldMap,
      Map<EncryptionKey, Map<String, List<CompositeField>>> compositeFieldMap) {
    HashSet<EncryptionKey> allKeys =
        new HashSet<>(singleFieldMap.keySet()); // Start with keys from map1
    allKeys.addAll(compositeFieldMap.keySet()); // Add keys from map2
    return allKeys;
  }

  private ImmutableList<String> getColumnList(Schema schema) {
    // generate schema ordering list<column_alias>
    return schema.getColumnsList().stream()
        .map(Column::getColumnAlias)
        .collect(ImmutableList.toImmutableList());
  }

  private ImmutableMap<String, ColumnType> getColumnListToValueTypeMap(Schema schema) {
    return ImmutableMap.copyOf(
        schema.getColumnsList().stream()
            .collect(Collectors.toUnmodifiableMap(Column::getColumnAlias, Column::getColumnType)));
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

  private ImmutableMap<String, String> getEncryptionKeyAliasesToTypeMap(
      EncryptionKeyColumns encryptionKeyColumns, EncryptionMetadata encryptionMetadata) {
    ImmutableMap.Builder<String, String> encryptionKeyAliasesToTypeMap = ImmutableMap.builder();
    switch (encryptionMetadata.getEncryptionKeyInfo().getKeyInfoCase()) {
      case COORDINATOR_KEY_INFO:
        encryptionKeyAliasesToTypeMap.put(
            encryptionKeyColumns.getCoordinatorKeyColumn().getCoordinatorKeyColumnAlias(),
            COORDINATOR_KEY);
        break;
      case WRAPPED_KEY_INFO:
        encryptionKeyAliasesToTypeMap.put(
            encryptionKeyColumns.getWrappedKeyColumns().getEncryptedDekColumnAlias(),
            ENCRYPTED_DEK);
        encryptionKeyAliasesToTypeMap.put(
            encryptionKeyColumns.getWrappedKeyColumns().getKekUriColumnAlias(), KEK_URI);
        encryptionKeyAliasesToTypeMap.put(
            encryptionKeyColumns
                .getWrappedKeyColumns()
                .getGcpWrappedKeyColumns()
                .getWipProviderAlias(),
            WIP_PROVIDER);
        break;
      default:
        throw new JobProcessorException(
            "Invalid encryption metadata encryption key info.", JobResultCode.INVALID_PARAMETERS);
    }
    return encryptionKeyAliasesToTypeMap.build();
  }

  private DataRecord.KeyValue getEncryptionKeyColumn(String column, EncryptionKey encryptionKey) {
    String encryptionKeyValue;
    switch (encryptionKeyAliasesToType.get(column)) {
      case COORDINATOR_KEY:
        if (encryptionKey.getCoordinatorKey().getKeyId().isEmpty()) {
          throw new JobProcessorException(
              "Encryption key missing coordinator key.", COORDINATOR_KEY_MISSING_IN_RECORD);
        }
        encryptionKeyValue = encryptionKey.getCoordinatorKey().getKeyId();
        break;
      case ENCRYPTED_DEK:
        if (encryptionKey.getWrappedKey().getEncryptedDek().isEmpty()) {
          throw new JobProcessorException(
              "Encryption key missing encrypted DEK.", DEK_MISSING_IN_RECORD);
        }
        encryptionKeyValue = encryptionKey.getWrappedKey().getEncryptedDek();
        break;
      case KEK_URI:
        if (encryptionKey.getWrappedKey().getKekUri().isEmpty()) {
          throw new JobProcessorException("Encryption key missing KEK URI.", KEK_MISSING_IN_RECORD);
        }
        encryptionKeyValue = encryptionKey.getWrappedKey().getKekUri();
        break;
      case WIP_PROVIDER:
        if (encryptionKey.getWrappedKey().getWip().isEmpty()) {
          throw new JobProcessorException(
              "Encryption key missing WIP provider.", WIP_MISSING_IN_RECORD);
        }
        encryptionKeyValue = encryptionKey.getWrappedKey().getWip();
        break;
      default:
        String message =
            String.format("Invalid key column type: %s", encryptionKeyAliasesToType.get(column));
        throw new JobProcessorException(message, JobResultCode.INVALID_ENCRYPTION_COLUMN);
    }
    return DataRecord.KeyValue.newBuilder()
        .setKey(column)
        .setStringValue(encryptionKeyValue)
        .build();
  }

  private ImmutableSet<String> getMetadataAliasSet() {
    return columnList.stream()
        .filter(
            column ->
                !singleMatchAliasSet.contains(column)
                    && !encryptionKeyAliasesToType.containsKey(column)
                    && !matchAliasToCompositeField.containsKey(column)
                    && !column.equals(ROW_MARKER_COLUMN_NAME))
        .collect(ImmutableSet.toImmutableSet());
  }

  private DataRecord.KeyValue convertKeyValue(KeyValue keyValue) {
    String key = keyValue.getKey();
    DataRecord.KeyValue.Builder keyValueBuilder = DataRecord.KeyValue.newBuilder().setKey(key);
    switch (columnListToValueTypeMap.get(key)) {
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
        throw new JobProcessorException(
            "Invalid key value type.", JobResultCode.INVALID_INPUT_FILE_ERROR);
    }
    return keyValueBuilder.build();
  }

  private boolean isMissingEncryptionKeyColumnError(JobResultCode jobResultCode) {
    switch (jobResultCode) {
      case COORDINATOR_KEY_MISSING_IN_RECORD:
      case DEK_MISSING_IN_RECORD:
      case KEK_MISSING_IN_RECORD:
      case WIP_MISSING_IN_RECORD:
        return true;
      default:
        return false;
    }
  }

  private DataRecord generateErrorDataRecord(JobResultCode errorCode, String rowId) {
    DataRecord.Builder errorRecordBuilder = DataRecord.newBuilder().setErrorCode(errorCode);
    for (String column : columnList) {
      if (column.equals(ROW_MARKER_COLUMN_NAME)) {
        errorRecordBuilder.addKeyValues(
            DataRecord.KeyValue.newBuilder().setKey(column).setStringValue(rowId).build());
      } else {
        errorRecordBuilder.addKeyValues(DataRecord.KeyValue.newBuilder().setKey(column).build());
      }
    }
    return errorRecordBuilder.build();
  }
}
