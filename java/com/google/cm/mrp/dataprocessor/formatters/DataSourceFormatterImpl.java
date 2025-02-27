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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_NESTED_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_ENCRYPTION_TYPE;
import static com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType.STRING;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.cm.mrp.dataprocessor.formatters.EnhancedMatchMapper.isValidEnhancedMatchSchema;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.CoordinatorKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.EncryptionKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.GcpWrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.CoordinatorKey;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.CompositeColumn;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns.WrappedKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.MatchCondition;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Formats a {@link DataRecord} based on given {@link Schema} */
public final class DataSourceFormatterImpl implements DataSourceFormatter {
  static final String REGEX_ENDING_DIGIT = "\\d+$";
  private static final Logger logger = LoggerFactory.getLogger(DataSourceFormatterImpl.class);

  /**
   * Map of composite column names to their indices in the schema. Each composite column key can
   * have multiple groups in the schema.
   *
   * <p>Example of the map: {address1 = [{first_name = index1, ..., country_code = indexN}, ...],
   * ..., addressN = [{first_name = index1, ..., country_code = indexN}, ...]}
   */
  private final ImmutableMap<String, List<Map<String, Integer>>> compositeColumnKeysIndicesMap;

  // Map of encryption key columns name to their index in the schema. Each key only have one index
  // in the schema
  private final ImmutableMap<String, Integer> encryptionKeyIndicesMap;

  /**
   * Map of other single column name to its index in the schema. Each key may have multiple indices
   * in the schema.
   *
   * <p>Example of the map: {email = [index1, ..., indexN], ..., error_codes = [index1], phone =
   * [index1, ..., indexN]}
   */
  private final ImmutableMap<String, List<Integer>> singleColumnKeyIndicesMap;

  /** The index of row marker column in the formatted schema. */
  private final Integer rowMarkerColumnIndex;

  /**
   * Map of the key name to schema or composite column name (if the key belongs to a composite key)
   * and its alias if available.
   *
   * <p>Example of the map: {fn = Pair<>(address, first_name), ..., em = Pair<>(em, email),
   * error_codes = Pair<>(error_code, ""), key_id = Pair<>(key_id, coordinator_key_id)}
   */
  private final ImmutableMap<String, Pair<String, String>> mapKeyToColumnNameAndAlias;

  // The schema the formatter will use to structure the data record.
  private final Schema formattedSchema;
  private final Optional<CryptoClient> cryptoClient;
  private final Optional<DataRecordEncryptionColumns> dataRecordEncryptionColumns;
  private final FeatureFlags featureFlags;

  /** Constructor for {@link DataSourceFormatterImpl}. */
  @AssistedInject
  public DataSourceFormatterImpl(
      @Assisted MatchConfig matchConfig,
      @Assisted Schema inputSchema,
      @Assisted FeatureFlags featureFlags) {
    this.featureFlags = featureFlags;
    this.dataRecordEncryptionColumns = Optional.empty();
    this.cryptoClient = Optional.empty();
    this.encryptionKeyIndicesMap = ImmutableMap.of();
    this.formattedSchema = createFormattedSchema(matchConfig, inputSchema);
    this.compositeColumnKeysIndicesMap =
        getCompositeColumnKeysIndicesMap(matchConfig, formattedSchema);
    this.singleColumnKeyIndicesMap = getSingleColumnKeyIndicesMap(matchConfig, formattedSchema);
    this.rowMarkerColumnIndex = getRowMarkerColumnIndex(formattedSchema);
    this.mapKeyToColumnNameAndAlias = getMapKeyToColumnNameAndAlias(matchConfig, formattedSchema);
  }

  /** Constructor for {@link DataSourceFormatterImpl}. */
  @AssistedInject
  public DataSourceFormatterImpl(
      @Assisted MatchConfig matchConfig,
      @Assisted Schema inputSchema,
      @Assisted FeatureFlags featureFlags,
      @Assisted EncryptionMetadata encryptionMetadata,
      @Assisted CryptoClient cryptoClient) {
    this.cryptoClient = Optional.of(cryptoClient);
    this.featureFlags = featureFlags;
    this.formattedSchema = createFormattedSchema(matchConfig, inputSchema);
    this.dataRecordEncryptionColumns =
        getDataRecordEncryptionColumns(encryptionMetadata, matchConfig, formattedSchema);
    this.encryptionKeyIndicesMap = getEncryptionKeyIndicesMap(formattedSchema);
    this.compositeColumnKeysIndicesMap =
        getCompositeColumnKeysIndicesMap(matchConfig, formattedSchema);
    this.singleColumnKeyIndicesMap = getSingleColumnKeyIndicesMap(matchConfig, formattedSchema);
    this.rowMarkerColumnIndex = getRowMarkerColumnIndex(formattedSchema);
    this.mapKeyToColumnNameAndAlias = getMapKeyToColumnNameAndAlias(matchConfig, formattedSchema);
  }

  private Schema createFormattedSchema(MatchConfig matchConfig, Schema inputSchema) {
    Schema.Builder formattedSchemaBuilder = Schema.newBuilder();
    for (Column column : inputSchema.getColumnsList()) {
      if (column.hasNestedSchema()) {
        if (!isNestedColumnSchemaValid(matchConfig, column)) {
          String message = "Schema file with invalid nested column schema.";
          logger.error(message);
          throw new JobProcessorException(message, INVALID_NESTED_SCHEMA_FILE_ERROR);
        }
        formattedSchemaBuilder.addAllColumns(column.getNestedSchema().getColumnsList());
      } else {
        formattedSchemaBuilder.addColumns(column);
      }
    }
    // Inserts row marker column to formattedSchemaBuilder
    formattedSchemaBuilder.addColumns(
        Column.newBuilder().setColumnName(ROW_MARKER_COLUMN_NAME).setColumnType(STRING).build());
    return formattedSchemaBuilder.build();
  }

  // Checks whether the nested column is valid.
  private boolean isNestedColumnSchemaValid(MatchConfig matchConfig, Column column) {
    if (!column.hasColumnFormat()) {
      String message =
          String.format(
              "Column format not found for column %s with the nested schema.",
              column.getColumnName());
      logger.error(message);
      throw new JobProcessorException(message, INVALID_NESTED_SCHEMA_FILE_ERROR);
    }

    switch (column.getColumnFormat()) {
      case GTAG:
        return isValidEnhancedMatchSchema(column.getNestedSchema(), matchConfig);
      default:
        return false;
    }
  }

  private Optional<DataRecordEncryptionColumns> getDataRecordEncryptionColumns(
      EncryptionMetadata encryptionMetadata, MatchConfig matchConfig, Schema schema) {
    var encryptionKeyColumnIndicesBuilder = EncryptionKeyColumnIndices.newBuilder();
    EncryptionKeyColumns encryptionKeyColumns = matchConfig.getEncryptionKeyColumns();
    EncryptionKeyInfo encryptionKeyInfo = encryptionMetadata.getEncryptionKeyInfo();
    switch (encryptionKeyInfo.getKeyInfoCase()) {
      case WRAPPED_KEY_INFO:
        if (!encryptionKeyColumns.hasWrappedKeyColumns()) {
          String msg = "Job has WrappedKey parameter but match config does not support it";
          logger.info(msg);
          throw new JobProcessorException(msg, UNSUPPORTED_ENCRYPTION_TYPE);
        }
        if (!encryptionKeyInfo.getWrappedKeyInfo().hasGcpWrappedKeyInfo()
            && !encryptionKeyInfo.getWrappedKeyInfo().hasAwsWrappedKeyInfo()) {
          String msg = "Encrypted job without Gcp or Aws WrappedKeyInfo";
          logger.info(msg);
          throw new JobProcessorException(msg, INVALID_PARAMETERS);
        }

        WrappedKeyColumns wrappedKeyColumns = encryptionKeyColumns.getWrappedKeyColumns();
        var wrappedKeyIndicesBuilder =
            WrappedKeyColumnIndices.newBuilder()
                .setEncryptedDekColumnIndex(
                    getEncryptionKeyColumnIndex(
                            schema, wrappedKeyColumns.getEncryptedDekColumnAlias())
                        .orElseThrow(
                            () ->
                                new JobProcessorException(
                                    "DEK column missing in schema.", MISSING_ENCRYPTION_COLUMN)))
                .setKekUriColumnIndex(
                    getEncryptionKeyColumnIndex(schema, wrappedKeyColumns.getKekUriColumnAlias())
                        .orElseThrow(
                            () ->
                                new JobProcessorException(
                                    "KEK column missing in schema.", MISSING_ENCRYPTION_COLUMN)));

        // For GCP, if WIP is not in request, then try to get it from the schema
        if (encryptionKeyInfo.getWrappedKeyInfo().hasGcpWrappedKeyInfo()
            && encryptionKeyInfo
                .getWrappedKeyInfo()
                .getGcpWrappedKeyInfo()
                .getWipProvider()
                .isBlank()) {
          // Check WIP alias exists in matchConfig
          if (!wrappedKeyColumns.hasGcpWrappedKeyColumns()) {
            String msg = "WIP missing in request and no WIP alias in match config.";
            logger.error(msg);
            throw new JobProcessorException(msg, ENCRYPTION_COLUMNS_CONFIG_ERROR);
          }
          wrappedKeyIndicesBuilder.setGcpColumnIndices(
              GcpWrappedKeyColumnIndices.newBuilder()
                  .setWipProviderIndex(
                      getEncryptionKeyColumnIndex(
                              schema,
                              wrappedKeyColumns.getGcpWrappedKeyColumns().getWipProviderAlias())
                          .orElseThrow(
                              () ->
                                  new JobProcessorException(
                                      "WIP missing in request and no WIP column in schema.",
                                      MISSING_ENCRYPTION_COLUMN))));
        }
        encryptionKeyColumnIndicesBuilder.setWrappedKeyColumnIndices(wrappedKeyIndicesBuilder);
        break;
      case COORDINATOR_KEY_INFO:
        if (!encryptionKeyColumns.hasCoordinatorKeyColumn()) {
          String msg = "Job has CoordinatorKey parameter but match config does not support it.";
          logger.info(msg);
          throw new JobProcessorException(msg, UNSUPPORTED_ENCRYPTION_TYPE);
        }
        encryptionKeyColumnIndicesBuilder.setCoordinatorKeyColumnIndices(
            CoordinatorKeyColumnIndices.newBuilder()
                .setCoordinatorKeyColumnIndex(
                    getEncryptionKeyColumnIndex(
                            schema,
                            encryptionKeyColumns
                                .getCoordinatorKeyColumn()
                                .getCoordinatorKeyColumnAlias())
                        .orElseThrow(
                            () ->
                                new JobProcessorException(
                                    "Coordinator key ID column missing in schema",
                                    MISSING_ENCRYPTION_COLUMN))));
        break;
      default:
        return Optional.empty();
    }
    var columnsToDecrypt =
        IntStream.range(0, schema.getColumnsCount())
            .filter(i -> schema.getColumns(i).getEncrypted())
            .boxed()
            .collect(Collectors.toList());

    return Optional.of(
        DataRecordEncryptionColumns.newBuilder()
            .addAllEncryptedColumnIndices(columnsToDecrypt)
            .setEncryptionKeyColumnIndices(encryptionKeyColumnIndicesBuilder.build())
            .build());
  }

  // Retrieves the schema indices for encryption keys previously parsed.
  private ImmutableMap<String, Integer> getEncryptionKeyIndicesMap(Schema schema) {
    ImmutableMap.Builder<String, Integer> encryptionKeyIndicesMapBuilder =
        new ImmutableMap.Builder<>();
    dataRecordEncryptionColumns.ifPresent(
        map -> {
          EncryptionKeyColumnIndices columnIndices = map.getEncryptionKeyColumnIndices();
          switch (columnIndices.getColumnIndicesCase()) {
            case WRAPPED_KEY_COLUMN_INDICES:
              WrappedKeyColumnIndices wrappedKeyColumnIndices =
                  columnIndices.getWrappedKeyColumnIndices();
              int dekIndex = wrappedKeyColumnIndices.getEncryptedDekColumnIndex();
              int kekIndex = wrappedKeyColumnIndices.getKekUriColumnIndex();
              encryptionKeyIndicesMapBuilder.put(
                  schema.getColumns(dekIndex).getColumnName(), dekIndex);
              encryptionKeyIndicesMapBuilder.put(
                  schema.getColumns(kekIndex).getColumnName(), kekIndex);
              // Check if WIP is in row
              if (wrappedKeyColumnIndices.hasGcpColumnIndices()) {
                int wipIndex = wrappedKeyColumnIndices.getGcpColumnIndices().getWipProviderIndex();
                encryptionKeyIndicesMapBuilder.put(
                    schema.getColumns(wipIndex).getColumnName(), wipIndex);
              }
              break;
            case COORDINATOR_KEY_COLUMN_INDICES:
              int coordKeyIndex =
                  columnIndices.getCoordinatorKeyColumnIndices().getCoordinatorKeyColumnIndex();
              encryptionKeyIndicesMapBuilder.put(
                  schema.getColumns(coordKeyIndex).getColumnName(), coordKeyIndex);
              break;
          }
        });
    return encryptionKeyIndicesMapBuilder.build();
  }

  // Iterates over matchConfig to get the composite key column and its composite columns, then
  // retrieves the schema indices for those keys.
  private ImmutableMap<String, List<Map<String, Integer>>> getCompositeColumnKeysIndicesMap(
      MatchConfig matchConfig, Schema schema) {
    return matchConfig.getMatchConditionsList().stream()
        .map(MatchCondition::getDataSource1Column)
        .filter(compositeColumn -> compositeColumn.getColumnsCount() > 1)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toMap(
                    CompositeColumn::getColumnAlias,
                    compositeColumn -> {
                      Set<String> aliasesSet =
                          compositeColumn.getColumnsList().stream()
                              .map(MatchConfig.Column::getColumnAlias)
                              .map(String::toLowerCase)
                              .collect(Collectors.toSet());
                      return getMultiColumnIndicesForColumnAlias(
                          schema, aliasesSet, compositeColumn);
                    }),
                ImmutableMap::copyOf));
  }

  // Iterates over schema to get the single key columns that are not part of a composite key or
  // encryption key, then retrieves the schema indices for those keys.
  private ImmutableMap<String, List<Integer>> getSingleColumnKeyIndicesMap(
      MatchConfig matchConfig, Schema schema) {
    return IntStream.range(0, schema.getColumnsCount())
        // Single columns are columns that are neither part of a composite key nor designated as
        // encryption keys or row marker column.
        .filter(
            index ->
                getCompositeColumnIfExists(
                            matchConfig, schema, schema.getColumns(index).getColumnAlias())
                        .isEmpty()
                    && !encryptionKeyIndicesMap.containsKey(
                        schema.getColumns(index).getColumnName())
                    && !schema.getColumns(index).getColumnName().equals(ROW_MARKER_COLUMN_NAME))
        .boxed()
        .collect(
            Collectors.toMap(index -> index, index -> schema.getColumns(index).getColumnName()))
        .entrySet()
        .stream()
        .collect(
            Collectors.collectingAndThen(
                Collectors.groupingBy(
                    Map.Entry::getValue,
                    Collectors.mapping(Map.Entry::getKey, Collectors.toList())),
                ImmutableMap::copyOf));
  }

  private Integer getRowMarkerColumnIndex(Schema schema) {
    return IntStream.range(0, schema.getColumnsCount())
        .filter(
            index ->
                schema.getColumns(index).getColumnName().equalsIgnoreCase(ROW_MARKER_COLUMN_NAME))
        .findAny()
        .orElseThrow(
            () -> {
              // This error won't occur as the row marker column is added by this class.
              String message = "Internal row marker column missing in schema.";
              logger.error(message);
              return new JobProcessorException(message);
            });
  }

  // Iterates over schema to get the key's schema column name (or composite column name) and column
  // alias.
  private ImmutableMap<String, Pair<String, String>> getMapKeyToColumnNameAndAlias(
      MatchConfig matchConfig, Schema schema) {
    Set<String> uniqueColumnNames = new HashSet<>();
    return schema.getColumnsList().stream()
        // To filter out the repeat key in schema
        .filter(column -> uniqueColumnNames.add(column.getColumnName().toLowerCase()))
        .collect(
            Collectors.collectingAndThen(
                Collectors.toMap(
                    Schema.Column::getColumnName,
                    column -> {
                      Optional<CompositeColumn> matchedCompositeColumn =
                          getCompositeColumnIfExists(matchConfig, schema, column.getColumnAlias());
                      // If the key belongs to a composite key, the composite column name and its
                      // own column alias will be return.
                      return matchedCompositeColumn.isPresent()
                          ? new Pair<String, String>(
                              matchedCompositeColumn.get().getColumnAlias(),
                              column.getColumnAlias())
                          : new Pair<String, String>(
                              column.getColumnName(), column.getColumnAlias());
                    }),
                ImmutableMap::copyOf));
  }

  // Gets the encryption key column index in the schema, if it exists.
  private OptionalInt getEncryptionKeyColumnIndex(Schema schema, String alias) {
    return IntStream.range(0, schema.getColumnsCount())
        .filter(index -> schema.getColumns(index).getColumnAlias().equalsIgnoreCase(alias))
        .findAny();
  }

  // Retrieves the composite column associated with a given column alias (if the alias is part of a
  // composite column).
  private static Optional<CompositeColumn> getCompositeColumnIfExists(
      MatchConfig matchConfig, Schema schema, String alias) {
    return matchConfig.getMatchConditionsList().stream()
        .map(MatchCondition::getDataSource1Column)
        .filter(compositeColumn -> compositeColumn.getColumnsCount() > 1)
        .filter(
            compositeColumn ->
                compositeColumn.getColumnsList().stream()
                    .filter(subColumn -> subColumn.getColumnAlias().equalsIgnoreCase(alias))
                    .anyMatch(subColumn -> true))
        .findAny();
  }

  /**
   * Gets a list of map of keys belonging to the same composite key and their indexes in the schema.
   * These keys in one map belong to the same group. The reason for multiple maps is because one
   * schema may contain multiple group of composite key.
   *
   * <p>Example of map list: [{fn = 1, ln = 2, co = 3, pc = 4}, {fn = 7, ln = 8, co= 9, pc = 10}]
   */
  private List<Map<String, Integer>> getMultiColumnIndicesForColumnAlias(
      Schema schema, Set<String> aliasesSet, CompositeColumn compositeColumn) {
    List<Map<String, Integer>> outputIndicesList =
        IntStream.range(0, schema.getColumnsCount())
            .filter(
                index ->
                    aliasesSet.contains(schema.getColumns(index).getColumnAlias().toLowerCase()))
            .mapToObj(
                // Map index of alias to a Pair of the alias's order in the column group and the
                // same index
                index ->
                    new Pair<>(
                        compositeColumn.getColumnsList().stream()
                            .filter(
                                c ->
                                    c.getColumnAlias()
                                        .equalsIgnoreCase(
                                            schema.getColumns(index).getColumnAlias()))
                            .findAny()
                            .map(MatchConfig.Column::getColumnAlias)
                            .orElseThrow(
                                () -> {
                                  String message = "Composite column missing key in schema.";
                                  logger.error(message);
                                  return new JobProcessorException(
                                      message, INVALID_SCHEMA_FILE_ERROR);
                                }),
                        index))
            .collect(
                // Group by column group. This gives us a Map<Integer, Pair<String, Integer>> where
                // the key is the column group
                Collectors.groupingBy(pair -> schema.getColumns(pair.getRight()).getColumnGroup()))
            .entrySet()
            .stream()
            .map(
                entry ->
                    entry.getValue().stream()
                        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight)))
            .collect(Collectors.toList());
    // Each composite column group from the schema should contain the indices for all sub_keys.
    if (!outputIndicesList.stream()
        .allMatch(
            compositeKeyGroup -> compositeKeyGroup.size() == compositeColumn.getColumnsCount())) {
      String message =
          String.format(
              "Composite column %s missing key in schema.", compositeColumn.getColumnAlias());
      logger.error(message);
      throw new JobProcessorException(message, INVALID_SCHEMA_FILE_ERROR);
    }
    return outputIndicesList;
  }

  private class Pair<T, S> {
    private T left;
    private S right;

    public Pair(T left, S right) {
      this.left = left;
      this.right = right;
    }

    public T getLeft() {
      return left;
    }

    public S getRight() {
      return right;
    }
  }

  /** Gets the formatted schema that the DataSourceFormatter formats the data record into. */
  @Override
  public Schema getFormattedSchema() {
    return formattedSchema;
  }

  @Override
  public Optional<DataRecordEncryptionColumns> getDataRecordEncryptionColumns() {
    return dataRecordEncryptionColumns;
  }

  /**
   * Formats a {@link DataRecord} based on the formatted schema which may return a list of {@link
   * DataRecord}
   *
   * @param dataRecord input {@link DataRecord} for formatting.
   * @return a list of {@link DataRecord} whose structure aligns with the formatted schema.
   */
  @Override
  public ImmutableList<DataRecord> format(DataRecord dataRecord) {
    Map<String, List<List<KeyValue>>> keyValueGroupMap = groupKeyValues(dataRecord);
    // Indices map to track key-value pairs assignment in data records
    Map<String, Integer> mapKeyIndex =
        keyValueGroupMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> 0));
    ImmutableList.Builder<DataRecord> dataRecordList = ImmutableList.builder();
    String rowMarkerUuid = UUID.randomUUID().toString();
    while (hasPendingFormatKeyValues(keyValueGroupMap, mapKeyIndex)) {
      List<KeyValue> keyValuesList =
          formattedSchema.getColumnsList().stream()
              .map(
                  column ->
                      KeyValue.newBuilder()
                          .setKey(column.getColumnName())
                          // Sets an empty StringValue to ensure that KeyValue.hasStringValue()
                          // returns true.
                          .setStringValue("")
                          .build())
              .collect(Collectors.toList());
      setSingleColumnKeyValues(keyValueGroupMap, mapKeyIndex, keyValuesList);
      setCompositeColumnKeyValues(keyValueGroupMap, mapKeyIndex, keyValuesList);
      // Populates encryption key values to keyValuesList.
      setEncryptionKeyValues(keyValueGroupMap, mapKeyIndex, keyValuesList);
      // Populates row marker UUID to keyValuesList.
      setRowMarkerKeyValue(keyValuesList, rowMarkerUuid);

      DataRecord.Builder dataRecordBuilder = DataRecord.newBuilder().addAllKeyValues(keyValuesList);
      if (dataRecord.hasProcessingMetadata()) {
        dataRecordBuilder.setProcessingMetadata(dataRecord.getProcessingMetadata());
      }
      // Add encrypted key values if cryptoClient and dataRecordEncryptionColumns are present and
      // batch encryption is disabled.
      if (cryptoClient.isPresent() && dataRecordEncryptionColumns.isPresent()) {
        DataRecordEncryptionKeys encryptionKeys = getEncryptionKeys(keyValuesList);
        if (!encryptionKeys.hasCoordinatorKey()
            || !featureFlags.coordinatorBatchEncryptionEnabled()) {
          dataRecordBuilder.putAllEncryptedKeyValues(
              getEncryptedKeyValues(keyValuesList, encryptionKeys));
        }
      }
      // Sets dataRecord error_code if it's present in input record.
      if (dataRecord.hasErrorCode()) {
        dataRecordBuilder.setErrorCode(dataRecord.getErrorCode());
      }
      dataRecordList.add(dataRecordBuilder.build());
    }
    return dataRecordList.build();
  }

  // Generates encrypted values for individual columns of PII data, which refers to columns marked
  // for encrypted, not as part of a composite key.
  private Map<Integer, String> getEncryptedKeyValues(
      List<KeyValue> keyValuesList, DataRecordEncryptionKeys encryptionKeys) {
    HashMap<Integer, String> encryptedKeyValues = new HashMap<>();
    IntStream.range(0, formattedSchema.getColumnsCount())
        // Gets the single PII columns which marked as encrypted.
        .filter(
            index ->
                formattedSchema.getColumns(index).getEncrypted()
                    && !formattedSchema.getColumns(index).hasColumnGroup())
        .forEach(
            index -> {
              KeyValue keyValue = keyValuesList.get(index);
              // Skips the key without string value.
              if (keyValue.getStringValue().isEmpty()) {
                return;
              }
              try {
                String encryptedValue =
                    getCryptoClient().encrypt(encryptionKeys, keyValue.getStringValue());
                encryptedKeyValues.put(index, encryptedValue);
              } catch (CryptoClientException | IllegalArgumentException e) {
                String message =
                    String.format(
                        "Could not encrypt record column %s",
                        formattedSchema.getColumns(index).getColumnName());
                logger.error(message);
                throw new JobProcessorException(message, e);
              }
            });
    return encryptedKeyValues;
  }

  // Gets the encryption key from keyValue list.
  private DataRecordEncryptionKeys getEncryptionKeys(List<KeyValue> keyValuesList) {
    var encryptionKey = DataRecordEncryptionKeys.newBuilder();
    var keyColumnIndices = dataRecordEncryptionColumns.get().getEncryptionKeyColumnIndices();
    if (keyColumnIndices.hasWrappedKeyColumnIndices()) {
      var dekIndex = keyColumnIndices.getWrappedKeyColumnIndices().getEncryptedDekColumnIndex();
      var kekIndex = keyColumnIndices.getWrappedKeyColumnIndices().getKekUriColumnIndex();
      encryptionKey.setWrappedEncryptionKeys(
          WrappedEncryptionKeys.newBuilder()
              .setEncryptedDek(keyValuesList.get(dekIndex).getStringValue())
              .setKekUri(keyValuesList.get(kekIndex).getStringValue())
              .build());
    } else if (keyColumnIndices.hasCoordinatorKeyColumnIndices()) {
      var coordKeyIndex =
          keyColumnIndices.getCoordinatorKeyColumnIndices().getCoordinatorKeyColumnIndex();
      encryptionKey.setCoordinatorKey(
          CoordinatorKey.newBuilder()
              .setKeyId(keyValuesList.get(coordKeyIndex).getStringValue())
              .build());
    }
    return encryptionKey.build();
  }

  // Check whether there are key-value pairs that need to be formatted by comparing whether the
  // specified key-value pair index is out of bounds.
  private static boolean hasPendingFormatKeyValues(
      Map<String, List<List<KeyValue>>> keyValueGroupMap, Map<String, Integer> mapKeyIndex) {
    return mapKeyIndex.entrySet().stream()
        .anyMatch(entry -> entry.getValue() < keyValueGroupMap.get(entry.getKey()).size());
  }

  /**
   * Categorize KeyValue pairs from the DataRecord. Composite keys use both column names and a group
   * ID for grouping, while non-composite keys are grouped using just their key name.
   *
   * @param dataRecord input {@link DataRecord} for category.
   * @return a map of composite key or non composite key with their composited {@link KeyValue}
   *     pairs.
   *     <p>Example of the output: {address = [[fn1_kv, ln1_kv, co1_kv, pc1_kv], ..., [fnN_kv,
   *     lnN_kv, coN_kv, pcN_kv]], em = [[em_kv], ..., [em_kv]], ..., key_id = [[key_id_kv]]}
   */
  private Map<String, List<List<KeyValue>>> groupKeyValues(DataRecord dataRecord) {
    HashMap<String, List<KeyValue>> singleColumnKeyValuesMap = new HashMap<>();
    HashMap<String, HashMap<String, List<KeyValue>>> multiColumnKeyValuesMap = new HashMap<>();
    dataRecord.getKeyValuesList().stream()
        // Keys are ignored unless they explicitly match a full or partial (with ending digits)
        // definition within the schema.
        .filter(
            keyValuePair ->
                mapKeyToColumnNameAndAlias.containsKey(keyValuePair.getKey())
                    || mapKeyToColumnNameAndAlias.containsKey(getKeyName(keyValuePair.getKey())))
        .forEach(
            keyValuePair -> {
              String key = keyValuePair.getKey();
              // A key has a full name definition within the schema.
              if (mapKeyToColumnNameAndAlias.containsKey(key)) {
                singleColumnKeyValuesMap.putIfAbsent(key, new ArrayList<>());
                singleColumnKeyValuesMap.get(key).add(keyValuePair);
              } else {
                String keyName = mapKeyToColumnNameAndAlias.get(getKeyName(key)).getLeft();
                String groupId = getGroupId(key);
                multiColumnKeyValuesMap.putIfAbsent(keyName, new HashMap<>());
                multiColumnKeyValuesMap.get(keyName).putIfAbsent(groupId, new ArrayList<>());
                multiColumnKeyValuesMap.get(keyName).get(groupId).add(keyValuePair);
              }
            });

    Map<String, List<List<KeyValue>>> keyValueGroupMap = new HashMap<>();
    singleColumnKeyValuesMap.forEach(
        (key, keyValueList) -> {
          keyValueGroupMap.putIfAbsent(key, new ArrayList<>());
          keyValueList.forEach((keyValue) -> keyValueGroupMap.get(key).add(List.of(keyValue)));
        });
    multiColumnKeyValuesMap.forEach(
        (key, keyValueListMap) -> {
          keyValueGroupMap.putIfAbsent(key, new ArrayList<>());
          keyValueListMap.values().stream()
              .forEach(keyValueList -> keyValueGroupMap.get(key).add(keyValueList));
        });

    return keyValueGroupMap;
  }

  // Populates the keyValuesList with single column key-value pairs, ensuring they conform to the
  // structure defined by the formatted schema.
  private void setSingleColumnKeyValues(
      Map<String, List<List<KeyValue>>> keyValueGroupMap,
      Map<String, Integer> mapKeyIndex,
      List<KeyValue> keyValuesList) {
    singleColumnKeyIndicesMap.entrySet().stream()
        .filter(entry -> keyValueGroupMap.containsKey(entry.getKey()))
        .filter(
            entry -> mapKeyIndex.get(entry.getKey()) < keyValueGroupMap.get(entry.getKey()).size())
        .forEach(
            entry -> {
              for (int keyIdx : entry.getValue()) {
                String mapKeyName = entry.getKey();
                Integer mapKeyIdx = mapKeyIndex.get(mapKeyName);
                KeyValue keyValuePair = keyValueGroupMap.get(mapKeyName).get(mapKeyIdx).get(0);
                keyValuesList.set(
                    keyIdx,
                    KeyValue.newBuilder()
                        .setKey(keyValuePair.getKey())
                        .setStringValue(keyValuePair.getStringValue())
                        .build());
                mapKeyIndex.put(mapKeyName, mapKeyIdx + 1);
                if (mapKeyIndex.get(mapKeyName) == keyValueGroupMap.get(entry.getKey()).size()) {
                  return;
                }
              }
            });
  }

  // Populates the keyValuesList with composite column key-value pairs, maintaining group
  // association and ensuring schema conformity.
  private void setCompositeColumnKeyValues(
      Map<String, List<List<KeyValue>>> keyValueGroupMap,
      Map<String, Integer> mapKeyIndex,
      List<KeyValue> keyValuesList) {
    compositeColumnKeysIndicesMap.entrySet().stream()
        .filter(
            entry ->
                keyValueGroupMap.containsKey(entry.getKey())
                    && mapKeyIndex.get(entry.getKey())
                        < keyValueGroupMap.get(entry.getKey()).size())
        .forEach(
            entry -> {
              for (Map<String, Integer> keyIndices : entry.getValue()) {
                String mapKeyName = entry.getKey();
                Integer mapKeyIdx = mapKeyIndex.get(mapKeyName);
                keyValueGroupMap.get(mapKeyName).get(mapKeyIdx).stream()
                    .forEach(
                        keyValuePair -> {
                          String keyName = getKeyName(keyValuePair.getKey());
                          Integer keyIdx =
                              keyIndices.get(mapKeyToColumnNameAndAlias.get(keyName).getRight());
                          keyValuesList.set(
                              keyIdx,
                              KeyValue.newBuilder()
                                  .setKey(keyName)
                                  .setStringValue(keyValuePair.getStringValue())
                                  .build());
                        });
                mapKeyIndex.put(mapKeyName, mapKeyIdx + 1);
                if (mapKeyIndex.get(mapKeyName) == keyValueGroupMap.get(entry.getKey()).size()) {
                  return;
                }
              }
            });
  }

  // Populates the keyValuesList with encryption key-value pairs.
  private void setEncryptionKeyValues(
      Map<String, List<List<KeyValue>>> keyValueGroupMap,
      Map<String, Integer> mapKeyIndex,
      List<KeyValue> keyValuesList) {
    encryptionKeyIndicesMap.entrySet().stream()
        .forEach(
            entry -> {
              String mapKeyName = entry.getKey();
              // Each encryption key should only have one value in one data record.
              if (!keyValueGroupMap.containsKey(mapKeyName)
                  || (keyValueGroupMap.get(mapKeyName).size() != 1
                      && keyValueGroupMap.get(mapKeyName).get(0).size() != 1)) {
                String message = "Invalid encryption key in data record";
                logger.error(message);
                throw new JobProcessorException(message, INVALID_ENCRYPTION_COLUMN);
              }
              KeyValue keyValuePair = keyValueGroupMap.get(mapKeyName).get(0).get(0);
              keyValuesList.set(
                  entry.getValue(),
                  KeyValue.newBuilder()
                      .setKey(mapKeyName)
                      .setStringValue(keyValuePair.getStringValue())
                      .build());
              mapKeyIndex.put(mapKeyName, mapKeyIndex.get(mapKeyName) + 1);
            });
  }

  // Populates the keyValuesList with row marker UUID key-value pair.
  private void setRowMarkerKeyValue(List<KeyValue> keyValuesList, String rowMarkerUuid) {
    keyValuesList.set(
        rowMarkerColumnIndex,
        KeyValue.newBuilder()
            .setKey(formattedSchema.getColumns(rowMarkerColumnIndex).getColumnName())
            .setStringValue(rowMarkerUuid)
            .build());
  }

  // Get the key group ID of the key that is the composite key by retrieving the trailing digits.
  private static String getGroupId(String input) {
    Pattern pattern = Pattern.compile(REGEX_ENDING_DIGIT);
    Matcher matcher = pattern.matcher(input);
    if (matcher.find()) {
      return matcher.group(0);
    }
    // Validation of the key with a trailing digit ensures this error won't occur.
    String message = "Cannot find the group ID for the key.";
    logger.error(message);
    throw new JobProcessorException(message);
  }

  // Gets the key name of a key that is a composite key by removing the trailing digits.
  private static String getKeyName(String key) {
    return key.replaceAll(REGEX_ENDING_DIGIT, "");
  }

  private CryptoClient getCryptoClient() {
    return cryptoClient.orElseThrow(
        () -> {
          String message = "CryptoClient missing in DataSourceFormatter.";
          logger.error(message);
          return new JobProcessorException(message, CRYPTO_CLIENT_CONFIGURATION_ERROR);
        });
  }
}
