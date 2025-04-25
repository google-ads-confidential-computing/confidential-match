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

package com.google.cm.mrp.dataprocessor.writers;

import static com.google.cm.mrp.backend.DataRecordProto.DataRecord.ProtoEncryptionLevel.ROW_LEVEL;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.OUTPUT_FILE_WRITE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WRITER_MISSING_COORDINATOR_KEY;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WRITER_MISSING_DEK;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WRITER_MISSING_KEK;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WRITER_MISSING_WIP;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Map.entry;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeChildField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.CompositeField;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchOutputDataRecord;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.CoordinatorKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.EncryptionKey.GcpWrappedKey;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.Field;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.KeyValue;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.MatchKey;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.CoordinatorKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.EncryptionKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.GcpWrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns.WrappedKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.PartialSuccessAttributes;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.dataprocessor.common.Annotations.MaxRecordsPerOutputFile;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.scp.shared.util.Base64Util;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of DataWriter for writing serialized proto files. Allows writing one
 * {@link DataChunk} at a time to a proto output file.
 */
public final class SerializedProtoDataWriter extends BaseDataWriter {
  private static final Logger logger = LoggerFactory.getLogger(SerializedProtoDataWriter.class);
  private static final Retry PRINTER_RETRY =
      Retry.of(
          "serialized_proto_data_writer_retry",
          RetryConfig.custom()
              .retryExceptions(IOException.class)
              .maxAttempts(3)
              .failAfterMaxAttempts(true)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      /* initialIntervalMillis */ 500,
                      /* multiplier */ 1.5,
                      /* maxIntervalMillis */ 1500))
              .build());
  private final ImmutableSet<Integer> metadataIndices;
  private final ImmutableSet<Integer> matchFieldIndices;
  private final ImmutableMap<Integer, String> groupNumberToGroupAlias;
  private final ImmutableMap<Integer, ImmutableSet<Integer>> matchCompositeFieldGroupToIndices;
  private final Optional<Integer> recordStatusFieldIndex;
  private final Optional<Integer> rowMarkerIndex;
  private final Optional<DataRecordEncryptionColumns> dataRecordEncryptionColumns;
  private final DataDestination dataDestination;
  private final int maxRecordsPerOutputFile;
  private final String name;
  private PrintWriter writer;
  private int numberOfRecords;
  private int fileNumber;

  /** Constructor for {@link DataWriter}. */
  @AssistedInject
  public SerializedProtoDataWriter(
      @MaxRecordsPerOutputFile Integer maxRecordsPerOutputFile,
      @Assisted DataDestination dataDestination,
      @Assisted String name,
      @Assisted Schema schema,
      @Assisted MatchConfig matchConfig) {
    this.maxRecordsPerOutputFile = maxRecordsPerOutputFile;
    this.dataDestination = dataDestination;
    this.name = name;
    numberOfRecords = 0;
    fileNumber = 0;

    ImmutableMap<String, Integer> columnIndexMap = getSchemaColumnIndexMap(schema);
    ImmutableMap<String, String> nameToAliasMap = getSchemaColumnNamesToAliases(schema);
    ImmutableMap<String, Integer> nameToGroupNumber = getSchemaColumnNamesToGroupNumber(schema);
    groupNumberToGroupAlias = getGroupNumberToGroupAlias(matchConfig, schema);
    matchFieldIndices = getMatchFieldIndices(matchConfig, columnIndexMap, nameToAliasMap);
    matchCompositeFieldGroupToIndices =
        getMatchCompositeFieldGroupNumberToIndices(columnIndexMap, nameToGroupNumber);
    dataRecordEncryptionColumns =
        getDataRecordEncryptionColumns(matchConfig, columnIndexMap, nameToAliasMap);
    recordStatusFieldIndex = getRecordStatusFieldIndex(matchConfig, columnIndexMap);
    rowMarkerIndex = Optional.ofNullable(columnIndexMap.get(ROW_MARKER_COLUMN_NAME));
    // Must be called last since dependent on initialization of other global variables.
    metadataIndices = getMetadataIndices(columnIndexMap);
  }

  /** {@inheritDoc} This implementation is synchronized. */
  @Override
  public synchronized void write(DataChunk dataChunk) throws IOException {
    if (fileNumber == 0) {
      newFile();
    }

    List<List<DataRecord>> groupedRecords = groupRecords(dataChunk);
    for (List<DataRecord> dataRecords : groupedRecords) {
      if (numberOfRecords >= maxRecordsPerOutputFile) {
        uploadThenDeleteFile();
        newFile();
      }
      String record = base64Encode(getConfidentialMatchOutputDataRecord(dataRecords));
      try {
        Retry.decorateCheckedRunnable(PRINTER_RETRY, () -> writer.write(record + "\n")).run();
      } catch (Throwable e) {
        String message =
            "Serialized proto data writer threw an exception while writing to the file.";
        logger.error(message, e);
        deleteFile();
        throw new IOException(message, e);
      }
      numberOfRecords++;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      if (writer.checkError()) {
        String message = "Serialized Proto data writer failed to close/flush the output file.";
        logger.error(message);
        deleteFile();
        throw new JobProcessorException(message);
      }
    }
    if (numberOfRecords > 0) {
      uploadThenDeleteFile();
    }
    if (file != null && file.exists()) {
      deleteFile();
      logger.error("Temporary file exists in SerializedProtoDataWriter after all uploads done.");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Logger getLogger() {
    return logger;
  }

  /**
   * Groups together {@link DataRecord}s by their `ROW_MARKER_COLUMN_NAME` if present. If not
   * present, records will be grouped alone.
   */
  private List<List<DataRecord>> groupRecords(DataChunk dataChunk) {
    List<DataRecord> dataRecords = dataChunk.records();
    List<List<DataRecord>> dataRecordGroups = new ArrayList<>();
    if (rowMarkerIndex.isPresent()) {
      // Groups records together by row ID
      Map<String, List<DataRecord>> dataRecordGroupsMap = new HashMap<>();
      for (DataRecord dataRecord : dataRecords) {
        List<DataRecord> dataRecordGroup =
            dataRecordGroupsMap.computeIfAbsent(
                dataRecord.getKeyValues(rowMarkerIndex.get()).getStringValue(),
                k -> new ArrayList<>());
        dataRecordGroup.add(dataRecord);
      }
      dataRecordGroups.addAll(dataRecordGroupsMap.values());
    } else {
      // Records do not require grouping so each group will contain only a single DataRecord
      for (DataRecord dataRecord : dataRecords) {
        dataRecordGroups.add(Collections.singletonList(dataRecord));
      }
    }
    return dataRecordGroups;
  }

  /**
   * Constructs and returns a {@link ConfidentialMatchOutputDataRecord} from a group of {@link
   * DataRecord}s.
   */
  private ConfidentialMatchOutputDataRecord getConfidentialMatchOutputDataRecord(
      List<DataRecord> groupedRecords) {
    ConfidentialMatchOutputDataRecord.Builder confidentialMatchOutputDataRecordBuilder =
        ConfidentialMatchOutputDataRecord.newBuilder();
    boolean hasRowLevelEncryption =
        isValidIndex(groupedRecords.size(), 0)
            && groupedRecords.get(0).hasProcessingMetadata()
            && groupedRecords.get(0).getProcessingMetadata().getProtoEncryptionLevel() == ROW_LEVEL;
    Optional<EncryptionKey> rowLevelEncryptionKey =
        hasRowLevelEncryption ? getEncryptionKey(groupedRecords.get(0)) : Optional.empty();
    for (DataRecord dataRecord : groupedRecords) {
      Optional<EncryptionKey> encryptionKey =
          hasRowLevelEncryption ? Optional.empty() : getEncryptionKey(dataRecord);
      confidentialMatchOutputDataRecordBuilder.addAllMatchKeys(
          getMatchKeys(dataRecord, encryptionKey));
      confidentialMatchOutputDataRecordBuilder.addAllMetadata(getMetadata(dataRecord));
    }
    rowLevelEncryptionKey.ifPresent(confidentialMatchOutputDataRecordBuilder::setEncryptionKey);
    recordStatusFieldIndex.ifPresent(
        index ->
            confidentialMatchOutputDataRecordBuilder.setStatus(getStatus(groupedRecords, index)));
    return confidentialMatchOutputDataRecordBuilder.build();
  }

  /** Builds out an {@link EncryptionKey} for a provided {@link DataRecord}. */
  private Optional<EncryptionKey> getEncryptionKey(DataRecord dataRecord) {
    if (dataRecordEncryptionColumns.isPresent()) {
      var encryptionKeyBuilder = EncryptionKey.newBuilder();
      EncryptionKeyColumnIndices encryptionKeyColumnIndices =
          dataRecordEncryptionColumns.get().getEncryptionKeyColumnIndices();
      if (encryptionKeyColumnIndices.hasWrappedKeyColumnIndices()) {
        encryptionKeyBuilder.setWrappedKey(
            getGcpWrappedKey(dataRecord, encryptionKeyColumnIndices.getWrappedKeyColumnIndices()));
      } else if (encryptionKeyColumnIndices.hasCoordinatorKeyColumnIndices()) {
        encryptionKeyBuilder.setCoordinatorKey(
            getCoordinatorKey(
                dataRecord, encryptionKeyColumnIndices.getCoordinatorKeyColumnIndices()));
      }
      return Optional.of(encryptionKeyBuilder.build());
    }
    return Optional.empty();
  }

  /** Builds out a {@link GcpWrappedKey} from a provided {@link DataRecord}. */
  private GcpWrappedKey getGcpWrappedKey(
      DataRecord dataRecord, WrappedKeyColumnIndices wrappedKeyColumnIndices) {
    GcpWrappedKey.Builder gcpWrappedKeyBuilder = GcpWrappedKey.newBuilder();
    int dataRecordSize = dataRecord.getKeyValuesCount();

    // Set the Dek.
    if (!isValidIndex(dataRecordSize, wrappedKeyColumnIndices.getEncryptedDekColumnIndex())) {
      String message = "Missing EncryptedDekColumnIndex in DataRecord.";
      logger.error(message);
      throw new JobProcessorException(message, WRITER_MISSING_DEK);
    }
    gcpWrappedKeyBuilder.setEncryptedDek(
        dataRecord
            .getKeyValues(wrappedKeyColumnIndices.getEncryptedDekColumnIndex())
            .getStringValue());

    // Set the Kek.
    if (!isValidIndex(dataRecordSize, wrappedKeyColumnIndices.getKekUriColumnIndex())) {
      String message = "Missing KekUriColumnIndex index in DataRecord.";
      logger.error(message);
      throw new JobProcessorException(message, WRITER_MISSING_KEK);
    }
    gcpWrappedKeyBuilder.setKekUri(
        dataRecord.getKeyValues(wrappedKeyColumnIndices.getKekUriColumnIndex()).getStringValue());

    // Set the Wip if it is present.
    if (wrappedKeyColumnIndices.hasGcpColumnIndices()) {
      if (!isValidIndex(
          dataRecordSize, wrappedKeyColumnIndices.getGcpColumnIndices().getWipProviderIndex())) {
        String message = "Missing WipProviderIndex index in DataRecord.";
        logger.error(message);
        throw new JobProcessorException(message, WRITER_MISSING_WIP);
      }
      gcpWrappedKeyBuilder.setWip(
          dataRecord
              .getKeyValues(wrappedKeyColumnIndices.getGcpColumnIndices().getWipProviderIndex())
              .getStringValue());
    }
    return gcpWrappedKeyBuilder.build();
  }

  /** Builds out a {@link CoordinatorKey} from a provided {@link DataRecord}. */
  private CoordinatorKey getCoordinatorKey(
      DataRecord dataRecord, CoordinatorKeyColumnIndices coordinatorKeyColumnIndices) {
    int dataRecordSize = dataRecord.getKeyValuesCount();
    if (!isValidIndex(dataRecordSize, coordinatorKeyColumnIndices.getCoordinatorKeyColumnIndex())) {
      String message = "Missing CoordinatorKeyColumnIndex index in DataRecord.";
      logger.error(message);
      throw new JobProcessorException(message, WRITER_MISSING_COORDINATOR_KEY);
    }
    return CoordinatorKey.newBuilder()
        .setKeyId(
            dataRecord
                .getKeyValues(coordinatorKeyColumnIndices.getCoordinatorKeyColumnIndex())
                .getStringValue())
        .build();
  }

  /**
   * Gets a {@link MatchKey} list for a given {@link DataRecord}. All generated MatchKeys will be
   * built with the same {@link EncryptionKey}.
   */
  private List<MatchKey> getMatchKeys(
      DataRecord dataRecord, Optional<EncryptionKey> encryptionKey) {
    // Builds MatchKeys with Fields
    List<MatchKey> matchKeys =
        matchFieldIndices.stream()
            .filter(index -> isValidIndex(dataRecord.getKeyValuesCount(), index))
            .map(index -> convertToExternalKeyValue(dataRecord.getKeyValues(index)))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(
                keyValue -> {
                  var matchKeyBuilder =
                      MatchKey.newBuilder()
                          .setField(Field.newBuilder().setKeyValue(keyValue).build());
                  encryptionKey.ifPresent(matchKeyBuilder::setEncryptionKey);
                  return matchKeyBuilder.build();
                })
            .collect(Collectors.toList());
    // Builds MatchKeys with CompositeFields
    matchCompositeFieldGroupToIndices.forEach(
        (compositeFieldGroupNumber, indices) -> {
          var matchKeyBuilder =
              MatchKey.newBuilder()
                  .setCompositeField(
                      CompositeField.newBuilder()
                          .setKey(groupNumberToGroupAlias.get(compositeFieldGroupNumber))
                          .addAllChildFields(
                              indices.stream()
                                  .filter(
                                      index -> isValidIndex(dataRecord.getKeyValuesCount(), index))
                                  .map(
                                      index ->
                                          convertToExternalKeyValue(dataRecord.getKeyValues(index)))
                                  .filter(Optional::isPresent)
                                  .map(Optional::get)
                                  .map(
                                      keyValue ->
                                          CompositeChildField.newBuilder()
                                              .setKeyValue(keyValue)
                                              .build())
                                  .collect(Collectors.toList())));
          encryptionKey.ifPresent(matchKeyBuilder::setEncryptionKey);
          if (matchKeyBuilder.getCompositeField().getChildFieldsCount() > 0) {
            matchKeys.add(matchKeyBuilder.build());
          }
        });
    return matchKeys;
  }

  /** Returns a mapping of column group numbers to their group alias */
  private ImmutableMap<Integer, String> getGroupNumberToGroupAlias(
      MatchConfig matchConfig, Schema schema) {
    ImmutableMap<String, String> matchColumnAliasToCompositeFieldAlias =
        matchConfig.getMatchConditionsList().stream()
            .filter(matchCondition -> matchCondition.getDataSource1Column().getColumnsCount() > 1)
            .flatMap(
                matchCondition ->
                    matchCondition.getDataSource1Column().getColumnsList().stream()
                        .map(
                            column ->
                                entry(
                                    column.getColumnAlias(),
                                    matchCondition.getDataSource1Column().getColumnAlias())))
            .collect(
                ImmutableMap.toImmutableMap(
                    Entry::getKey, Entry::getValue, (existingValue, newValue) -> existingValue));

    return schema.getColumnsList().stream()
        .filter(
            column ->
                column.hasColumnGroup()
                    && column.hasColumnAlias()
                    && matchColumnAliasToCompositeFieldAlias.containsKey(column.getColumnAlias()))
        .map(
            column ->
                entry(
                    column.getColumnGroup(),
                    matchColumnAliasToCompositeFieldAlias.get(column.getColumnAlias())))
        .collect(
            ImmutableMap.toImmutableMap(
                Entry::getKey, Entry::getValue, (existingValue, newValue) -> existingValue));
  }

  /** Returns a mapping of column names to their alias from a {@link Schema}. */
  private ImmutableMap<String, String> getSchemaColumnNamesToAliases(Schema schema) {
    return schema.getColumnsList().stream()
        .filter(Column::hasColumnAlias)
        .collect(ImmutableMap.toImmutableMap(Column::getColumnName, Column::getColumnAlias));
  }

  /** Returns a mapping of column names to their group number from a {@link Schema}. */
  private ImmutableMap<String, Integer> getSchemaColumnNamesToGroupNumber(Schema schema) {
    return schema.getColumnsList().stream()
        .filter(Column::hasColumnGroup)
        .map(column -> entry(column.getColumnName(), column.getColumnGroup()))
        .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
  }

  /** Gets the metadata fields from a {@link DataRecord}. */
  private List<KeyValue> getMetadata(DataRecord dataRecord) {
    int size = dataRecord.getKeyValuesCount();
    return metadataIndices.stream()
        .filter(index -> isValidIndex(size, index))
        .map(index -> convertToExternalKeyValue(dataRecord.getKeyValues(index)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  /** Returns SUCCESS or the first located status field error from a {@link DataRecord} group. */
  private String getStatus(List<DataRecord> groupedRecords, int index) {
    if (!isValidIndex(groupedRecords.get(0).getKeyValuesCount(), index)) {
      String message = "Missing status index in DataRecord.";
      logger.error(message);
      throw new JobProcessorException(message);
    }
    return groupedRecords.stream()
        .filter(dataRecord -> isValidIndex(dataRecord.getKeyValuesCount(), index))
        .map(dataRecord -> dataRecord.getKeyValues(index).getStringValue())
        .filter(jobResult -> SUCCESS != JobResultCode.valueOf(jobResult))
        .findAny()
        .orElse(SUCCESS.toString());
  }

  /** Returns a serialized and base64 encoded {@link ConfidentialMatchOutputDataRecord}. */
  private static String base64Encode(
      ConfidentialMatchOutputDataRecord confidentialMatchOutputDataRecord) {
    try {
      return Base64Util.toBase64String(confidentialMatchOutputDataRecord.toByteString());
    } catch (Exception e) {
      String message =
          "Unexpected error during Base64 encoding of ConfidentialMatchOutputDataRecord.";
      logger.error(message, e);
      throw new JobProcessorException(message, e, OUTPUT_FILE_WRITE_ERROR);
    }
  }

  /** Returns a map of column names to their index for a {@link Schema}. */
  private ImmutableMap<String, Integer> getSchemaColumnIndexMap(Schema schema) {
    return IntStream.range(0, schema.getColumnsCount())
        .boxed()
        .collect(
            toImmutableMap(index -> schema.getColumns(index).getColumnName(), Function.identity()));
  }

  /**
   * Gets the set of {@link Schema} indices used for matching based on their alias in the {@link
   * MatchConfig}.
   */
  private ImmutableSet<Integer> getMatchFieldIndices(
      MatchConfig matchConfig,
      ImmutableMap<String, Integer> schemaMap,
      ImmutableMap<String, String> nameToAliasMap) {
    ImmutableSet<String> matchAliases =
        matchConfig.getMatchConditionsList().stream()
            .filter(matchCondition -> matchCondition.getDataSource1Column().getColumnsCount() == 1)
            .map(matchCondition -> matchCondition.getDataSource1Column().getColumnAlias())
            .collect(ImmutableSet.toImmutableSet());

    return schemaMap.entrySet().stream()
        .filter(
            entry -> {
              String columnName = entry.getKey();
              return nameToAliasMap.containsKey(columnName)
                  && matchAliases.contains(nameToAliasMap.get(columnName));
            })
        .map(Entry::getValue)
        .collect(ImmutableSet.toImmutableSet());
  }

  /**
   * Gets a mapping of compositeColumn group numbers to the set of their corresponding column
   * indices in the {@link Schema}.
   */
  private ImmutableMap<Integer, ImmutableSet<Integer>> getMatchCompositeFieldGroupNumberToIndices(
      ImmutableMap<String, Integer> schemaMap, ImmutableMap<String, Integer> nameToGroupNumberMap) {

    Map<Integer, ImmutableSet<Integer>> compositeColumnGroupNumberToColumnIndices =
        schemaMap.entrySet().stream()
            .filter(entrySet -> nameToGroupNumberMap.containsKey(entrySet.getKey()))
            .collect(
                Collectors.groupingBy(
                    entrySet -> nameToGroupNumberMap.get(entrySet.getKey()),
                    Collectors.mapping(Entry::getValue, ImmutableSet.toImmutableSet())));

    return ImmutableMap.copyOf(compositeColumnGroupNumberToColumnIndices);
  }

  /**
   * If present, gets a {@link DataRecordEncryptionColumns} with the indices from the {@link Schema}
   * for encryption key columns specified in the {@link MatchConfig}.
   */
  private Optional<DataRecordEncryptionColumns> getDataRecordEncryptionColumns(
      MatchConfig matchConfig,
      ImmutableMap<String, Integer> schemaMap,
      ImmutableMap<String, String> nameToAliasMap) {

    // Maps alias to index. This is valid for encryption key data since only one set of encryption
    // keys can be in a DataRecord.
    ImmutableMap.Builder<String, Integer> aliasToIndexBuilder = new ImmutableMap.Builder<>();
    schemaMap.forEach(
        (columnName, index) -> {
          if (nameToAliasMap.containsKey(columnName)) {
            String columnAlias = nameToAliasMap.get(columnName);
            aliasToIndexBuilder.put(columnAlias, index);
          }
        });
    ImmutableMap<String, Integer> aliasToIndexMap = aliasToIndexBuilder.build();

    // Creates the DataRecordEncryptionColumns
    var encryptionKeyColumnIndicesBuilder = EncryptionKeyColumnIndices.newBuilder();
    EncryptionKeyColumns encryptionKeyColumns = matchConfig.getEncryptionKeyColumns();
    if (encryptionKeyColumns.hasWrappedKeyColumns()
        && aliasToIndexMap.containsKey(
            encryptionKeyColumns.getWrappedKeyColumns().getEncryptedDekColumnAlias())
        && aliasToIndexMap.containsKey(
            encryptionKeyColumns.getWrappedKeyColumns().getKekUriColumnAlias())) {
      WrappedKeyColumns wrappedKeyColumns = encryptionKeyColumns.getWrappedKeyColumns();
      var wrappedKeyIndicesBuilder =
          WrappedKeyColumnIndices.newBuilder()
              .setEncryptedDekColumnIndex(
                  aliasToIndexMap.get(wrappedKeyColumns.getEncryptedDekColumnAlias()))
              .setKekUriColumnIndex(aliasToIndexMap.get(wrappedKeyColumns.getKekUriColumnAlias()));
      if (wrappedKeyColumns.hasGcpWrappedKeyColumns()
          && aliasToIndexMap.containsKey(
              wrappedKeyColumns.getGcpWrappedKeyColumns().getWipProviderAlias())) {
        wrappedKeyIndicesBuilder.setGcpColumnIndices(
            GcpWrappedKeyColumnIndices.newBuilder()
                .setWipProviderIndex(
                    aliasToIndexMap.get(
                        wrappedKeyColumns.getGcpWrappedKeyColumns().getWipProviderAlias())));
      }
      encryptionKeyColumnIndicesBuilder.setWrappedKeyColumnIndices(wrappedKeyIndicesBuilder);
    } else if (encryptionKeyColumns.hasCoordinatorKeyColumn()
        && aliasToIndexMap.containsKey(
            encryptionKeyColumns.getCoordinatorKeyColumn().getCoordinatorKeyColumnAlias())) {
      encryptionKeyColumnIndicesBuilder.setCoordinatorKeyColumnIndices(
          CoordinatorKeyColumnIndices.newBuilder()
              .setCoordinatorKeyColumnIndex(
                  aliasToIndexMap.get(
                      encryptionKeyColumns
                          .getCoordinatorKeyColumn()
                          .getCoordinatorKeyColumnAlias())));
    } else {
      return Optional.empty();
    }

    return Optional.of(
        DataRecordEncryptionColumns.newBuilder()
            .setEncryptionKeyColumnIndices(encryptionKeyColumnIndicesBuilder.build())
            .build());
  }

  /**
   * If it exists, gets the {@link Schema} index of the recordStatusFieldName specified in the
   * {@link MatchConfig}.
   */
  private Optional<Integer> getRecordStatusFieldIndex(
      MatchConfig matchConfig, ImmutableMap<String, Integer> schemaMap) {
    return Optional.of(matchConfig.getSuccessConfig())
        .map(SuccessConfig::getPartialSuccessAttributes)
        .map(PartialSuccessAttributes::getRecordStatusFieldName)
        .map(schemaMap::get);
  }

  /** Gets the set of {@link Schema} indices for metadata columns. */
  private ImmutableSet<Integer> getMetadataIndices(ImmutableMap<String, Integer> schemaMap) {
    Set<Integer> excludedIndices = new HashSet<>(matchFieldIndices);
    matchCompositeFieldGroupToIndices.values().forEach(excludedIndices::addAll);
    recordStatusFieldIndex.ifPresent(excludedIndices::add);
    rowMarkerIndex.ifPresent(excludedIndices::add);

    // Handles excluding encryption column indices
    dataRecordEncryptionColumns.ifPresent(
        dataRecordEncryptionColumns -> {
          EncryptionKeyColumnIndices encryptionKeyColumnIndices =
              dataRecordEncryptionColumns.getEncryptionKeyColumnIndices();
          if (encryptionKeyColumnIndices.hasWrappedKeyColumnIndices()) {
            WrappedKeyColumnIndices wrappedKeyColumnIndices =
                encryptionKeyColumnIndices.getWrappedKeyColumnIndices();
            excludedIndices.add(wrappedKeyColumnIndices.getEncryptedDekColumnIndex());
            excludedIndices.add(wrappedKeyColumnIndices.getKekUriColumnIndex());
            if (wrappedKeyColumnIndices.hasGcpColumnIndices()) {
              excludedIndices.add(
                  wrappedKeyColumnIndices.getGcpColumnIndices().getWipProviderIndex());
            }
          } else if (encryptionKeyColumnIndices.hasCoordinatorKeyColumnIndices()) {
            excludedIndices.add(
                encryptionKeyColumnIndices
                    .getCoordinatorKeyColumnIndices()
                    .getCoordinatorKeyColumnIndex());
          }
        });

    return schemaMap.values().stream()
        .filter(index -> !excludedIndices.contains(index))
        .collect(toImmutableSet());
  }

  /** Converts from a {@link DataRecord.KeyValue} to a {@link KeyValue}. */
  private Optional<KeyValue> convertToExternalKeyValue(DataRecord.KeyValue internalKeyValue) {
    var externalKeyValue = KeyValue.newBuilder().setKey(internalKeyValue.getKey());
    switch (internalKeyValue.getValueCase()) {
      case STRING_VALUE:
        return !internalKeyValue.getStringValue().isEmpty()
            ? Optional.of(
                externalKeyValue.setStringValue(internalKeyValue.getStringValue()).build())
            : Optional.empty();
      case DOUBLE_VALUE:
        return Optional.of(
            externalKeyValue.setDoubleValue(internalKeyValue.getDoubleValue()).build());
      case BOOL_VALUE:
        return Optional.of(externalKeyValue.setBoolValue(internalKeyValue.getBoolValue()).build());
      case INT_VALUE:
        return Optional.of(externalKeyValue.setIntValue(internalKeyValue.getIntValue()).build());
      case VALUE_NOT_SET:
      default:
        return Optional.empty();
    }
  }

  /** Helper method to verify indices before lookup. */
  private boolean isValidIndex(int size, int index) {
    return index >= 0 && index < size;
  }

  private void uploadThenDeleteFile() {
    writer.close();
    if (writer.checkError()) {
      String message = "Serialized Proto data writer failed to close/flush the output file.";
      logger.error(message);
      deleteFile();
      throw new JobProcessorException(message);
    }

    try {
      dataDestination.write(file, getFilename(name, fileNumber));
    } catch (Exception e) {
      String message = "Serialized proto data writer threw an exception while uploading the file.";
      logger.error(message, e);
      throw new JobProcessorException(message, e);
    } finally {
      deleteFile();
    }
  }

  private void newFile() throws IOException {
    fileNumber++;
    numberOfRecords = 0;
    file = File.createTempFile("mrp", "");
    file.deleteOnExit();
    try {
      writer = new PrintWriter(file);
    } catch (IOException ex) {
      writer.close(); // does not throw
      String message =
          writer.checkError()
              ? "Writer failed to flush/close and IO Exception encountered."
              : "IO Exception encountered.";
      logger.error(message, ex);
      throw ex;
    }
  }
}
