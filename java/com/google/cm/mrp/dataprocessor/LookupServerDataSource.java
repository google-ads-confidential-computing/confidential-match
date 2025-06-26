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

import static com.google.cm.lookupserver.api.LookupProto.LookupRequest.KeyFormat.KEY_FORMAT_HASHED;
import static com.google.cm.lookupserver.api.LookupProto.LookupRequest.KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_PROCESSING_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_FAILURE;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_INVALID_ASSOCIATED_DATA;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_INVALID_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_MODE_ERROR;
import static com.google.cm.mrp.dataprocessor.models.MatchColumnIndices.Kind.SINGLE_COLUMN_INDICES;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;

import com.google.cm.lookupserver.api.LookupProto;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.cm.lookupserver.api.LookupProto.LookupResult.Status;
import com.google.cm.lookupserver.api.LookupProto.MatchedDataRecord;
import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.EncryptionKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.CoordinatorKey;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.ModeProto.Mode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient.LookupServiceClientException;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientRequest;
import com.google.cm.mrp.dataprocessor.converters.EncryptionMetadataConverter;
import com.google.cm.mrp.dataprocessor.converters.ErrorCodeConverter;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.LookupDataSourceResult;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformer;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerFactory;
import com.google.cm.mrp.models.JobParameters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.io.BaseEncoding;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete class representing lookup data sources that can be called using an endpoint. Lookup data
 * sources can be looked up using LookupServiceClient.
 */
public final class LookupServerDataSource implements LookupDataSource {

  private static final Logger logger = LoggerFactory.getLogger(LookupServerDataSource.class);

  /** Matched dataSource2 fields are categorized as "pii_value" */
  static final String PII_VALUE = "pii_value";

  private final LookupServiceClient lookupServiceClient;
  private final DataRecordTransformerFactory dataRecordTransformerFactory;
  private final Optional<CryptoClient> cryptoClient;
  private final JobParameters jobParameters;
  private final MatchConfig matchConfig;
  private final SuccessConfig successConfig;
  private final FeatureFlags featureFlags;

  /** Constructor for {@link LookupServerDataSource}. */
  @AssistedInject
  public LookupServerDataSource(
      LookupServiceClient lookupServiceClient,
      DataRecordTransformerFactory dataRecordTransformerFactory,
      @Assisted MatchConfig matchConfig,
      @Assisted FeatureFlags featureFlags,
      @Assisted JobParameters jobParameters) {
    this.lookupServiceClient = lookupServiceClient;
    this.dataRecordTransformerFactory = dataRecordTransformerFactory;
    this.matchConfig = matchConfig;
    this.jobParameters = jobParameters;
    this.featureFlags = featureFlags;
    this.successConfig = matchConfig.getSuccessConfig();
    this.cryptoClient = Optional.empty();
  }

  /** Constructor for {@link LookupServerDataSource}. */
  @AssistedInject
  public LookupServerDataSource(
      LookupServiceClient lookupServiceClient,
      DataRecordTransformerFactory dataRecordTransformerFactory,
      @Assisted MatchConfig matchConfig,
      @Assisted CryptoClient cryptoClient,
      @Assisted FeatureFlags featureFlags,
      @Assisted JobParameters jobParameters) {
    this.lookupServiceClient = lookupServiceClient;
    this.dataRecordTransformerFactory = dataRecordTransformerFactory;
    this.jobParameters = jobParameters;
    this.matchConfig = matchConfig;
    this.featureFlags = featureFlags;
    this.successConfig = matchConfig.getSuccessConfig();
    this.cryptoClient = Optional.of(cryptoClient);
  }

  /**
   * This method returns the lookup results after retrieving data from the underlying {@link
   * LookupDataSource}.
   *
   * @return {@link DataChunk}
   */
  @Override
  public LookupDataSourceResult lookup(
      DataChunk dataChunk, Optional<EncryptionMetadata> encryptionMetadata)
      throws LookupServiceClientException {

    if (dataChunk.records().isEmpty()) {
      return LookupDataSourceResult.create(
          DataChunk.builder().setSchema(getResponseSchema()).build());
    } else {
      MatchColumnsList matchColumnsList =
          MatchColumnsList.generateMatchColumnsListForDataSource1(dataChunk.schema(), matchConfig);
      int piisPerRow = matchColumnsList.countPiis();
      DataRecordTransformer dataRecordTransformer =
          dataRecordTransformerFactory.create(matchConfig, dataChunk.schema(), jobParameters);

      if (encryptionMetadata.isEmpty()) {
        return buildAndSendHashedRequestToLookupService(
            dataChunk, piisPerRow, matchColumnsList, dataRecordTransformer);
      } else if (encryptionMetadata.get().getEncryptionKeyInfo().hasCoordinatorKeyInfo()
          && featureFlags.coordinatorBatchEncryptionEnabled()) {
        return buildAndSendBatchEncryptedRequestsToLookupService(
            encryptionMetadata.get(), dataChunk, matchColumnsList, dataRecordTransformer);
      } else {
        return buildAndSendEncryptedRequestsToLookupService(
            encryptionMetadata.get(), dataChunk, matchColumnsList, dataRecordTransformer);
      }
    }
  }

  /*
   *  Parses data from hashed data source and sends to the lookup service
   */
  @SuppressWarnings("UnstableApiUsage")
  private LookupDataSourceResult buildAndSendHashedRequestToLookupService(
      DataChunk dataChunk,
      int piisPerRow,
      MatchColumnsList matchColumnsList,
      DataRecordTransformer dataRecordTransformer)
      throws LookupServiceClientException {
    var resultsBuilder = DataChunk.builder().setSchema(getResponseSchema());

    // now we need to allocate enough space to hold all PIIs for every row
    ImmutableList.Builder<LookupDataRecord> piis =
        ImmutableList.builderWithExpectedSize(dataChunk.records().size() * piisPerRow);

    // now we are ready to add all single-column pii values and multi-column pii values to a
    // common list
    dataChunk.records().stream()
        .filter(dataRecord -> !dataRecord.hasErrorCode())
        .map(dataRecordTransformer::transform)
        .forEach(record -> addColumnValuesToBuilder(matchColumnsList, record, piis));

    var piisList = piis.build();
    if (piisList.isEmpty()) {
      return LookupDataSourceResult.create(resultsBuilder.setRecords(ImmutableList.of()).build());
    } else {
      LookupServiceClientRequest lookupRequest =
          LookupServiceClientRequest.builder()
              .setKeyFormat(KEY_FORMAT_HASHED)
              .setAssociatedDataKeys(getAssociatedDataKeys())
              .setRecords(piisList)
              .build();
      var lookupResults = lookupServiceClient.lookupRecords(lookupRequest).results();
      // Group LookupResults by status
      Map<Status, List<LookupResult>> lookupResultsByStatus =
          lookupResults.stream().collect(groupingBy(LookupResult::getStatus));
      // Make sure there are no unrecognized statuses
      validateLookupStatuses(lookupResultsByStatus);
      if (lookupResultsByStatus.containsKey(Status.STATUS_FAILED)) {
        // TODO(b/349668162): Add partial successes to hashed data requests
        String message =
            "Lookup server results contain a failed result but partial success not allowed for"
                + " job.";
        throw new JobProcessorException(message, LOOKUP_SERVICE_FAILURE);
      }

      // Only process successful results
      var dataRecords =
          lookupResultsByStatus.getOrDefault(Status.STATUS_SUCCESS, ImmutableList.of()).stream()
              .filter(lookupResult -> lookupResult.getMatchedDataRecordsCount() > 0)
              .flatMap(this::toDataRecords)
              .collect(ImmutableList.toImmutableList());

      return LookupDataSourceResult.create(resultsBuilder.setRecords(dataRecords).build());
    }
  }

  /*
   *  Parses data from encrypted data source and groups it to send to the lookup service
   */
  @SuppressWarnings("UnstableApiUsage")
  private LookupDataSourceResult buildAndSendEncryptedRequestsToLookupService(
      EncryptionMetadata encryptionMetadata,
      DataChunk dataChunk,
      MatchColumnsList matchColumnsList,
      DataRecordTransformer dataRecordTransformer)
      throws LookupServiceClientException {

    // The outer map is needed because records are likely to repeat DataRecordEncryptionKeys (DEK
    // and KEK).
    // A further map is to be able to quickly query for the original LookupDataRecord after we
    // receive the results from the Lookup Service.
    Map<DataRecordEncryptionKeys, Map<String, LookupDataRecord>> keyToRecordsMap = new HashMap<>();

    var encryptionColumns =
        dataChunk
            .encryptionColumns()
            .orElseThrow(
                () -> {
                  String message = "DataChunk missing encryption columns";
                  logger.error(message);
                  throw new JobProcessorException(message, ENCRYPTION_COLUMNS_PROCESSING_ERROR);
                });
    var encryptedColumnIndices =
        ImmutableSet.copyOf(encryptionColumns.getEncryptedColumnIndicesList());

    // Build mappings of DEK+KEK+ Maybe WIP (stored in DataRecordEncryptionKeys proto) to PII.
    // This allows us to send batch requests to Lookup Server
    dataChunk.records().stream()
        .filter(dataRecord -> !dataRecord.hasErrorCode())
        .map(dataRecordTransformer::transform)
        .forEach(
            record -> {
              // Get encryptionKeys for this record
              DataRecordEncryptionKeys encryptionKeys =
                  getDataRecordEncryptionKeys(
                      encryptionMetadata.getEncryptionKeyInfo(),
                      record,
                      encryptionColumns.getEncryptionKeyColumnIndices(),
                      encryptedColumnIndices);

              var piisMap = keyToRecordsMap.getOrDefault(encryptionKeys, new HashMap<>());

              // Get all the PIIs for this record
              addEncryptedColumnValuesToBuilder(
                  matchColumnsList, record, encryptedColumnIndices, encryptionKeys, piisMap);

              // Add PIIs to existing mapping
              keyToRecordsMap.put(encryptionKeys, piisMap);
            });

    // Build result list with expected size to avoid reallocation
    ImmutableList.Builder<DataRecord> lookupResults =
        ImmutableList.builderWithExpectedSize(keyToRecordsMap.size());
    // Error list size is not predictable
    ImmutableList.Builder<DataRecord> erroredDataRecords = ImmutableList.builder();
    // Now we send lookup server requests in batches
    // We expect a small number of keys (and even all the same key), so it's ok not to parallelize
    for (DataRecordEncryptionKeys encryptionKeys : keyToRecordsMap.keySet()) {
      var piisMap = keyToRecordsMap.get(encryptionKeys);
      if (!piisMap.isEmpty()) {
        // Build requests with KmsMetadata
        LookupServiceClientRequest lookupRequest =
            LookupServiceClientRequest.builder()
                .setKeyFormat(KEY_FORMAT_HASHED_ENCRYPTED)
                .setEncryptionKeyInfo(
                    EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
                        encryptionKeys, encryptionMetadata))
                .setRecords(piisMap.values().stream().collect(ImmutableList.toImmutableList()))
                .setAssociatedDataKeys(getAssociatedDataKeys())
                .build();
        var lookupServiceClientResults = lookupServiceClient.lookupRecords(lookupRequest).results();

        // Group results by status
        Map<Status, List<LookupResult>> lookupResultsByStatus =
            lookupServiceClientResults.stream().collect(groupingBy(LookupResult::getStatus));
        // Make sure there are no unrecognized statuses
        validateLookupStatuses(lookupResultsByStatus);
        if (lookupResultsByStatus.containsKey(Status.STATUS_FAILED)) {
          // Get lookup results with errors and map them to original PII
          erroredDataRecords.addAll(
              getDataRecordFromLookupResultsWithErrors(
                  lookupResultsByStatus.get(Status.STATUS_FAILED), Optional.of(piisMap)));
        }

        // Only processes successful lookup results here
        List<DataRecord> resultRecords =
            lookupResultsByStatus.getOrDefault(Status.STATUS_SUCCESS, ImmutableList.of()).stream()
                .filter(lookupResult -> lookupResult.getMatchedDataRecordsCount() > 0)
                .flatMap(lookupResult -> toDataRecords(lookupResult, piisMap))
                .collect(Collectors.toList());
        lookupResults.addAll(resultRecords);
      }
    }
    return constructLookupDataSourceResultWithError(
        lookupResults.build(), erroredDataRecords.build());
  }

  /*
   * Parses data from encrypted data source and groups it to send to the lookup service with batch
   * encryption.
   */
  private LookupDataSourceResult buildAndSendBatchEncryptedRequestsToLookupService(
      EncryptionMetadata encryptionMetadata,
      DataChunk dataChunk,
      MatchColumnsList matchColumnsList,
      DataRecordTransformer dataRecordTransformer)
      throws LookupServiceClientException {
    Map<DataRecordEncryptionKeys, ImmutableList.Builder<LookupDataRecord>> keyToRecordsBuilderMap =
        new HashMap<>();

    if (cryptoClient.isEmpty()) {
      String message = "Batch encryption with encrypted records missing CryptoClient.";
      logger.error(message);
      throw new JobProcessorException(message, CRYPTO_CLIENT_CONFIGURATION_ERROR);
    }

    var encryptionColumns =
        dataChunk
            .encryptionColumns()
            .orElseThrow(
                () -> {
                  String message = "DataChunk missing encryption columns";
                  logger.error(message);
                  throw new JobProcessorException(message, ENCRYPTION_COLUMNS_PROCESSING_ERROR);
                });
    var encryptedColumnIndices =
        ImmutableSet.copyOf(encryptionColumns.getEncryptedColumnIndicesList());

    // Build mappings of encryption keys (stored in DataRecordEncryptionKeys proto) to PII.
    // This allows us to send batch requests to Lookup Server.
    dataChunk.records().stream()
        .filter(dataRecord -> !dataRecord.hasErrorCode())
        .map(dataRecordTransformer::transform)
        .forEach(
            record -> {
              // Get encryptionKeys for this record
              DataRecordEncryptionKeys encryptionKeys =
                  getDataRecordEncryptionKeys(
                      encryptionMetadata.getEncryptionKeyInfo(),
                      record,
                      encryptionColumns.getEncryptionKeyColumnIndices(),
                      encryptedColumnIndices);

              var lookupRecords =
                  keyToRecordsBuilderMap.getOrDefault(encryptionKeys, ImmutableList.builder());
              addColumnValuesToBuilder(matchColumnsList, record, lookupRecords);
              // Add PIIs to existing mapping
              keyToRecordsBuilderMap.put(encryptionKeys, lookupRecords);
            });

    Map<DataRecordEncryptionKeys, ImmutableList> keyToRecordsMap =
        keyToRecordsBuilderMap.entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().build()));
    // Build result list with expected size to avoid reallocation
    ImmutableList.Builder<DataRecord> lookupResults =
        ImmutableList.builderWithExpectedSize(keyToRecordsBuilderMap.size());
    // Error list size is not predictable
    ImmutableList.Builder<DataRecord> erroredDataRecords = ImmutableList.builder();
    // Now we send lookup server requests in batches
    // We expect a small number of keys (and even the same key), so it's ok not to parallelize
    for (DataRecordEncryptionKeys encryptionKey : keyToRecordsMap.keySet()) {
      ImmutableList<LookupDataRecord> piiList = keyToRecordsMap.get(encryptionKey);
      if (!piiList.isEmpty()) {
        // Build requests with KmsMetadata
        LookupServiceClientRequest lookupRequest =
            LookupServiceClientRequest.builder()
                .setKeyFormat(KEY_FORMAT_HASHED_ENCRYPTED)
                .setEncryptionKeyInfo(
                    EncryptionMetadataConverter.convertToLookupEncryptionKeyInfo(
                        encryptionKey, encryptionMetadata))
                .setRecords(piiList.stream().collect(ImmutableList.toImmutableList()))
                .setCryptoClient(cryptoClient.get())
                .setAssociatedDataKeys(getAssociatedDataKeys())
                .build();
        var lookupServiceClientResults = lookupServiceClient.lookupRecords(lookupRequest).results();
        // Group results by status
        Map<Status, List<LookupResult>> lookupResultsByStatus =
            lookupServiceClientResults.stream().collect(groupingBy(LookupResult::getStatus));
        // Make sure there are no unrecognized statuses
        validateLookupStatuses(lookupResultsByStatus);
        if (lookupResultsByStatus.containsKey(Status.STATUS_FAILED)) {
          // Get lookup results with errors and map them to original PII
          erroredDataRecords.addAll(
              getDataRecordFromLookupResultsWithErrors(
                  lookupResultsByStatus.get(Status.STATUS_FAILED), Optional.empty()));
        }

        // Only processes successful lookup results here
        List<DataRecord> resultRecords =
            lookupResultsByStatus.getOrDefault(Status.STATUS_SUCCESS, ImmutableList.of()).stream()
                .filter(lookupResult -> lookupResult.getMatchedDataRecordsCount() > 0)
                .flatMap(lookupResult -> toDataRecords(lookupResult))
                .collect(Collectors.toList());
        lookupResults.addAll(resultRecords);
      }
    }

    return constructLookupDataSourceResultWithError(
        lookupResults.build(), erroredDataRecords.build());
  }

  /* Get associatedData keys to send with every LookupClient request. Only for Join mode */
  private List<String> getAssociatedDataKeys() {
    if (jobParameters.mode() != Mode.JOIN) {
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
   * Adds the PII values of each data source column to a list builder of records
   */
  private void addColumnValuesToBuilder(
      MatchColumnsList matchColumnsList,
      DataRecord dataRecord,
      ImmutableList.Builder<LookupDataRecord> records) {
    matchColumnsList
        .getList()
        .forEach(
            matchColumnIndices -> {
              if (matchColumnIndices.getKind().equals(SINGLE_COLUMN_INDICES)) {
                addCompositeSingleColumn(
                    records, dataRecord, matchColumnIndices.singleColumnIndices().indicesList());
              } else {
                addCompositeMultiColumn(
                    records,
                    dataRecord,
                    matchColumnIndices.columnGroupIndices().columnGroupIndicesMultimap());
              }
            });
  }

  /*
   * Adds the PII values of each data source column to a list builder of records, for encrypted requests
   */
  private void addEncryptedColumnValuesToBuilder(
      MatchColumnsList matchColumnsList,
      DataRecord dataRecord,
      Set<Integer> encryptedColumnIndices,
      DataRecordEncryptionKeys encryptionKeys,
      Map<String, LookupDataRecord> piiMap) {
    matchColumnsList
        .getList()
        .forEach(
            matchColumnIndices -> {
              if (matchColumnIndices.getKind().equals(SINGLE_COLUMN_INDICES)) {
                var indexList = matchColumnIndices.singleColumnIndices().indicesList();
                addCompositeSingleColumn(piiMap, dataRecord, indexList, encryptedColumnIndices);
              } else {
                var columnGroupsMultimap =
                    matchColumnIndices.columnGroupIndices().columnGroupIndicesMultimap();
                addCompositeMultiColumn(piiMap, dataRecord, columnGroupsMultimap, encryptionKeys);
              }
            });
  }

  private Schema getResponseSchema() {
    Schema.Builder builder =
        Schema.newBuilder()
            .addColumns(
                Schema.Column.newBuilder().setColumnName(PII_VALUE).setColumnAlias(PII_VALUE));
    for (String associatedDataKey : getAssociatedDataKeys()) {
      builder.addColumns(
          Schema.Column.newBuilder()
              .setColumnName(associatedDataKey)
              .setColumnAlias(associatedDataKey));
    }
    return builder.build();
  }

  /*
   * Convert encrypted lookup results to DataRecords
   */
  private Stream<DataRecord> toDataRecords(
      LookupResult lookupResult, Map<String, LookupDataRecord> lookupDataRecordMap) {
    return lookupResult.getMatchedDataRecordsList().stream()
        .map(
            matchedDataRecord -> {
              String lookupResponseKey = matchedDataRecord.getLookupKey().getKey();
              String piiValue = getOriginalPiiValue(lookupResponseKey, lookupDataRecordMap);
              return toDataRecord(piiValue, matchedDataRecord);
            });
  }

  private Stream<DataRecord> toDataRecords(LookupResult lookupResult) {
    return lookupResult.getMatchedDataRecordsList().stream()
        .map(
            matchedDataRecord ->
                toDataRecord(matchedDataRecord.getLookupKey().getKey(), matchedDataRecord));
  }

  private DataRecord toDataRecord(String value, MatchedDataRecord matchedDataRecord) {
    return DataRecord.newBuilder()
        .addKeyValues(DataRecord.KeyValue.newBuilder().setKey(PII_VALUE).setStringValue(value))
        .addAllKeyValues(
            matchedDataRecord.getAssociatedDataList().stream()
                .map(this::toMrpKeyValue)
                .collect(Collectors.toList()))
        .build();
  }

  private DataRecord.KeyValue toMrpKeyValue(LookupProto.KeyValue lookupKeyValue) {
    var builder = DataRecord.KeyValue.newBuilder()
        .setKey(lookupKeyValue.getKey());

    switch (lookupKeyValue.getValueCase()) {
      case STRING_VALUE:
        return builder.setStringValue(lookupKeyValue.getStringValue()).build();
      case BYTES_VALUE:
        String encodedValue = BaseEncoding.base64().encode(lookupKeyValue.getBytesValue().toByteArray());
        return builder.setStringValue(encodedValue).build();
      case INT_VALUE:
      case DOUBLE_VALUE:
      case BOOL_VALUE:
      case VALUE_NOT_SET:
      default:
        String message = "Lookup associated data in an unexpected value type: " + lookupKeyValue.getValueCase().name();
        logger.error(message);
        throw new JobProcessorException(message, LOOKUP_SERVICE_INVALID_ASSOCIATED_DATA);
    }
  }

  private String getOriginalPiiValue(
      String lookupResponseKey, Map<String, LookupDataRecord> lookupDataRecordMap) {
    // Look for original request dataRecord
    if (!lookupDataRecordMap.containsKey(lookupResponseKey)) {
      // Should not happen
      String message = "Lookup result key not found in original data record";
      logger.error(message);
      throw new JobProcessorException(message, LOOKUP_SERVICE_FAILURE);
    }
    LookupDataRecord originalDataRecord = lookupDataRecordMap.get(lookupResponseKey);

    // If original dataRecord had a decryptedKey, use this instead
    return originalDataRecord.getLookupKey().hasDecryptedKey()
        ? originalDataRecord.getLookupKey().getDecryptedKey()
        : lookupResponseKey;
  }

  private ImmutableList<DataRecord> getDataRecordFromLookupResultsWithErrors(
      List<LookupResult> failedLookupResults,
      Optional<Map<String, LookupDataRecord>> lookupDataRecordMap) {
    // First check if we need to handle row-level errors
    if (!allowPartialSuccess()) {
      String message =
          "Lookup server results contain a failed result but partial success not allowed for"
              + " job.";
      throw new JobProcessorException(message, LOOKUP_SERVICE_FAILURE);
    } else {
      // Create new list for DataRecords
      ImmutableList.Builder<DataRecord> erroredDataRecords =
          ImmutableList.builderWithExpectedSize(failedLookupResults.size());
      failedLookupResults.forEach(
          lookupResult -> {
            // Verify that lookup response has correct fields
            var errorResponse = lookupResult.getErrorResponse();
            if (errorResponse.getDetailsCount() < 1) {
              String message = "Lookup Service returned error result without details.";
              logger.error(message);
              throw new JobProcessorException(message, LOOKUP_SERVICE_INVALID_ERROR);
            }
            String reason = errorResponse.getDetails(0).getReason();
            String lookupKey = lookupResult.getClientDataRecord().getLookupKey().getKey();
            // Find original PII value so correct DataRecord can be returned
            String piiValue =
                lookupDataRecordMap.isPresent()
                    ? getOriginalPiiValue(lookupKey, lookupDataRecordMap.get())
                    : lookupKey;
            erroredDataRecords.add(
                DataRecord.newBuilder()
                    .addKeyValues(
                        DataRecord.KeyValue.newBuilder().setKey(PII_VALUE).setStringValue(piiValue))
                    .setErrorCode(ErrorCodeConverter.convertToJobResultCode(reason))
                    .build());
          });
      return erroredDataRecords.build();
    }
  }

  private void validateLookupStatuses(Map<Status, List<LookupResult>> lookupResultsByStatus) {
    if (lookupResultsByStatus.containsKey(Status.STATUS_UNSPECIFIED)) {
      // Should not happen
      String message = "Lookup server returned STATUS_UNSPECIFIED.";
      logger.error(message);
      throw new JobProcessorException(message, LOOKUP_SERVICE_INVALID_ERROR);
    }
  }

  private DataRecordEncryptionKeys getDataRecordEncryptionKeys(
      EncryptionKeyInfo encryptionKeyInfo,
      DataRecord dataRecord,
      EncryptionKeyColumnIndices keyColumnIndices,
      Set<Integer> encryptedColumnIndices) {
    var encryptionKey = DataRecordEncryptionKeys.newBuilder();
    if (keyColumnIndices.hasCoordinatorKeyColumnIndices()) {
      encryptionKey.setCoordinatorKey(
          getCoordinatorKey(
              dataRecord,
              keyColumnIndices.getCoordinatorKeyColumnIndices().getCoordinatorKeyColumnIndex(),
              encryptedColumnIndices));
    } else {
      WrappedKeyColumnIndices wrappedKeyColumnIndices =
          keyColumnIndices.getWrappedKeyColumnIndices();
      if (encryptionKeyInfo.getWrappedKeyInfo().hasGcpWrappedKeyInfo()) {
        OptionalInt wipColumnIndex;
        // Do not try to search if request has valid WIP Provider
        if (!encryptionKeyInfo
            .getWrappedKeyInfo()
            .getGcpWrappedKeyInfo()
            .getWipProvider()
            .isBlank()) {
          wipColumnIndex = OptionalInt.empty();
        } else if (!wrappedKeyColumnIndices.hasGcpColumnIndices()) {
          String message = "No WIP in request and no WIP column index found.";
          logger.error(message);
          throw new JobProcessorException(message, ENCRYPTION_COLUMNS_PROCESSING_ERROR);
        } else {
          wipColumnIndex =
              OptionalInt.of(wrappedKeyColumnIndices.getGcpColumnIndices().getWipProviderIndex());
        }
        encryptionKey.setWrappedEncryptionKeys(
            getWrappedEncryptionKeys(
                dataRecord,
                wrappedKeyColumnIndices.getEncryptedDekColumnIndex(),
                wrappedKeyColumnIndices.getKekUriColumnIndex(),
                wipColumnIndex,
                encryptedColumnIndices));
      } else if (encryptionKeyInfo.getWrappedKeyInfo().hasAwsWrappedKeyInfo()) {
        // TODO(b/384749925):Add row-level roleARN support
        if (encryptionKeyInfo.getWrappedKeyInfo().getAwsWrappedKeyInfo().getRoleArn().isBlank()) {
          String message = "No role ARN in request found.";
          logger.error(message);
          throw new JobProcessorException(message, ENCRYPTION_COLUMNS_PROCESSING_ERROR);
        }
        encryptionKey.setWrappedEncryptionKeys(
            getWrappedEncryptionKeys(
                dataRecord,
                wrappedKeyColumnIndices.getEncryptedDekColumnIndex(),
                wrappedKeyColumnIndices.getKekUriColumnIndex(),
                OptionalInt.empty(),
                encryptedColumnIndices));
      }
    }
    return encryptionKey.build();
  }

  private CoordinatorKey getCoordinatorKey(
      DataRecord dataRecord, int coordKeyColumnIndex, Set<Integer> encryptedColumnIndices) {
    if (coordKeyColumnIndex < 0 || coordKeyColumnIndex >= dataRecord.getKeyValuesCount()) {
      throw new JobProcessorException(
          "Coordinator key column index out of bounds", INVALID_ENCRYPTION_COLUMN);
    }
    if (encryptedColumnIndices.contains(coordKeyColumnIndex)) {
      throw new JobProcessorException(
          "Coordinator key index cannot be part of columns-to-encrypt", INVALID_ENCRYPTION_COLUMN);
    }
    String coordKeyValue = dataRecord.getKeyValues(coordKeyColumnIndex).getStringValue();

    if (coordKeyValue.isBlank()) {
      throw new JobProcessorException(
          "Coordinator key value missing in input data row", MISSING_ENCRYPTION_COLUMN);
    }
    return CoordinatorKey.newBuilder().setKeyId(coordKeyValue).build();
  }

  private WrappedEncryptionKeys getWrappedEncryptionKeys(
      DataRecord dataRecord,
      int dekColumnIndex,
      int kekColumnIndex,
      OptionalInt optionalWipColumnIndex,
      Set<Integer> encryptedColumnIndices) {
    if (dekColumnIndex < 0
        || dekColumnIndex >= dataRecord.getKeyValuesCount()
        || kekColumnIndex < 0
        || kekColumnIndex >= dataRecord.getKeyValuesCount()) {
      String message = "DEK or KEK indices out of bounds";
      logger.info(message);
      throw new JobProcessorException(message, INVALID_ENCRYPTION_COLUMN);
    }
    if (encryptedColumnIndices.contains(dekColumnIndex)
        || encryptedColumnIndices.contains(kekColumnIndex)) {
      String message = "DEK or KEK indices cannot be part of columns-to-encrypt";
      logger.info(message);
      throw new JobProcessorException(message, INVALID_ENCRYPTION_COLUMN);
    }
    String dekValue = dataRecord.getKeyValues(dekColumnIndex).getStringValue();
    String kekValue = dataRecord.getKeyValues(kekColumnIndex).getStringValue();

    if (dekValue.isBlank() || kekValue.isBlank()) {
      String message = "DEK or KEK key values missing in input data row";
      logger.info(message);
      throw new JobProcessorException(message, MISSING_ENCRYPTION_COLUMN);
    }
    var wrappedKeysBuilder =
        WrappedEncryptionKeys.newBuilder().setEncryptedDek(dekValue).setKekUri(kekValue);
    optionalWipColumnIndex.ifPresent(
        wipColumnIndex -> {
          if (wipColumnIndex < 0 || wipColumnIndex >= dataRecord.getKeyValuesCount()) {
            String message = "WIP index out of bounds";
            logger.info(message);
            throw new JobProcessorException(message, INVALID_ENCRYPTION_COLUMN);
          }
          if (encryptedColumnIndices.contains(wipColumnIndex)) {
            String message = "WIP index cannot be part of columns-to-encrypt";
            logger.info(message);
            throw new JobProcessorException(message, INVALID_ENCRYPTION_COLUMN);
          }
          String wipValue = dataRecord.getKeyValues(wipColumnIndex).getStringValue();
          if (wipValue.isBlank()) {
            String message = "WIP key value missing in input data row";
            logger.info(message);
            throw new JobProcessorException(message, MISSING_ENCRYPTION_COLUMN);
          }
          wrappedKeysBuilder.setGcpWrappedKeys(
              GcpWrappedKeys.newBuilder().setWipProvider(wipValue));
        });
    return wrappedKeysBuilder.build();
  }

  private void addCompositeSingleColumn(
      ImmutableList.Builder<LookupDataRecord> records,
      DataRecord dataRecord,
      List<Integer> columns) {
    columns.forEach(
        idx -> {
          var keyValue = dataRecord.getKeyValues(idx);
          if (!keyValue.getStringValue().isBlank()) {
            records.add(getLookupRecord(keyValue.getStringValue()));
          }
        });
  }

  private void addCompositeSingleColumn(
      Map<String, LookupDataRecord> records,
      DataRecord dataRecord,
      List<Integer> columns,
      Set<Integer> encryptedColumnIndices) {
    columns.forEach(
        idx -> {
          var keyVal = dataRecord.getKeyValues(idx);
          if (encryptedColumnIndices.contains(idx) && !keyVal.getStringValue().isBlank()) {
            if (!dataRecord.containsEncryptedKeyValues(idx)) {
              String message =
                  "Column index in set of columns-to-decrypt, but dataRecord missing"
                      + " encryptedKeyValue";
              logger.error(message);
              throw new JobProcessorException(message, ENCRYPTION_COLUMNS_PROCESSING_ERROR);
            }
            // EncryptedKeyValues idx has been checked before this.
            // getEncryptedKeyValuesOrThrow throws IllegalArgumentException and will not retry.
            String encryptedKeyVal = dataRecord.getEncryptedKeyValuesOrThrow(idx);
            var decryptedKeyVal = dataRecord.getKeyValues(idx);
            records.put(
                encryptedKeyVal,
                getLookupRecord(encryptedKeyVal, decryptedKeyVal.getStringValue()));
          } else {
            var keyValue = dataRecord.getKeyValues(idx);
            if (!keyValue.getStringValue().isBlank()) {
              records.put(keyValue.getStringValue(), getLookupRecord(keyValue.getStringValue()));
            }
          }
        });
  }

  /*
   * Finds columns with aliases matching the given composite column (multi-column)
   * and adds values to the lookup request.
   */
  private void addCompositeMultiColumn(
      Map<String, LookupDataRecord> records,
      DataRecord dataRecord,
      ListMultimap<Integer, Integer> columnGroups,
      DataRecordEncryptionKeys encryptionKeys) {

    columnGroups.asMap().values().stream()
        .map(indexList -> getCompositeValue(dataRecord, indexList))
        .filter(Predicate.not(String::isBlank))
        .forEach(
            value -> {
              var hashedKey =
                  BaseEncoding.base64().encode(sha256().hashBytes(value.getBytes(UTF_8)).asBytes());
              try {
                var encryptedHashedKey = getCryptoClient().encrypt(encryptionKeys, hashedKey);
                records.put(encryptedHashedKey, getLookupRecord(encryptedHashedKey, hashedKey));
              } catch (CryptoClientException | IllegalArgumentException e) {
                logger.warn("Could not encrypt record. Skipping", e);
              }
            });
  }

  private CryptoClient getCryptoClient() {
    return cryptoClient.orElseThrow(
        () -> {
          String message = "CryptoClient missing in LookupServerDataSource";
          logger.error(message);
          return new JobProcessorException(message, CRYPTO_CLIENT_CONFIGURATION_ERROR);
        });
  }

  /*
   * Finds columns with aliases matching the given composite column (multi-column)
   * and adds values to the lookup request.
   */
  private void addCompositeMultiColumn(
      ImmutableList.Builder<LookupDataRecord> records,
      DataRecord dataRecord,
      ListMultimap<Integer, Integer> columns) {

    columns.asMap().values().stream()
        // Maps returned by ListMultimap.asMap() have List values with insert ordering preserved,
        // although the method signature doesn't explicitly say so.
        .map(indexList -> getCompositeValue(dataRecord, indexList))
        .filter(Predicate.not(String::isBlank))
        .forEach(
            value ->
                records.add(
                    getLookupRecord(
                        BaseEncoding.base64()
                            .encode(sha256().hashBytes(value.getBytes(UTF_8)).asBytes()))));
  }

  private String getCompositeValue(DataRecord dataRecord, Collection<Integer> indexList) {
    return indexList.stream()
        .map(index -> dataRecord.getKeyValues(index).getStringValue())
        .collect(Collectors.joining());
  }

  private LookupDataRecord getLookupRecord(String value) {
    return LookupDataRecord.newBuilder()
        .setLookupKey(LookupDataRecord.LookupKey.newBuilder().setKey(value))
        .build();
  }

  private LookupDataRecord getLookupRecord(String value, String decryptedString) {
    var lookupKeyBuilder =
        LookupDataRecord.LookupKey.newBuilder().setKey(value).setDecryptedKey(decryptedString);
    return LookupDataRecord.newBuilder().setLookupKey(lookupKeyBuilder).build();
  }

  private boolean allowPartialSuccess() {
    return successConfig.getSuccessMode() == SuccessMode.ALLOW_PARTIAL_SUCCESS;
  }

  private LookupDataSourceResult constructLookupDataSourceResultWithError(
      ImmutableList<DataRecord> lookupResults, ImmutableList<DataRecord> erroredDataRecordsList) {
    Schema responseSchema = getResponseSchema();
    Optional<DataChunk> errorChunk =
        erroredDataRecordsList.isEmpty()
            ? Optional.empty()
            : Optional.of(
                DataChunk.builder()
                    .setSchema(responseSchema)
                    .setRecords(erroredDataRecordsList)
                    .build());
    return LookupDataSourceResult.builder()
        .setLookupResults(
            DataChunk.builder().setSchema(responseSchema).setRecords(lookupResults).build())
        .setErroredLookupResults(errorChunk)
        .build();
  }
}
