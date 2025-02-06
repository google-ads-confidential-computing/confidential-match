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

package com.google.cm.mrp.clients.lookupserviceclient;

import static com.google.cm.lookupserver.api.LookupProto.LookupRequest.HashInfo.HashType.HASH_TYPE_SHA_256;
import static com.google.cm.mrp.dataprocessor.converters.EncryptionMetadataConverter.convertToDataEncryptionKeys;
import static com.google.cm.mrp.dataprocessor.converters.ErrorCodeConverter.isValidRowLevelErrorReason;
import static com.google.cm.util.JumpConsistentHasher.hash;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cm.lookupserver.api.LookupProto.DataRecord;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.HashInfo;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.SerializableDataRecords;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.ShardingScheme;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.cm.lookupserver.api.LookupProto.LookupResult.Status;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceClientClusterGroupId;
import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceClientExecutor;
import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceClientMaxRecordsPerRequest;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClient.LookupServiceShardClientException;
import com.google.cm.mrp.clients.lookupserviceclient.converters.DataRecordConverter;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientRequest;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientResponse;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient.OrchestratorClientException;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse.Shard;
import com.google.cm.shared.api.errors.ErrorResponseProto.Details;
import com.google.cm.shared.api.errors.ErrorResponseProto.ErrorResponse;
import com.google.cm.shared.api.model.Code;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for the lookup service. */
public final class LookupServiceClientImpl implements LookupServiceClient {

  private static final Logger logger = LoggerFactory.getLogger(LookupServiceClientImpl.class);

  private final OrchestratorClient orchestratorClient;
  private final LookupServiceShardClient lookupServiceShardClient;
  private final Executor threadPool;
  private final int maxRecordsPerRequest;
  private final String clusterGroupId;
  private ShardingScheme shardingScheme;
  private ImmutableList<Shard> shards;

  /** Constructor for {@link LookupServiceClientImpl}. */
  @Inject
  public LookupServiceClientImpl(
      OrchestratorClient orchestratorClient,
      LookupServiceShardClient lookupServiceShardClient,
      @LookupServiceClientExecutor Executor threadPool,
      @LookupServiceClientMaxRecordsPerRequest int maxRecordsPerRequest,
      @LookupServiceClientClusterGroupId String clusterGroupId) {
    this.orchestratorClient = orchestratorClient;
    this.lookupServiceShardClient = lookupServiceShardClient;
    this.threadPool = threadPool;
    this.maxRecordsPerRequest = maxRecordsPerRequest;
    this.clusterGroupId = clusterGroupId;
  }

  /** {@inheritDoc} */
  @Override
  public LookupServiceClientResponse lookupRecords(LookupServiceClientRequest request)
      throws LookupServiceClientException {
    logger.info("LookupServiceClient starting to send requests to lookup service.");
    // Don't try to refresh scheme on first try
    return lookupRecords(request, /*forceShardingSchemeRefresh*/ false);
  }

  private LookupServiceClientResponse lookupRecords(
      LookupServiceClientRequest request, boolean forceShardingSchemeRefresh)
      throws LookupServiceClientException {
    try {

      return getLookupServiceClientResponse(request, forceShardingSchemeRefresh);

    } catch (OrchestratorClientException ex) {

      /*
       * getShardingScheme() may fail on an attempt to get the scheme from the orchestrator,
       * if this is the first call to getShardingScheme().
       */
      String message =
          String.format(
              "LookupServiceClient threw an exception while attempting to get the %s from the"
                  + " orchestrator.",
              forceShardingSchemeRefresh ? "updated scheme" : "scheme");
      logger.error(message);
      throw new LookupServiceClientException(Code.INTERNAL.name(), message, ex);

    } catch (LookupServiceClientException ex) {
      /*
       * LookupServiceShardClient.lookupRecords() may fail, if an outdated shardingScheme was
       * rejected by the LookupServer.
       */
      if (isInvalidSchemeException(ex) && !forceShardingSchemeRefresh) {
        // Try to get the records again, this time by forcing a scheme refresh
        return lookupRecords(request, /* forceShardingSchemeRefresh= */ true);
      }
      String message =
          String.format(
              "LookupServiceClient threw an exception while %s lookup results from one of the"
                  + " shards.",
              forceShardingSchemeRefresh ? "retrying" : "retrieving");
      logger.error(message);
      throw new LookupServiceClientException(Code.INTERNAL.name(), message, ex);
    }
  }

  private LookupServiceClientResponse getLookupServiceClientResponse(
      LookupServiceClientRequest request, boolean forceShardingSchemeRefresh)
      throws LookupServiceClientException, OrchestratorClientException {
    if (shardingScheme == null || shards == null || forceShardingSchemeRefresh) {
      refreshShardingScheme();
    }
    LookupServiceClientResponse result = lookupRecordsFromShards(request);
    logger.info("LookupServiceClient finished receiving responses from the lookup service.");
    return result;
  }

  private LookupServiceClientResponse lookupRecordsFromShards(
      LookupServiceClientRequest clientRequest) throws LookupServiceClientException {
    List<LookupDataRecord> allRecords = clientRequest.records();

    var lookupRequestBuilder =
        LookupRequest.newBuilder()
            .setShardingScheme(shardingScheme)
            .setKeyFormat(clientRequest.keyFormat())
            .setHashInfo(HashInfo.newBuilder().setHashType(HASH_TYPE_SHA_256));
    clientRequest.encryptionKeyInfo().ifPresent(lookupRequestBuilder::setEncryptionKeyInfo);

    try {
      ImmutableList<CompletableFuture<LookupResponse>> futureResults =
          Multimaps.index(
                  allRecords,
                  record -> {
                    // If DecryptedKey has been set, use this to hash
                    var lookupKey = record.getLookupKey();
                    var keyToHash =
                        lookupKey.hasDecryptedKey()
                            ? lookupKey.getDecryptedKey()
                            : lookupKey.getKey();
                    return hash(keyToHash, shards.size());
                  })
              .asMap() // Data records grouped (as lists) by the index of the target shard
              .entrySet()
              .stream()
              .flatMap(
                  entry ->
                      clientRequest.cryptoClient().isPresent()
                          ? toBatchEncryptedRequestFuture(
                              shards.get(entry.getKey()).getServerAddressUri(),
                              entry.getValue(),
                              lookupRequestBuilder.build(),
                              clientRequest.cryptoClient().get())
                          : toRequestFuture(entry, lookupRequestBuilder.build()))
              .collect(toImmutableList());

      List<LookupResult> results =
          futureResults.stream()
              .flatMap(future -> future.join().getLookupResultsList().stream())
              .collect(toImmutableList());
      return LookupServiceClientResponse.builder().setResults(results).build();
    } catch (RuntimeException ex) {
      String message = "LookupServiceClient threw an exception.";
      logger.error(message);
      throw new LookupServiceClientException(Code.INTERNAL.name(), message, ex);
    }
  }

  private Stream<CompletableFuture<LookupResponse>> toRequestFuture(
      Entry<Integer, Collection<LookupDataRecord>> shardRecords, LookupRequest lookupRequestBase) {
    String shardUri = shards.get(shardRecords.getKey()).getServerAddressUri();
    Collection<DataRecord> recordsPerShard =
        shardRecords.getValue().stream()
            .map(DataRecordConverter::convertToDataRecord)
            .collect(toImmutableList());
    return Streams.stream(Iterables.partition(recordsPerShard, maxRecordsPerRequest))
        .map(
            recordsPerRequest ->
                lookupRequestBase.toBuilder().addAllDataRecords(recordsPerRequest).build())
        .map(request -> getAsyncShardResponse(request, shardUri));
  }

  private Stream<CompletableFuture<LookupResponse>> toBatchEncryptedRequestFuture(
      String shardUri,
      Collection<LookupDataRecord> lookupDataRecords,
      LookupRequest lookupRequestBase,
      CryptoClient cryptoClient) {
    Collection<DataRecord> recordsPerShard =
        lookupDataRecords.stream()
            .map(DataRecordConverter::convertToDataRecord)
            .collect(toImmutableList());

    return Streams.stream(Iterables.partition(recordsPerShard, maxRecordsPerRequest))
        .map(
            recordsPerRequest ->
                buildAndSubmitAsyncBatchRequest(
                    recordsPerRequest, lookupRequestBase, cryptoClient, shardUri));
  }

  private CompletableFuture<LookupResponse> buildAndSubmitAsyncBatchRequest(
      List<DataRecord> records,
      LookupRequest lookupRequestBase,
      CryptoClient cryptoClient,
      String shardEndpoint) {
    EncryptionKeyInfo encryptionKeyInfo = lookupRequestBase.getEncryptionKeyInfo();

    SerializableDataRecords serializableDataRecords =
        SerializableDataRecords.newBuilder().addAllDataRecords(records).build();

    String encryptedDataRecords;
    // Batch encrypt the serialized data records.
    try {
      encryptedDataRecords =
          cryptoClient.encrypt(
              convertToDataEncryptionKeys(encryptionKeyInfo),
              serializableDataRecords.toByteArray());
    } catch (JobProcessorException | CryptoClientException e) {
      String message = "LookupServiceClient failed to batch encrypt the data records.";
      logger.error(message);
      throw new UncheckedLookupServiceClientException(message, e);
    }

    LookupRequest request =
        lookupRequestBase.toBuilder().setEncryptedDataRecords(encryptedDataRecords).build();
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return lookupServiceShardClient.lookupRecords(shardEndpoint, request);
          } catch (LookupServiceShardClientException ex) {
            if (isValidRowLevelErrorReason(ex.getErrorReason())) {
              List<LookupResult> results =
                  records.stream()
                      .map(record -> generateLookupResultWithError(record, ex.getErrorReason()))
                      .collect(Collectors.toList());
              return LookupResponse.newBuilder().addAllLookupResults(results).build();
            }
            throw new CompletionException(ex);
          }
        },
        threadPool);
  }

  private LookupResult generateLookupResultWithError(DataRecord record, String errorReason) {
    Details details = Details.newBuilder().setReason(errorReason).build();
    ErrorResponse errorResponse = ErrorResponse.newBuilder().addDetails(details).build();
    return LookupResult.newBuilder()
        .setClientDataRecord(record)
        .setErrorResponse(errorResponse)
        .setStatusValue(Status.STATUS_FAILED_VALUE)
        .build();
  }

  // Executes async, may throw a CompletionException on join()/get()/etc.
  private CompletableFuture<LookupResponse> getAsyncShardResponse(
      LookupRequest request, String shardEndpoint) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return lookupServiceShardClient.lookupRecords(shardEndpoint, request);
          } catch (LookupServiceShardClientException ex) {
            throw new CompletionException(ex);
          }
        },
        threadPool);
  }

  /*
   * Get the sharding scheme. If this is the first call to this method or if forceRefresh == true,
   * then fetch the current sharding scheme from the orchestrator first.
   */
  private void refreshShardingScheme() throws OrchestratorClientException {
    GetCurrentShardingSchemeResponse response =
        orchestratorClient.getCurrentShardingScheme(clusterGroupId);

    shardingScheme =
        ShardingScheme.newBuilder()
            .setNumShards(response.getShardsCount())
            .setType(response.getType())
            .build();

    shards =
        response.getShardsList().stream()
            .sorted(Comparator.comparingLong(Shard::getShardNumber))
            .collect(ImmutableList.toImmutableList());
  }

  /*
   * Return true, if LookupServiceShardClient.lookupRecords() throws CompletionException,
   * and it has an ErrorReason.INVALID_SCHEME, which is a retriable error.
   */
  private boolean isInvalidSchemeException(LookupServiceClientException e) {
    return e.getCause() != null
        && e.getCause() instanceof CompletionException
        && e.getCause().getCause() != null
        && e.getCause().getCause() instanceof LookupServiceShardClientException
        && ErrorReason.INVALID_SCHEME
            .name()
            .equalsIgnoreCase(
                ((LookupServiceShardClientException) e.getCause().getCause()).getErrorReason());
  }
}
