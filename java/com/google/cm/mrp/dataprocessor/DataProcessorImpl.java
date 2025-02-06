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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.SchemaProto.Schema.DataFormat.SERIALIZED_PROTO;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.backend.DestinationInfoProto.DestinationInfo;
import com.google.cm.mrp.backend.DestinationInfoProto.DestinationInfo.GcsDestination;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.backend.SchemaProto.Schema.OutputColumn;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClientFactory;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.AeadProviderFactory;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.clients.cryptoclient.HybridCryptoClientFactory;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider.HybridEncryptionKeyServiceProviderException;
import com.google.cm.mrp.dataprocessor.common.Annotations.DataProcessorExecutorService;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputCondenserFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatter;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatterFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputSchemaValidator;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatter;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatterFactory;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparerFactory;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparerFactory;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.writers.DataWriter;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete class implementing {@link DataProcessor} interface. */
public final class DataProcessorImpl implements DataProcessor {

  private static final Logger logger = LoggerFactory.getLogger(DataProcessorImpl.class);

  private final ExecutorService executorService;
  private final DataMatcherFactory dataMatcherFactory;
  private final CsvDataWriterFactory csvDataWriterFactory;
  private final SerializedProtoDataWriterFactory serializedProtoDataWriterFactory;
  private final LookupDataSourceFactory lookupDataSourceFactory;
  private final StreamDataSourceFactory streamDataSourceFactory;
  private final DataDestinationFactory dataDestinationFactory;
  private final AeadProviderFactory aeadProviderFactory;
  private final AeadCryptoClientFactory aeadCryptoClientFactory;
  private final HybridCryptoClientFactory hybridCryptoClientFactory;
  private final HybridEncryptionKeyServiceProvider hybridEncryptionKeyServiceProvider;
  private final DataSourceFormatterFactory dataSourceFormatterFactory;
  private final DataSourcePreparerFactory dataSourcePreparerFactory;
  private final DataOutputCondenserFactory dataOutputCondenserFactory;
  private final DataOutputFormatterFactory dataOutputFormatterFactory;
  private final DataOutputPreparerFactory dataOutputPreparerFactory;

  /** Constructor for {@link DataProcessorImpl} */
  @Inject
  public DataProcessorImpl(
      @DataProcessorExecutorService ExecutorService executorService,
      DataMatcherFactory dataMatcherFactory,
      CsvDataWriterFactory csvDataWriterFactory,
      SerializedProtoDataWriterFactory serializedProtoDataWriterFactory,
      LookupDataSourceFactory lookupDataSourceFactory,
      StreamDataSourceFactory streamDataSourceFactory,
      DataDestinationFactory dataDestinationFactory,
      AeadProviderFactory aeadProviderFactory,
      AeadCryptoClientFactory aeadCryptoClientFactory,
      HybridCryptoClientFactory hybridCryptoClientFactory,
      HybridEncryptionKeyServiceProvider hybridEncryptionKeyServiceProvider,
      DataSourceFormatterFactory dataSourceFormatterFactory,
      DataSourcePreparerFactory dataSourcePreparerFactory,
      DataOutputCondenserFactory dataOutputCondenserFactory,
      DataOutputFormatterFactory dataOutputFormatterFactory,
      DataOutputPreparerFactory dataOutputPreparerFactory) {
    this.executorService = executorService;
    this.dataMatcherFactory = dataMatcherFactory;
    this.csvDataWriterFactory = csvDataWriterFactory;
    this.serializedProtoDataWriterFactory = serializedProtoDataWriterFactory;
    this.lookupDataSourceFactory = lookupDataSourceFactory;
    this.streamDataSourceFactory = streamDataSourceFactory;
    this.dataDestinationFactory = dataDestinationFactory;
    this.aeadProviderFactory = aeadProviderFactory;
    this.aeadCryptoClientFactory = aeadCryptoClientFactory;
    this.hybridCryptoClientFactory = hybridCryptoClientFactory;
    this.hybridEncryptionKeyServiceProvider = hybridEncryptionKeyServiceProvider;
    this.dataSourceFormatterFactory = dataSourceFormatterFactory;
    this.dataSourcePreparerFactory = dataSourcePreparerFactory;
    this.dataOutputCondenserFactory = dataOutputCondenserFactory;
    this.dataOutputFormatterFactory = dataOutputFormatterFactory;
    this.dataOutputPreparerFactory = dataOutputPreparerFactory;
  }

  /**
   * A method that takes a streamed data source, performs a match, and writes the output to an
   * output bucket.
   */
  @Override
  @SuppressWarnings("UnstableApiUsage") // ImmutableList::builderWithExpectedSize
  public MatchStatistics process(
      FeatureFlags featureFlags,
      DataOwnerList dataOwnerList,
      String outputBucket,
      String outputPrefix,
      String jobRequestId,
      MatchConfig matchConfig,
      Optional<EncryptionMetadata> encryptionMetadata,
      Optional<String> dataOwnerIdentity)
      throws JobProcessorException {

    Optional<CryptoClient> cryptoClient = encryptionMetadata.flatMap(this::getCryptoClient);
    cryptoClient.ifPresent((unused) -> logger.info("Job {}: Created CryptoClient", jobRequestId));

    final Optional<DataOwner> dataOwnerOptional =
        dataOwnerList.getDataOwnersList().stream().filter(DataOwner::hasLookupEndpoint).findAny();

    // If the second data source is omitted, then Google data is assumed
    String lookupEndpoint = dataOwnerOptional.map(DataOwner::getLookupEndpoint).orElse("");
    DataLocation dataLocation =
        dataOwnerList.getDataOwnersList().stream()
            .filter(dataOwner -> dataOwner.getDataLocation().getIsStreamed())
            .findAny()
            .orElseThrow()
            .getDataLocation();

    LookupDataSource lookupDataSource;
    final StreamDataSource streamDataSource;
    if (cryptoClient.isPresent()) {
      lookupDataSource =
          lookupDataSourceFactory.create(
              lookupEndpoint, matchConfig, cryptoClient.get(), featureFlags);
      logger.info("Job {}: Created LookupDataSource", jobRequestId);
      streamDataSource =
          streamDataSourceFactory.create(
              dataLocation,
              matchConfig,
              dataOwnerIdentity,
              featureFlags,
              encryptionMetadata.get(),
              cryptoClient.get());
      logger.info("Job {}: Created StreamDataSource", jobRequestId);
    } else {
      lookupDataSource = lookupDataSourceFactory.create(lookupEndpoint, matchConfig, featureFlags);
      logger.info("Job {}: Created LookupDataSource", jobRequestId);
      streamDataSource =
          streamDataSourceFactory.create(
              dataLocation, matchConfig, dataOwnerIdentity, featureFlags);
      logger.info("Job {}: Created StreamDataSource", jobRequestId);
    }
    ImmutableList.Builder<CompletableFuture<ImmutableList<MatchStatistics>>> completableFutures =
        ImmutableList.builderWithExpectedSize(streamDataSource.size());

    DataMatcher dataMatcher = dataMatcherFactory.create(matchConfig);
    logger.info("Job {}: Created DataMatcher", jobRequestId);
    DataDestination dataDestination =
        dataDestinationFactory.create(
            DestinationInfo.newBuilder()
                .setGcsDestination(
                    dataOwnerIdentity.isPresent()
                        ? GcsDestination.newBuilder()
                            .setOutputBucket(outputBucket)
                            .setOutputPrefix(outputPrefix + File.separator + jobRequestId)
                            .setDataOwnerIdentity(dataOwnerIdentity.get())
                        : GcsDestination.newBuilder()
                            .setOutputBucket(outputBucket)
                            .setOutputPrefix(outputPrefix))
                .build());
    logger.info("Job {}: Created DataDestination", jobRequestId);

    // Creates dataSourcePreparer instance when MIC feature is enabled.
    final Optional<DataSourcePreparer> dataSourcePreparer =
        getDataSourcePreparer(
            featureFlags, streamDataSource, matchConfig, cryptoClient, encryptionMetadata);
    dataSourcePreparer.ifPresent(
        unused -> logger.info("Job {}: Created DataSourcePreparer", jobRequestId));

    final Optional<DataOutputFormatter> dataOutputFormatter =
        getDataOutputFormatter(featureFlags, streamDataSource, matchConfig);
    dataOutputFormatter.ifPresent(
        unused -> logger.info("Job {}: Created DataOutputFormatter", jobRequestId));

    final Optional<DataOutputPreparer> dataOutputPreparer;
    if (!featureFlags.enableMIC()) {
      dataOutputPreparer = Optional.empty();
    } else {
      dataOutputPreparer = Optional.of(dataOutputPreparerFactory.create(dataOutputFormatter));
      logger.info("Job {}: Created DataOutputPreparer", jobRequestId);
    }

    for (int i = 0; i < streamDataSource.size(); ++i) {
      completableFutures.add(
          CompletableFuture.supplyAsync(
              () -> {
                final DataReader dataReader = streamDataSource.next();
                final Schema outputSchema =
                    dataOutputFormatter.isPresent()
                        ? dataOutputFormatter.get().getOutputSchema()
                        : getOutputSchema(matchConfig, dataReader.getSchema());
                final DataWriter dataWriter =
                    getDataWriter(
                        featureFlags,
                        outputSchema,
                        dataDestination,
                        dataReader.getName(),
                        matchConfig);
                logger.info("File {}: Created DataWriter", dataReader.getName());
                return DataProcessorTask.run(
                    dataReader,
                    lookupDataSource,
                    dataMatcher,
                    dataWriter,
                    encryptionMetadata,
                    dataSourcePreparer,
                    dataOutputPreparer);
              },
              executorService));
    }

    try {
      // Finish all tasks and combine match statistics
      ImmutableList<MatchStatistics> stats =
          completableFutures.build().stream()
              .map(CompletableFuture::join)
              .flatMap(Collection::stream)
              .collect(toImmutableList());
      return MatchStatistics.create(
          /* numFiles */ streamDataSource.size(),
          stats.stream().mapToLong(MatchStatistics::numDataRecords).sum(),
          stats.stream().mapToLong(MatchStatistics::numDataRecordsWithMatch).sum(),
          sumStatsMapEntries(stats, MatchStatistics::conditionMatches),
          sumStatsMapEntries(stats, MatchStatistics::validConditionChecks),
          sumStatsMapEntries(stats, MatchStatistics::datasource1Errors),
          sumStatsMapEntries(stats, MatchStatistics::datasource2ConditionMatches));

    } catch (CompletionException e) {
      // Unwrap JobProcessorException from the CompletionException to allow job-level retries
      if (e.getCause() != null && e.getCause() instanceof JobProcessorException) {
        throw (JobProcessorException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  private Optional<DataSourcePreparer> getDataSourcePreparer(
      FeatureFlags featureFlags,
      StreamDataSource streamDataSource,
      MatchConfig matchConfig,
      Optional<CryptoClient> cryptoClient,
      Optional<EncryptionMetadata> encryptionMetadata) {
    if (!featureFlags.enableMIC()) {
      return Optional.empty();
    }
    DataSourceFormatter formatter;
    // Evaluate if data is encrypted
    if (cryptoClient.isPresent()) {
      formatter =
          dataSourceFormatterFactory.create(
              matchConfig,
              streamDataSource.getSchema(),
              featureFlags,
              // always present if cryptoClient present
              encryptionMetadata.get(),
              cryptoClient.get());
    } else {
      formatter =
          dataSourceFormatterFactory.create(
              matchConfig, streamDataSource.getSchema(), featureFlags);
    }
    logger.info("Created DataSourceFormatter");
    return Optional.of(
        dataSourcePreparerFactory.create(
            formatter, matchConfig.getSuccessConfig().getSuccessMode()));
  }

  private DataWriter getDataWriter(
      FeatureFlags featureFlags,
      Schema schema,
      DataDestination dataDestination,
      String dataReaderName,
      MatchConfig matchConfig) {
    return schema.getDataFormat() == SERIALIZED_PROTO && featureFlags.enableSerializedProto()
        ? serializedProtoDataWriterFactory.create(
            dataDestination, dataReaderName, schema, matchConfig)
        : csvDataWriterFactory.create(
            dataDestination,
            dataReaderName,
            schema,
            matchConfig.getOutputColumnsList().stream()
                .sorted(comparing(outputColumn -> outputColumn.getColumn().getOrder()))
                .map(outputColumn -> outputColumn.getColumn().getColumnAlias())
                .collect(toImmutableList()));
  }

  private Optional<DataOutputFormatter> getDataOutputFormatter(
      FeatureFlags featureFlags, StreamDataSource streamDataSource, MatchConfig matchConfig) {
    if (!featureFlags.enableMIC()) {
      return Optional.empty();
    }
    DataOutputSchemaValidator.validateOutputSchema(streamDataSource.getSchema());
    if (streamDataSource.getSchema().getOutputColumnsList().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        dataOutputFormatterFactory.create(
            matchConfig,
            streamDataSource.getSchema(),
            streamDataSource.getSchema().getOutputColumnsList().stream()
                    .anyMatch(OutputColumn::hasCondensedColumn)
                ? Optional.of(dataOutputCondenserFactory.create(streamDataSource.getSchema()))
                : Optional.empty()));
  }

  private static Map<String, Long> sumStatsMapEntries(
      List<MatchStatistics> statsList, Function<MatchStatistics, Map<String, Long>> mapSelector) {
    return statsList.stream()
        .flatMap(stats -> mapSelector.apply(stats).entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
  }

  private Schema getOutputSchema(MatchConfig matchConfig, Schema readerSchema) {
    switch (matchConfig.getSuccessConfig().getSuccessMode()) {
      case SUCCESS_TYPE_UNDEFINED:
      case ONLY_COMPLETE_SUCCESS:
      default:
        return readerSchema;
      case ALLOW_PARTIAL_SUCCESS:
        if (!matchConfig.getSuccessConfig().hasPartialSuccessAttributes()) {
          String message =
              "SUCCESS_MODE is ALLOW_PARTIAL_SUCCESS, but partial_success_attributes empty";
          logger.error(message);
          throw new JobProcessorException(message, PARTIAL_SUCCESS_CONFIG_ERROR);
        }
        String name =
            matchConfig.getSuccessConfig().getPartialSuccessAttributes().getRecordStatusFieldName();
        return readerSchema.toBuilder()
            .addColumns(
                Column.newBuilder()
                    .setColumnName(name)
                    .setColumnAlias(name)
                    .setColumnType(ColumnType.STRING))
            .build();
    }
  }

  private Optional<CryptoClient> getCryptoClient(EncryptionMetadata encryptionMetadata) {
    return encryptionMetadata.hasEncryptionKeyInfo()
        ? getCryptoClient(encryptionMetadata.getEncryptionKeyInfo())
        : Optional.empty();
  }

  private Optional<CryptoClient> getCryptoClient(EncryptionKeyInfo encryptionKeyInfo) {
    if (encryptionKeyInfo.hasWrappedKeyInfo()) {
      return Optional.of(getAeadCryptoClient(encryptionKeyInfo));
    } else if (encryptionKeyInfo.hasCoordinatorKeyInfo()) {
      return Optional.of(getHybridCryptoClient(encryptionKeyInfo));
    } else {
      return Optional.empty();
    }
  }

  private CryptoClient getAeadCryptoClient(EncryptionKeyInfo encryptionKeyInfo) {
    try {
      // TODO(b/377983978): Select between GCP and AWS
      AeadProvider aeadProvider = aeadProviderFactory.createGcpAeadProvider();
      return aeadCryptoClientFactory.create(aeadProvider, encryptionKeyInfo);
    } catch (CryptoClientException e) {
      logger.error("Could not create AeadCryptoClient ", e);
      throw new JobProcessorException(e.getMessage(), e.getErrorCode());
    }
  }

  private CryptoClient getHybridCryptoClient(EncryptionKeyInfo encryptionKeyInfo) {
    try {
      return hybridCryptoClientFactory.create(
          hybridEncryptionKeyServiceProvider.getHybridEncryptionKeyService(
              encryptionKeyInfo.getCoordinatorKeyInfo()));
    } catch (HybridEncryptionKeyServiceProviderException | RuntimeException e) {
      throw new JobProcessorException(
          "Could not create Hybrid crypto client", e.getCause(), CRYPTO_CLIENT_CONFIGURATION_ERROR);
    }
  }
}
