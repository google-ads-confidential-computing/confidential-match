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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_DEK_KEY_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.MatchConfigProvider;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.backend.DestinationInfoProto.DestinationInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.Column;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.OutputColumn;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClientFactory;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.AeadProviderFactory;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.clients.cryptoclient.HybridCryptoClientFactory;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider.HybridEncryptionKeyServiceProviderException;
import com.google.cm.mrp.clients.cryptoclient.gcp.GcpAeadProvider;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputCondenser;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputCondenserFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatter;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatterFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatter;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatterFactory;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.DataMatchResult;
import com.google.cm.mrp.dataprocessor.models.LookupDataSourceResult;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparerFactory;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparerFactory;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.writers.DataWriter;
import com.google.cm.mrp.models.JobParameters;
import com.google.cm.mrp.models.JobParameters.OutputDataLocation;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class DataProcessorImplTest {

  private static final String INPUT_BUCKET = "input_bucket";
  private static final String INPUT_PREFIX = "input_prefix";
  private static final String OUTPUT_BUCKET = "output_bucket";
  private static final String OUTPUT_PREFIX = "output_prefix";
  private static final String JOB_REQUEST_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
  private static final Schema DEFAULT_SCHEMA = Schema.getDefaultInstance();
  private static final MatchConfig CM_CONFIG = MatchConfigProvider.getMatchConfig("customer_match");
  private static final List<String> CM_OUTPUT_COLUMNS = newOutputColumnsList(CM_CONFIG);
  private static final FeatureFlags DEFAULT_FEATURE_FLAGS = FeatureFlags.builder().build();
  private static final DataOwnerList DATA_OWNER_LIST =
      DataOwnerList.newBuilder()
          .addDataOwners(
              DataOwner.newBuilder()
                  .setDataLocation(
                      DataOwner.DataLocation.newBuilder()
                          .setInputDataBucketName(INPUT_BUCKET)
                          .setInputDataBlobPrefix(INPUT_PREFIX)
                          .setIsStreamed(true)))
          .build();
  private static final JobParameters DEFAULT_PARAMS =
      JobParameters.builder()
          .setJobId(JOB_REQUEST_ID)
          .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
          .setOutputDataLocation(OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
          .build();

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataMatcherFactory mockDataMatcherFactory;
  @Mock private DataWriterFactory mockDataWriterFactory;
  @Mock private LookupDataSourceFactory mockLookupDataSourceFactory;
  @Mock private StreamDataSourceFactory mockStreamDataSourceFactory;
  @Mock private DataDestinationFactory mockDataDestinationFactory;
  @Mock private StreamDataSource mockStreamDataSource;
  @Mock private LookupDataSource mockLookupDataSource;
  @Mock private DataReader mockDataReader;
  @Mock private DataMatcher mockDataMatcher;
  @Mock private DataWriter mockDataWriter;
  @Mock private DataChunk mockDataChunk;
  @Mock private LookupDataSourceResult mockLookupDataSourceResult;
  @Mock private DataDestination mockDataDestination;
  @Mock private AeadProviderFactory mockAeadProviderFactory;
  @Mock private AeadCryptoClientFactory mockAeadCryptoClientFactory;
  @Mock private HybridCryptoClientFactory mockHybridCryptoClientFactory;
  @Mock private HybridEncryptionKeyServiceProvider mockHybridEncryptionKeyServiceProvider;
  @Mock private DataSourceFormatterFactory mockDataSourceFormatterFactory;
  @Mock private DataSourcePreparerFactory mockDataSourcePreparerFactory;
  @Mock private DataSourcePreparer mockDataSourcePreparer;
  @Mock private DataSourceFormatter mockDataSourceFormatter;
  @Mock private DataOutputCondenserFactory mockDataOutputCondenserFactory;
  @Mock private DataOutputFormatterFactory mockDataOutputFormatterFactory;
  @Mock private DataOutputPreparerFactory mockDataOutputPreparerFactory;
  @Mock private DataOutputCondenser mockDataOutputCondenser;
  @Mock private DataOutputFormatter mockDataOutputFormatter;
  @Mock private DataOutputPreparer mockDataOutputPreparer;
  @Captor private ArgumentCaptor<DestinationInfo> destinationInfoCaptor;

  private DataProcessor dataProcessor;

  @Before
  public void setUp() {
    dataProcessor =
        new DataProcessorImpl(
            Executors.newFixedThreadPool(1),
            mockDataMatcherFactory,
            mockDataWriterFactory,
            mockLookupDataSourceFactory,
            mockStreamDataSourceFactory,
            mockDataDestinationFactory,
            mockAeadProviderFactory,
            mockAeadCryptoClientFactory,
            mockHybridCryptoClientFactory,
            mockHybridEncryptionKeyServiceProvider,
            mockDataSourceFormatterFactory,
            mockDataSourcePreparerFactory,
            mockDataOutputCondenserFactory,
            mockDataOutputFormatterFactory,
            mockDataOutputPreparerFactory);
  }

  @Test
  public void process_readsAndProcessesAllDataChunks() throws Exception {
    when(mockStreamDataSourceFactory.create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS)))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockStreamDataSource.size()).thenReturn(2);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(CM_CONFIG, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    var stats = MatchStatistics.emptyInstance();
    var result = DataMatchResult.create(mockDataChunk, stats);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS))
        .thenReturn(mockDataWriter);

    dataProcessor.process(
        FeatureFlags.builder().setEnableMIC(false).build(),
        CM_CONFIG,
        JobParameters.builder()
            .setJobId(JOB_REQUEST_ID)
            .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
            .setOutputDataLocation(
                OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
            .build());

    verify(mockDataReader, times(4)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader, times(2)).getSchema();
    verify(mockDataReader, times(2)).close();
    verify(mockDataReader, atLeastOnce()).getName();
    verify(mockDataMatcherFactory).create(CM_CONFIG, DEFAULT_PARAMS);
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriterFactory, times(2))
        .createCsvDataWriter(mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS);
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockDataWriter, times(2)).close();
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verify(mockStreamDataSourceFactory).create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(5)).size();
    verify(mockStreamDataSource, times(2)).next();
    verify(mockStreamDataSource, times(1)).getSchema();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_readsAndProcessesAllDataChunks_withMicFeatureEnabled() throws Exception {
    FeatureFlags micFeatureFlag = FeatureFlags.builder().setEnableMIC(true).build();
    when(mockDataSourceFormatterFactory.create(any(), any(), any()))
        .thenReturn(mockDataSourceFormatter);
    when(mockDataSourcePreparerFactory.create(any(), any())).thenReturn(mockDataSourcePreparer);
    when(mockDataSourcePreparer.prepare(any())).thenReturn(mockDataChunk);
    when(mockStreamDataSourceFactory.create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(micFeatureFlag)))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.size()).thenReturn(2);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockStreamDataSource.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(CM_CONFIG, micFeatureFlag, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(CM_CONFIG, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    var stats = MatchStatistics.emptyInstance();
    var result = DataMatchResult.create(mockDataChunk, stats);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataOutputCondenserFactory.create(any())).thenReturn(mockDataOutputCondenser);
    when(mockDataOutputFormatterFactory.create(any(), any(), any()))
        .thenReturn(mockDataOutputFormatter);
    when(mockDataOutputFormatter.getOutputSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockDataOutputPreparerFactory.create(any())).thenReturn(mockDataOutputPreparer);
    when(mockDataOutputPreparer.prepare(eq(result))).thenReturn(result);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS))
        .thenReturn(mockDataWriter);

    dataProcessor.process(
        micFeatureFlag,
        CM_CONFIG,
        JobParameters.builder()
            .setJobId(JOB_REQUEST_ID)
            .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
            .setOutputDataLocation(
                OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
            .build());

    verify(mockDataReader, times(4)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader, times(2)).getSchema();
    verify(mockDataReader, times(2)).close();
    verify(mockDataReader, atLeastOnce()).getName();
    verify(mockDataSourceFormatterFactory, times(1))
        .create(CM_CONFIG, DEFAULT_SCHEMA, micFeatureFlag);
    verify(mockDataSourcePreparerFactory, times(1))
        .create(mockDataSourceFormatter, SuccessMode.ONLY_COMPLETE_SUCCESS);
    verify(mockDataSourcePreparer, times(2)).prepare(eq(mockDataChunk));
    verify(mockDataMatcherFactory).create(CM_CONFIG, DEFAULT_PARAMS);
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataOutputCondenserFactory, times(0)).create(DEFAULT_SCHEMA);
    verify(mockDataOutputFormatterFactory, times(0))
        .create(CM_CONFIG, DEFAULT_SCHEMA, Optional.empty());
    verify(mockDataOutputFormatter, times(0)).getOutputSchema();
    verify(mockDataOutputPreparerFactory, times(1)).create(Optional.empty());
    verify(mockDataOutputPreparer, times(2)).prepare(eq(result));
    verify(mockDataWriterFactory, times(2))
        .createCsvDataWriter(mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS);
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockDataWriter, times(2)).close();
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(CM_CONFIG, micFeatureFlag, DEFAULT_PARAMS);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verify(mockStreamDataSourceFactory).create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(micFeatureFlag));
    verify(mockStreamDataSource, times(5)).size();
    verify(mockStreamDataSource, times(2)).next();
    verify(mockStreamDataSource, times(5)).getSchema();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockDataSourcePreparer,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataOutputCondenserFactory,
        mockDataOutputFormatterFactory,
        mockDataOutputFormatter,
        mockDataOutputPreparerFactory,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_adhMatchConfig_readsAndProcessesAllDataChunks() throws Exception {
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("adh");
    List<String> outputColumns = newOutputColumnsList(matchConfig);
    when(mockStreamDataSourceFactory.create(eq(matchConfig), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS)))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockStreamDataSource.size()).thenReturn(2);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(matchConfig, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(matchConfig, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    var stats = MatchStatistics.emptyInstance();
    var result = DataMatchResult.create(mockDataChunk, stats);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", DEFAULT_SCHEMA, outputColumns))
        .thenReturn(mockDataWriter);

    dataProcessor.process(
        FeatureFlags.builder().setEnableMIC(false).build(),
        matchConfig,
        JobParameters.builder()
            .setJobId(JOB_REQUEST_ID)
            .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
            .setOutputDataLocation(
                OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
            .build());

    verify(mockDataReader, times(4)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader, times(2)).getSchema();
    verify(mockDataReader, times(2)).close();
    verify(mockDataReader, atLeastOnce()).getName();
    verify(mockDataMatcherFactory).create(matchConfig, DEFAULT_PARAMS);
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriterFactory, times(2))
        .createCsvDataWriter(mockDataDestination, "test.csv", DEFAULT_SCHEMA, outputColumns);
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockDataWriter, times(2)).close();
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(matchConfig, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verify(mockStreamDataSourceFactory).create(eq(matchConfig), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(1)).getSchema();
    verify(mockStreamDataSource, times(5)).size();
    verify(mockStreamDataSource, times(2)).next();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_whenPartialSuccessMode_addsStatusToDataWriterSchema() throws Exception {
    MatchConfig matchConfig = MatchConfigProvider.getMatchConfig("copla");
    var newSchema =
        DEFAULT_SCHEMA.toBuilder()
            .addColumns(
                Schema.Column.newBuilder()
                    .setColumnName("row_status")
                    .setColumnAlias("row_status")
                    .setColumnType(ColumnType.STRING))
            .build();
    List<String> outputColumns = newOutputColumnsList(matchConfig);
    when(mockStreamDataSourceFactory.create(eq(matchConfig), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS)))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockStreamDataSource.size()).thenReturn(2);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(matchConfig, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(matchConfig, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    var stats = MatchStatistics.emptyInstance();
    var result = DataMatchResult.create(mockDataChunk, stats);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", newSchema, outputColumns))
        .thenReturn(mockDataWriter);

    dataProcessor.process(
        FeatureFlags.builder().setEnableMIC(false).build(),
        matchConfig,
        JobParameters.builder()
            .setJobId(JOB_REQUEST_ID)
            .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
            .setOutputDataLocation(
                OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
            .build());

    verify(mockDataReader, times(4)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader, times(2)).getSchema();
    verify(mockDataReader, times(2)).close();
    verify(mockDataReader, atLeastOnce()).getName();
    verify(mockDataMatcherFactory).create(matchConfig, DEFAULT_PARAMS);
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriterFactory, times(2))
        .createCsvDataWriter(mockDataDestination, "test.csv", newSchema, outputColumns);
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockDataWriter, times(2)).close();
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(matchConfig, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verify(mockStreamDataSourceFactory).create(eq(matchConfig), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(5)).size();
    verify(mockStreamDataSource, times(2)).next();
    verify(mockStreamDataSource, times(1)).getSchema();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_whenPartialSuccessModeButNoPartialAttributesThenUnwrapsThenThrows()
      throws Exception {
    MatchConfig matchConfig =
        CM_CONFIG.toBuilder()
            .setSuccessConfig(
                SuccessConfig.newBuilder().setSuccessMode(SuccessMode.ALLOW_PARTIAL_SUCCESS))
            .build();
    when(mockStreamDataSourceFactory.create(eq(matchConfig), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS)))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.size()).thenReturn(1);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockDataReader.hasNext()).thenReturn(true);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(matchConfig, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(matchConfig, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    var stats = MatchStatistics.emptyInstance();
    var result = DataMatchResult.create(mockDataChunk, stats);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS))
        .thenReturn(mockDataWriter);

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                dataProcessor.process(
                    FeatureFlags.builder().setEnableMIC(false).build(),
                    matchConfig,
                    JobParameters.builder()
                        .setJobId(JOB_REQUEST_ID)
                        .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                        .setOutputDataLocation(
                            OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                        .build()));

    verify(mockDataReader).getSchema();
    verify(mockDataMatcherFactory).create(matchConfig, DEFAULT_PARAMS);
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(matchConfig, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockStreamDataSourceFactory).create(eq(matchConfig), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(3)).size();
    verify(mockStreamDataSource).next();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_whenStreamDataSourceFailsThenThrows() {
    when(mockStreamDataSourceFactory.create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS) ))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.size()).thenReturn(1);
    when(mockStreamDataSource.next()).thenThrow(RuntimeException.class);
    when(mockLookupDataSourceFactory.create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockDataMatcherFactory.create(CM_CONFIG, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);

    assertThrows(
        RuntimeException.class,
        () ->
            dataProcessor.process(
                FeatureFlags.builder().setEnableMIC(false).build(),
                CM_CONFIG,
                JobParameters.builder()
                    .setJobId(JOB_REQUEST_ID)
                    .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                    .setOutputDataLocation(
                        OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                    .build()));

    verify(mockDataMatcherFactory).create(CM_CONFIG, DEFAULT_PARAMS);
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockStreamDataSourceFactory).create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(3)).size();
    verify(mockStreamDataSource).next();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_whenDataMatcherFailsThenThrows() throws Exception {
    when(mockStreamDataSourceFactory.create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS) ))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.size()).thenReturn(1);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockDataReader.hasNext()).thenReturn(true);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(CM_CONFIG, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk)))
        .thenThrow(RuntimeException.class);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS))
        .thenReturn(mockDataWriter);

    assertThrows(
        RuntimeException.class,
        () ->
            dataProcessor.process(
                FeatureFlags.builder().setEnableMIC(false).build(),
                CM_CONFIG,
                JobParameters.builder()
                    .setJobId(JOB_REQUEST_ID)
                    .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                    .setOutputDataLocation(
                        OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                    .build()));

    verify(mockDataReader).hasNext();
    verify(mockDataReader).next();
    verify(mockDataReader).close();
    verify(mockDataReader).getSchema();
    verify(mockDataReader, atLeastOnce()).getName();
    verify(mockDataMatcherFactory).create(CM_CONFIG, DEFAULT_PARAMS);
    verify(mockDataMatcher).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriterFactory)
        .createCsvDataWriter(mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS);
    verify(mockDataWriter).close();
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockLookupDataSource).lookup(mockDataChunk, Optional.empty());
    verify(mockStreamDataSourceFactory).create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(3)).size();
    verify(mockStreamDataSource).next();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_whenDataWriterFailsThenThrows() throws Exception {
    when(mockStreamDataSourceFactory.create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS) ))
        .thenReturn(mockStreamDataSource);
    when(mockStreamDataSource.size()).thenReturn(1);
    when(mockStreamDataSource.next()).thenReturn(mockDataReader);
    when(mockDataReader.hasNext()).thenReturn(true);
    when(mockDataReader.next()).thenReturn(mockDataChunk);
    when(mockDataReader.getName()).thenReturn("test.csv");
    when(mockDataReader.getSchema()).thenReturn(DEFAULT_SCHEMA);
    when(mockLookupDataSourceFactory.create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS))
        .thenReturn(mockLookupDataSource);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockDataMatcherFactory.create(CM_CONFIG, DEFAULT_PARAMS)).thenReturn(mockDataMatcher);
    var stats = MatchStatistics.emptyInstance();
    var result = DataMatchResult.create(mockDataChunk, stats);
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataDestinationFactory.create(any(DestinationInfo.class)))
        .thenReturn(mockDataDestination);
    when(mockDataWriterFactory.createCsvDataWriter(
            mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS))
        .thenReturn(mockDataWriter);
    doThrow(RuntimeException.class).when(mockDataWriter).write(mockDataChunk);

    assertThrows(
        RuntimeException.class,
        () ->
            dataProcessor.process(
                FeatureFlags.builder().setEnableMIC(false).build(),
                CM_CONFIG,
                JobParameters.builder()
                    .setJobId(JOB_REQUEST_ID)
                    .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                    .setOutputDataLocation(
                        OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                    .build()));

    verify(mockDataReader).hasNext();
    verify(mockDataReader).next();
    verify(mockDataReader).getSchema();
    verify(mockDataReader).close();
    verify(mockDataReader, atLeastOnce()).getName();
    verify(mockDataMatcherFactory).create(CM_CONFIG, DEFAULT_PARAMS);
    verify(mockDataMatcher).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriterFactory)
        .createCsvDataWriter(mockDataDestination, "test.csv", DEFAULT_SCHEMA, CM_OUTPUT_COLUMNS);
    verify(mockDataWriter).write(mockDataChunk);
    verify(mockDataWriter).close();
    verify(mockDataDestinationFactory).create(destinationInfoCaptor.capture());
    verify(mockLookupDataSourceFactory).create(CM_CONFIG, DEFAULT_FEATURE_FLAGS, DEFAULT_PARAMS);
    verify(mockLookupDataSource).lookup(mockDataChunk, Optional.empty());
    verify(mockStreamDataSourceFactory).create(eq(CM_CONFIG), eq(DEFAULT_PARAMS), eq(DEFAULT_FEATURE_FLAGS));
    verify(mockStreamDataSource, times(3)).size();
    verify(mockStreamDataSource).next();
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputBucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().getOutputPrefix())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(destinationInfoCaptor.getValue().getGcsDestination().hasDataOwnerIdentity())
        .isFalse();
    verifyNoMoreInteractions(
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockDataDestination);
  }

  @Test
  public void process_whenEncryptedDataButNoWrappedKeyEncryptionInfoThenThrows()
      throws JobProcessorException {
    var encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder().setKeyType(KeyType.XCHACHA20_POLY1305)))
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                dataProcessor.process(
                    FeatureFlags.builder().setEnableMIC(false).build(),
                    CM_CONFIG,
                    JobParameters.builder()
                        .setJobId(JOB_REQUEST_ID)
                        .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                        .setOutputDataLocation(
                            OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                        .setEncryptionMetadata(encryptionMetadata)
                        .build()));

    assertThat(ex.getMessage()).isEqualTo("Job parameters missing cloud wrappedKeyInfo.");
    assertThat(ex.getErrorCode()).isEqualTo(INVALID_PARAMETERS);
    verifyNoMoreInteractions(
        mockStreamDataSourceFactory,
        mockDataMatcherFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockAeadCryptoClientFactory,
        mockAeadProviderFactory,
        mockDataDestination);
  }

  @Test
  public void process_whenAeadProviderFailsWithCryptoExceptionThenThrows()
      throws JobProcessorException, CryptoClientException {
    String testWip = "testWip";
    var encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder()
                            .setKeyType(KeyType.XCHACHA20_POLY1305)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider(testWip))))
            .build();
    AeadProvider aeadProvider = new GcpAeadProvider();
    when(mockAeadProviderFactory.createGcpAeadProvider()).thenReturn(aeadProvider);
    when(mockAeadCryptoClientFactory.create(
            eq(aeadProvider), eq(encryptionMetadata.getEncryptionKeyInfo())))
        .thenThrow(new CryptoClientException(UNSUPPORTED_DEK_KEY_TYPE));

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                dataProcessor.process(
                    FeatureFlags.builder().setEnableMIC(false).build(),
                    CM_CONFIG,
                    JobParameters.builder()
                        .setJobId(JOB_REQUEST_ID)
                        .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                        .setOutputDataLocation(
                            OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                        .setEncryptionMetadata(encryptionMetadata)
                        .build()));

    assertThat(ex.getMessage()).isEqualTo(UNSUPPORTED_DEK_KEY_TYPE.name());
    assertThat(ex.getErrorCode()).isEqualTo(UNSUPPORTED_DEK_KEY_TYPE);
    verify(mockAeadCryptoClientFactory)
        .create(eq(aeadProvider), eq(encryptionMetadata.getEncryptionKeyInfo()));
    verify(mockAeadProviderFactory).createGcpAeadProvider();
    verifyNoMoreInteractions(
        mockStreamDataSourceFactory,
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockAeadCryptoClientFactory,
        mockAeadProviderFactory,
        mockDataDestination);
  }

  @Test
  public void process_whenHybridEncryptionKeyServiceProviderFailsThenThrows() throws Exception {
    when(mockHybridEncryptionKeyServiceProvider.getHybridEncryptionKeyService(any()))
        .thenThrow(HybridEncryptionKeyServiceProviderException.class);

    CoordinatorKeyInfo coordinatorKeyInfo =
        CoordinatorKeyInfo.newBuilder()
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint("endpoint1")
                    .setKmsIdentity("testSa1")
                    .setKmsWipProvider("testWip1"))
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint("endpoint2")
                    .setKmsIdentity("testSa2")
                    .setKmsWipProvider("testWip2"))
            .build();
    var encryptionMetadata =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder().setCoordinatorKeyInfo(coordinatorKeyInfo))
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                dataProcessor.process(
                    FeatureFlags.builder().setEnableMIC(false).build(),
                    CM_CONFIG,
                    JobParameters.builder()
                        .setJobId(JOB_REQUEST_ID)
                        .setDataLocation(DATA_OWNER_LIST.getDataOwners(0).getDataLocation())
                        .setOutputDataLocation(
                            OutputDataLocation.forNameAndPrefix(OUTPUT_BUCKET, OUTPUT_PREFIX))
                        .setEncryptionMetadata(encryptionMetadata)
                        .build()));

    assertThat(ex.getMessage()).isEqualTo("Could not create Hybrid crypto client");
    assertThat(ex.getErrorCode()).isEqualTo(CRYPTO_CLIENT_CONFIGURATION_ERROR);
    verify(mockHybridEncryptionKeyServiceProvider)
        .getHybridEncryptionKeyService(coordinatorKeyInfo);
    verifyNoMoreInteractions(
        mockStreamDataSourceFactory,
        mockDataMatcherFactory,
        mockDataWriterFactory,
        mockLookupDataSourceFactory,
        mockStreamDataSourceFactory,
        mockDataDestinationFactory,
        mockStreamDataSource,
        mockLookupDataSource,
        mockDataReader,
        mockDataMatcher,
        mockDataWriter,
        mockDataChunk,
        mockHybridEncryptionKeyServiceProvider,
        mockAeadCryptoClientFactory,
        mockDataDestination);
  }

  private static List<String> newOutputColumnsList(MatchConfig matchConfig) {
    return matchConfig.getOutputColumnsList().stream()
        .map(OutputColumn::getColumn)
        .sorted(Comparator.comparing(Column::getOrder))
        .map(Column::getColumnAlias)
        .collect(Collectors.toList());
  }
}
