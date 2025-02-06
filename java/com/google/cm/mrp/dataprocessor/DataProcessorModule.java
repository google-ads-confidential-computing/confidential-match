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

import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClient;
import com.google.cm.mrp.clients.cryptoclient.AeadCryptoClientFactory;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.HybridCryptoClient;
import com.google.cm.mrp.clients.cryptoclient.HybridCryptoClientFactory;
import com.google.cm.mrp.dataprocessor.common.Annotations.DataProcessorExecutorService;
import com.google.cm.mrp.dataprocessor.common.Annotations.DataProcessorThreadPoolSize;
import com.google.cm.mrp.dataprocessor.common.Annotations.InputDataChunkSize;
import com.google.cm.mrp.dataprocessor.common.Annotations.MaxRecordsPerOutputFile;
import com.google.cm.mrp.dataprocessor.destinations.BlobStoreDataDestination;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputCondenser;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputCondenserFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatter;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatterFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatterImpl;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatter;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatterFactory;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatterImpl;
import com.google.cm.mrp.dataprocessor.formatters.ProtoDataOutputCondenser;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparerFactory;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparerImpl;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparerFactory;
import com.google.cm.mrp.dataprocessor.preparers.NestedDataSourcePreparer;
import com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParser;
import com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserFactory;
import com.google.cm.mrp.dataprocessor.readers.ConfidentialMatchDataRecordParserImpl;
import com.google.cm.mrp.dataprocessor.readers.CsvDataReader;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.readers.SerializedProtoDataReader;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformer;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerFactory;
import com.google.cm.mrp.dataprocessor.transformations.DataRecordTransformerImpl;
import com.google.cm.mrp.dataprocessor.writers.CsvDataWriter;
import com.google.cm.mrp.dataprocessor.writers.DataWriter;
import com.google.cm.mrp.dataprocessor.writers.SerializedProtoDataWriter;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/** Guice module for the implementation of {@link DataProcessor}. */
public final class DataProcessorModule extends AbstractModule {
  private final int threadPoolSize;
  private final int inputDataChunkSize;
  private final int maxRecordsPerOutputFile;

  /** Constructor for DataProcessorModule. */
  public DataProcessorModule(
      int threadPoolSize, int inputDataChunkSize, int maxRecordsPerOutputFile) {
    this.threadPoolSize = threadPoolSize;
    this.inputDataChunkSize = inputDataChunkSize;
    this.maxRecordsPerOutputFile = maxRecordsPerOutputFile;
  }

  /**
   * Configures injected dependencies for this module. Includes a binding for {@link DataProcessor}.
   */
  @Override
  protected void configure() {
    bind(DataProcessor.class).to(DataProcessorImpl.class);
    install(
        new FactoryModuleBuilder()
            .implement(DataMatcher.class, DataMatcherImpl.class)
            .build(DataMatcherFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(
                ConfidentialMatchDataRecordParser.class,
                ConfidentialMatchDataRecordParserImpl.class)
            .build(ConfidentialMatchDataRecordParserFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataReader.class, Names.named("csvWithoutEncryption"), CsvDataReader.class)
            .implement(DataReader.class, Names.named("csvWithEncryption"), CsvDataReader.class)
            .implement(
                DataReader.class,
                Names.named("serializedProtoWithoutEncryption"),
                SerializedProtoDataReader.class)
            .implement(
                DataReader.class,
                Names.named("serializedProtoWithEncryption"),
                SerializedProtoDataReader.class)
            .build(DataReaderFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataWriter.class, CsvDataWriter.class)
            .build(CsvDataWriterFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataWriter.class, SerializedProtoDataWriter.class)
            .build(SerializedProtoDataWriterFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(LookupDataSource.class, LookupServerDataSource.class)
            .build(LookupDataSourceFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(StreamDataSource.class, BlobStoreStreamDataSource.class)
            .build(StreamDataSourceFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataDestination.class, BlobStoreDataDestination.class)
            .build(DataDestinationFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(CryptoClient.class, AeadCryptoClient.class)
            .build(AeadCryptoClientFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(CryptoClient.class, HybridCryptoClient.class)
            .build(HybridCryptoClientFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataRecordTransformer.class, DataRecordTransformerImpl.class)
            .build(DataRecordTransformerFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataSourceFormatter.class, DataSourceFormatterImpl.class)
            .build(DataSourceFormatterFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataSourcePreparer.class, NestedDataSourcePreparer.class)
            .build(DataSourcePreparerFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataOutputCondenser.class, ProtoDataOutputCondenser.class)
            .build(DataOutputCondenserFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataOutputFormatter.class, DataOutputFormatterImpl.class)
            .build(DataOutputFormatterFactory.class));
    install(
        new FactoryModuleBuilder()
            .implement(DataOutputPreparer.class, DataOutputPreparerImpl.class)
            .build(DataOutputPreparerFactory.class));
  }

  /** Provider method for threadPoolSize. */
  @Provides
  @Singleton
  @DataProcessorThreadPoolSize
  Integer provideThreadPoolSize() {
    return threadPoolSize;
  }

  /** Provider method for ExecutorService. */
  @Provides
  @Singleton
  @DataProcessorExecutorService
  @SuppressWarnings("UnstableApiUsage")
  ExecutorService provideExecutorService() {
    return getExitingExecutorService((ThreadPoolExecutor) newFixedThreadPool(threadPoolSize));
  }

  /** Provider method for inputDataChunkSize. */
  @Provides
  @Singleton
  @InputDataChunkSize
  Integer provideInputDataChunkSize() {
    return inputDataChunkSize;
  }

  /** Provider method for maxRecordsPerOutputFile. */
  @Provides
  @Singleton
  @MaxRecordsPerOutputFile
  Integer maxRecordsPerOutputFile() {
    return maxRecordsPerOutputFile;
  }
}
