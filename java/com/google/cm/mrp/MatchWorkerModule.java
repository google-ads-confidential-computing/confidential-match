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

package com.google.cm.mrp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cm.mrp.Annotations.JobProcessorMaxRetries;
import com.google.cm.mrp.Annotations.JobQueueRetryDelaySec;
import com.google.cm.mrp.clients.attestation.AttestationTokenModule;
import com.google.cm.mrp.clients.blobstoreclient.BlobStoreClientModule;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.AeadProviderFactory;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider;
import com.google.cm.mrp.clients.cryptoclient.aws.AwsAeadProvider;
import com.google.cm.mrp.clients.cryptoclient.gcp.GcpAeadProvider;
import com.google.cm.mrp.clients.cryptoclient.gcp.MultiPartyHybridEncryptionKeyServiceProvider;
import com.google.cm.mrp.clients.lookupserviceclient.gcp.LookupServiceClientGcpModule;
import com.google.cm.mrp.dataprocessor.DataProcessorModule;
import com.google.cm.mrp.selectors.LookupProtoFormatSelector;
import com.google.cm.mrp.selectors.LookupProtoFormatSelector.LookupProtoFormatHandler;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.google.scp.operator.cpio.blobstorageclient.gcp.Annotations.GcsEndpointUrl;
import com.google.scp.operator.cpio.jobclient.gcp.GcpJobHandlerConfig;
import com.google.scp.operator.cpio.jobclient.local.LocalFileJobHandlerModule.LocalFileJobHandlerPath;
import com.google.scp.operator.cpio.jobclient.local.LocalFileJobHandlerModule.LocalFileJobHandlerResultPath;
import com.google.scp.operator.cpio.jobclient.local.LocalFileJobHandlerModule.LocalFileJobParameters;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.ParameterClient.ParameterClientException;
import com.google.scp.shared.mapper.TimeObjectMapper;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures the worker service and its dependencies. Requires additional bindings for the
 * parameter client and client config.
 */
public final class MatchWorkerModule extends AbstractModule {

  private static final Logger logger = LoggerFactory.getLogger(MatchWorkerModule.class);

  private static final Optional<String> GCS_ENDPOINT_OVERRIDE = Optional.empty();
  private static final String GCP_PROJECT_ID_OVERRIDE = "";
  private static final Optional<String> SPANNER_ENDPOINT_OVERRIDE = Optional.empty();
  private static final String SPANNER_INSTANCE_ID_OVERRIDE = "";
  private static final String SPANNER_DATABASE_NAME_OVERRIDE = "";
  private static final Optional<String> PUBSUB_ENDPOINT_OVERRIDE = Optional.empty();
  private static final String PUBSUB_TOPIC_ID_OVERRIDE = "";
  private static final String PUBSUB_SUBSCRIPTION_ID_OVERRIDE = "";
  // Message size is required, but the job client will ignore it and always set it to 1000
  private static final int GCP_PUBSUB_MAX_MESSAGE_SIZE_BYTES = 1000;
  private static final int GCP_PUBSUB_MESSAGE_LEASE_SECONDS = 600;

  private final MatchWorkerArgs args;
  private final ParameterClient parameterClient;

  /** Constructs a new instance. */
  public MatchWorkerModule(MatchWorkerArgs args, ParameterClient parameterClient) {
    this.args = args;
    this.parameterClient = parameterClient;
  }

  /** Configures the bindings for this module. */
  @Override
  protected void configure() {
    try {
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      logger.error("Could not register AeadConfig.");
      throw new RuntimeException(e);
    }
    // Parameter Client and Client Config modules are provided by the MatchWorkerParameterModule
    install(new WorkerModule(parameterClient));
    install(args.getLifecycleClientSelector().getLifecycleClientModule());
    install(args.getMetricClientSelector().getMetricClientModule());
    install(args.getNotificationClientSelector().getNotificationModule());
    install(
        new DataProcessorModule(
            getMrpThreadPoolSize(), getInputDataChunkSize(), getMaxRecordsPerOutputFile()));
    install(
        new FactoryModuleBuilder()
            .implement(AeadProvider.class, Names.named("aws"), AwsAeadProvider.class)
            .implement(AeadProvider.class, Names.named("gcp"), GcpAeadProvider.class)
            .build(AeadProviderFactory.class));
    bind(HybridEncryptionKeyServiceProvider.class)
        .to(MultiPartyHybridEncryptionKeyServiceProvider.class);
    install(new AttestationTokenModule(getAwsDefaultKmsAudience(), getAwsSignaturesList()));

    // TODO(b/309462840): LookupServiceClientGcpModule should be in a selector
    install(
        new LookupServiceClientGcpModule(
            getOrchestratorEndpoint(),
            getLookupServiceAudience(),
            getLookupClientThreadPoolSize(),
            getLookupClientMaxRecordsPerRequest(),
            getLookupClientMaxRequestRetries(),
            getLookupClientClusterGroupId()));
    bind(FeatureFlagProvider.class).to(FeatureFlagProviderImpl.class);
    bind(StartupConfigProvider.class).to(StartupConfigProviderImpl.class);
    bind(Integer.class)
        .annotatedWith(JobProcessorMaxRetries.class)
        .toInstance(getJobProcessorMaxRetries());
    bind(Integer.class)
        .annotatedWith(JobQueueRetryDelaySec.class)
        .toInstance(getJobQueueRetryDelaySec());
    bind(JobProcessor.class).to(MatchJobProcessor.class);
    bind(ObjectMapper.class).to(TimeObjectMapper.class);
    bind(LookupProtoFormatHandler.class).toInstance(getLookupProtoFormat().getFormatHandler());
    bind(DataSourceSizeProvider.class).to(BlobStoreDataSourceSizeProvider.class);

    switch (args.getBlobStorageClientSelector()) {
      case LOCAL_FS:
        bind(FileSystem.class).toInstance(FileSystems.getDefault());
        break;
      case GCS:
        bind(new TypeLiteral<Optional<String>>() {})
            .annotatedWith(GcsEndpointUrl.class)
            .toInstance(GCS_ENDPOINT_OVERRIDE);
        break;
    }
    install(args.getBlobStorageClientSelector().getBlobStorageClientModule());
    install(new BlobStoreClientModule());

    switch (args.getJobClientSelector()) {
      case LOCAL_FILE:
        bind(Path.class)
            .annotatedWith(LocalFileJobHandlerPath.class)
            .toInstance(args.getLocalJobClientInputFilePath());
        bind(new TypeLiteral<Optional<Path>>() {})
            .annotatedWith(LocalFileJobHandlerResultPath.class)
            .toInstance(args.getLocalJobClientOutputFilePath());
        bind(new TypeLiteral<Supplier<ImmutableMap<String, String>>>() {})
            .annotatedWith(LocalFileJobParameters.class)
            .toInstance(Suppliers.ofInstance(ImmutableMap.of()));
        break;
      case GCP:
        bind(GcpJobHandlerConfig.class)
            .toInstance(
                GcpJobHandlerConfig.builder()
                    .setGcpProjectId(GCP_PROJECT_ID_OVERRIDE)
                    .setMaxNumAttempts(args.getGcpJobMaxNumAttempts())
                    .setPubSubMaxMessageSizeBytes(GCP_PUBSUB_MAX_MESSAGE_SIZE_BYTES)
                    .setPubSubMessageLeaseSeconds(GCP_PUBSUB_MESSAGE_LEASE_SECONDS)
                    .setPubSubEndpoint(PUBSUB_ENDPOINT_OVERRIDE)
                    .setPubSubTopicId(PUBSUB_TOPIC_ID_OVERRIDE)
                    .setPubSubSubscriptionId(PUBSUB_SUBSCRIPTION_ID_OVERRIDE)
                    .setSpannerEndpoint(SPANNER_ENDPOINT_OVERRIDE)
                    .setSpannerInstanceId(SPANNER_INSTANCE_ID_OVERRIDE)
                    .setSpannerDbName(SPANNER_DATABASE_NAME_OVERRIDE)
                    .build());
        break;
    }
    install(args.getJobClientSelector().getJobClientModule());
  }

  private int getMrpThreadPoolSize() {
    return getValue(Parameter.MRP_THREAD_POOL_SIZE)
        .map(Integer::parseInt)
        .orElse(args.getMrpThreadPoolSize());
  }

  private int getInputDataChunkSize() {
    return getValue(Parameter.INPUT_DATA_CHUNK_SIZE)
        .map(Integer::parseInt)
        .orElse(args.getInputDataChunkSize());
  }

  private int getMaxRecordsPerOutputFile() {
    return getValue(Parameter.MAX_RECORDS_PER_OUTPUT_FILE)
        .map(Integer::parseInt)
        .orElse(args.getMaxRecordsPerOutputFile());
  }

  private String getOrchestratorEndpoint() {
    return getValue(Parameter.ORCHESTRATOR_ENDPOINT).orElse(args.getOrchestratorEndpoint());
  }

  private String getLookupServiceAudience() {
    return getValue(Parameter.LOOKUP_SERVICE_AUDIENCE).orElse(args.getLookupServiceAudience());
  }

  private int getLookupClientThreadPoolSize() {
    return getValue(Parameter.LOOKUP_CLIENT_THREAD_POOL_SIZE)
        .map(Integer::parseInt)
        .orElse(args.getLookupClientThreadPoolSize());
  }

  private int getLookupClientMaxRecordsPerRequest() {
    return getValue(Parameter.LOOKUP_CLIENT_MAX_RECORDS_PER_REQUEST)
        .map(Integer::parseInt)
        .orElse(args.getLookupClientMaxRecordsPerRequest());
  }

  private String getLookupClientClusterGroupId() {
    return getValue(Parameter.LOOKUP_CLIENT_CLUSTER_GROUP_ID)
        .orElse(args.getLookupClientClusterGroupId());
  }

  private int getLookupClientMaxRequestRetries() {
    return getValue(Parameter.LOOKUP_CLIENT_MAX_REQUEST_RETRIES)
        .map(Integer::parseInt)
        .orElse(args.getLookupClientMaxRequestRetries());
  }

  private LookupProtoFormatSelector getLookupProtoFormat() {
    return getValue(Parameter.LOOKUP_PROTO_FORMAT)
        .map(LookupProtoFormatSelector::valueOf)
        .orElse(args.getLookupProtoFormat());
  }

  private int getJobProcessorMaxRetries() {
    return getValue(Parameter.JOB_PROCESSOR_MAX_RETRIES)
        .map(Integer::parseInt)
        .orElse(args.getJobProcessorMaxRetries());
  }

  private int getJobQueueRetryDelaySec() {
    return getValue(Parameter.JOB_QUEUE_RETRY_DELAY_SEC)
        .map(Integer::parseInt)
        .orElse(args.getJobQueueRetryDelaySec());
  }

  private String getAwsDefaultKmsAudience() {
    return getValue(Parameter.AWS_KMS_DEFAULT_AUDIENCE).orElse(args.getAwsDefaultKmsAudience());
  }

  private List<String> getAwsSignaturesList() {
    String stringList =
        getValue(Parameter.AWS_KMS_DEFAULT_SIGNATURES).orElse(args.getAwsKmsSignaturesList());
    return ImmutableList.copyOf(stringList.split(","));
  }

  private Optional<String> getValue(Parameter parameter) {
    try {
      // Parameters are stored in the format CFM-{environment}-{name}
      return parameterClient.getParameter(
          parameter.name(), Optional.of(Parameter.CFM_PREFIX), /* includeEnvironmentParam */ true);
    } catch (ParameterClientException ex) {
      throw new RuntimeException(ex);
    }
  }
}
