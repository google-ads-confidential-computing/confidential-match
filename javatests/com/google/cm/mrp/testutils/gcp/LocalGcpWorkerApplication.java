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

package com.google.cm.mrp.testutils.gcp;

import static com.google.cm.mrp.Parameter.MIC_FEATURE_ENABLED;
import static com.google.cm.mrp.Parameter.NOTIFICATION_TOPIC_PREFIX;
import static com.google.cm.mrp.selectors.LookupProtoFormatSelector.JSON;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.getAeadFromJsonKeyset;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getHybridDecryptFromJsonKeyset;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getHybridEncryptFromJsonKeyset;
import static com.google.cm.mrp.testutils.gcp.Constants.CLUSTER_GROUP_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.DATA_CHUNK_SIZE;
import static com.google.cm.mrp.testutils.gcp.Constants.JOB_QUEUE_MAX_ATTEMPTS;
import static com.google.cm.mrp.testutils.gcp.Constants.JOB_QUEUE_RETRY_DELAY_SEC;
import static com.google.cm.mrp.testutils.gcp.Constants.LOCAL_MIC_FEATURE_ENABLED;
import static com.google.cm.mrp.testutils.gcp.Constants.LOOKUP_CLIENT_MAX_RECORDS_PER_REQUEST;
import static com.google.cm.mrp.testutils.gcp.Constants.LOOKUP_CLIENT_THREADS;
import static com.google.cm.mrp.testutils.gcp.Constants.LOOKUP_SERVICE_MAX_RETRIES;
import static com.google.cm.mrp.testutils.gcp.Constants.MAX_RECORDS_PER_OUTPUT_FILE;
import static com.google.cm.mrp.testutils.gcp.Constants.MRP_JOB_PROCESSOR_RETRIES;
import static com.google.cm.mrp.testutils.gcp.Constants.MRP_THREADS;
import static com.google.cm.mrp.testutils.gcp.Constants.PROJECT_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_MAX_MESSAGE_SIZE_BYTES;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_MESSAGE_LEASE_SECONDS;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_SUBSCRIPTION_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_TOPIC_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.SPANNER_DATABASE_NAME;
import static com.google.cm.mrp.testutils.gcp.Constants.SPANNER_INSTANCE_NAME;
import static com.google.cm.mrp.testutils.gcp.Constants.TEST_HYBRID_PRIVATE_KEYSET;
import static com.google.cm.mrp.testutils.gcp.Constants.TEST_KEK_JSON;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cm.mrp.Annotations.JobProcessorMaxRetries;
import com.google.cm.mrp.Annotations.JobQueueRetryDelaySec;
import com.google.cm.mrp.FeatureFlagProvider;
import com.google.cm.mrp.FeatureFlagProviderImpl;
import com.google.cm.mrp.JobProcessor;
import com.google.cm.mrp.MatchJobProcessor;
import com.google.cm.mrp.MatchWorker;
import com.google.cm.mrp.StartupConfigProvider;
import com.google.cm.mrp.StartupConfigProviderImpl;
import com.google.cm.mrp.WorkerModule;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.AeadProviderFactory;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClientImpl;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClientImpl;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClientImpl;
import com.google.cm.mrp.dataprocessor.DataProcessorModule;
import com.google.cm.mrp.selectors.JobClientSelector;
import com.google.cm.mrp.selectors.LifecycleClientSelector;
import com.google.cm.mrp.selectors.MetricClientSelector;
import com.google.cm.util.ExponentialBackoffRetryStrategy;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.HybridEncrypt;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.gcp.GcsBlobStorageClient;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;
import com.google.scp.operator.cpio.jobclient.gcp.GcpJobHandlerConfig;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.gcp.Annotations.GcpProjectId;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import com.google.scp.shared.mapper.TimeObjectMapper;
import java.util.Optional;
import javax.net.ssl.HttpsURLConnection;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.mockserver.configuration.Configuration;
import org.mockserver.logging.MockServerLogger;
import org.mockserver.socket.tls.KeyStoreFactory;

/** Entrypoint for a local worker that uses emulated GCP clients. */
public final class LocalGcpWorkerApplication {

  /** Application entry point. */
  public static void main(String[] args) {
    MatchWorker.create(new LocalGcpWorkerModule()).getServiceManager().startAsync();
  }

  private static final class LocalGcpWorkerModule extends AbstractModule {

    private static final Timeout DEFAULT_TIMEOUT = Timeout.ofSeconds(2);
    private static final String GCS_ENDPOINT = System.getenv("GCS_ENDPOINT");
    private static final Optional<String> PUBSUB_ENDPOINT =
        Optional.ofNullable(System.getenv("PUBSUB_ENDPOINT"));
    private static final Optional<String> SPANNER_ENDPOINT =
        Optional.ofNullable(System.getenv("SPANNER_ENDPOINT"));
    private static final String ORCHESTRATOR_ENDPOINT = System.getenv("ORCHESTRATOR_ENDPOINT");

    /** Configures the bindings for this module. */
    @Override
    protected void configure() {
      // Required SSL config for making HTTPS connections to mock server containers
      HttpsURLConnection.setDefaultSSLSocketFactory(
          new KeyStoreFactory(Configuration.configuration(), new MockServerLogger())
              .sslContext()
              .getSocketFactory());
      bind(String.class).annotatedWith(GcpProjectId.class).toInstance(PROJECT_ID);
      bind(Integer.class)
          .annotatedWith(JobProcessorMaxRetries.class)
          .toInstance(MRP_JOB_PROCESSOR_RETRIES);
      bind(Integer.class)
          .annotatedWith(JobQueueRetryDelaySec.class)
          .toInstance(JOB_QUEUE_RETRY_DELAY_SEC);
      bind(JobProcessor.class).to(MatchJobProcessor.class);
      bind(ObjectMapper.class).to(TimeObjectMapper.class);
      bind(FeatureFlagProvider.class).to(FeatureFlagProviderImpl.class);
      bind(StartupConfigProvider.class).to(StartupConfigProviderImpl.class);
      install(new WorkerModule(createParameterClient()));
      install(new DataProcessorModule(MRP_THREADS, DATA_CHUNK_SIZE, MAX_RECORDS_PER_OUTPUT_FILE));
      install(LifecycleClientSelector.LOCAL.getLifecycleClientModule());
      install(MetricClientSelector.LOCAL.getMetricClientModule());
      install(JobClientSelector.GCP.getJobClientModule());

      bind(ParameterClient.class).toInstance(createParameterClient());
      install(
          new FactoryModuleBuilder()
              .implement(AeadProvider.class, Names.named("gcp"), MockGcpAeadProvider.class)
              .build(AeadProviderFactory.class));
    }

    @Provides
    @Singleton
    LookupServiceClient provideLookupServiceClient() {
      // Create orchestrator client's HTTP client
      HttpRequestFactory orchestratorHttpClient = new NetHttpTransport().createRequestFactory();
      // Create lookup service shard client's HTTP client
      TimeValue lookupRequestRetryDelayBase = TimeValue.ofMilliseconds(10);
      int lookupRequestRetryDelayMultiplier = 2;
      HttpRequestRetryStrategy retryStrategy =
          new ExponentialBackoffRetryStrategy(
              LOOKUP_SERVICE_MAX_RETRIES,
              lookupRequestRetryDelayBase,
              lookupRequestRetryDelayMultiplier);
      RequestConfig requestConfig =
          RequestConfig.custom()
              .setConnectionRequestTimeout(DEFAULT_TIMEOUT)
              .setConnectTimeout(DEFAULT_TIMEOUT)
              .setResponseTimeout(DEFAULT_TIMEOUT)
              .build();
      CloseableHttpAsyncClient shardHttpClient =
          HttpAsyncClients.customHttp2()
              .setRetryStrategy(retryStrategy)
              .setDefaultConnectionConfig(
                  ConnectionConfig.custom().setConnectTimeout(DEFAULT_TIMEOUT).build())
              .setIOReactorConfig(IOReactorConfig.custom().setSoTimeout(DEFAULT_TIMEOUT).build())
              .setDefaultRequestConfig(requestConfig)
              .build();

      shardHttpClient.start();
      // Create lookup service client
      return new LookupServiceClientImpl(
          new OrchestratorClientImpl(orchestratorHttpClient, ORCHESTRATOR_ENDPOINT),
          new LookupServiceShardClientImpl(shardHttpClient, JSON.getFormatHandler()),
          newFixedThreadPool(LOOKUP_CLIENT_THREADS),
          LOOKUP_CLIENT_MAX_RECORDS_PER_REQUEST,
          CLUSTER_GROUP_ID);
    }

    @Provides
    @Singleton
    BlobStorageClient provideBlobStorageClient() {
      return new GcsBlobStorageClient(
          StorageOptions.newBuilder()
              .setProjectId(PROJECT_ID)
              .setHost(GCS_ENDPOINT)
              .setCredentials(NoCredentials.getInstance())
              .build()
              .getService());
    }

    @Provides
    @Singleton
    GcpJobHandlerConfig provideGcpJobHandlerConfig() {
      return GcpJobHandlerConfig.builder()
          .setGcpProjectId(PROJECT_ID)
          .setMaxNumAttempts(JOB_QUEUE_MAX_ATTEMPTS)
          .setPubSubMaxMessageSizeBytes(PUBSUB_MAX_MESSAGE_SIZE_BYTES)
          .setPubSubMessageLeaseSeconds(PUBSUB_MESSAGE_LEASE_SECONDS)
          .setPubSubEndpoint(PUBSUB_ENDPOINT)
          .setPubSubTopicId(PUBSUB_TOPIC_ID)
          .setPubSubSubscriptionId(PUBSUB_SUBSCRIPTION_ID)
          .setSpannerEndpoint(SPANNER_ENDPOINT)
          .setSpannerInstanceId(SPANNER_INSTANCE_NAME)
          .setSpannerDbName(SPANNER_DATABASE_NAME)
          .build();
    }

    @Provides
    @Singleton
    HybridEncryptionKeyServiceProvider provideHybridEncryptionKeyServiceProvider() {
      return (unusedCoordinatorInfo) -> getMockHybridEncryptionKeyService();
    }

    @Singleton
    private ParameterClient createParameterClient() {
      return new ParameterClient() {
        /** Always returns an empty optional. */
        @Override
        public Optional<String> getParameter(String param) {
          return Optional.empty();
        }

        /** Returns an empty optional except MIC_FEATURE_ENABLED and notification_topic_mic flags */
        @Override
        public Optional<String> getParameter(
            String param,
            Optional<String> paramPrefix,
            boolean includeEnvironmentParam,
            boolean getLatest) {
          if (param.equalsIgnoreCase(MIC_FEATURE_ENABLED.name())) {
            return Optional.of(String.valueOf(LOCAL_MIC_FEATURE_ENABLED));
          }
          if (param.equalsIgnoreCase(NOTIFICATION_TOPIC_PREFIX + "MIC")) {
            return Optional.of("fake_mic_topic");
          }
          return Optional.empty();
        }

        /** Always returns an empty optional. */
        @Override
        public Optional<String> getLatestParameter(String param) {
          return Optional.empty();
        }

        /** Always returns an optional of "LOCAL_ARGS". */
        @Override
        public Optional<String> getEnvironmentName() {
          return Optional.of("LOCAL_ARGS");
        }
      };
    }
  }

  private static HybridEncryptionKeyService getMockHybridEncryptionKeyService() {
    return new HybridEncryptionKeyService() {
      /** Returns a default static HybridDecrypt primitive. */
      @Override
      public HybridDecrypt getDecrypter(String keyId) {
        return getHybridDecryptFromJsonKeyset(TEST_HYBRID_PRIVATE_KEYSET, TEST_KEK_JSON);
      }

      /** Returns a default static HybridEncrypt primitive. */
      @Override
      public HybridEncrypt getEncrypter(String keyId) {
        return getHybridEncryptFromJsonKeyset(TEST_HYBRID_PRIVATE_KEYSET, TEST_KEK_JSON);
      }
    };
  }

  private static class MockGcpAeadProvider implements AeadProvider {

    @Override
    public CloudAeadSelector getAeadSelector(AeadProviderParameters aeadProviderParameters) {
      return (unusedKekUri) -> getAeadFromJsonKeyset(TEST_KEK_JSON);
    }
  }
}
