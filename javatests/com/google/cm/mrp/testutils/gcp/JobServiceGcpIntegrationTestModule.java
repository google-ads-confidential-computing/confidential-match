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

import static com.google.cm.mrp.testutils.gcp.Constants.PROJECT_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_SUBSCRIPTION_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_TOPIC_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.SPANNER_DATABASE_NAME;
import static com.google.cm.mrp.testutils.gcp.Constants.SPANNER_INSTANCE_NAME;
import static com.google.cm.testutils.gcp.Constants.JOB_DB_DDL;
import static com.google.cm.testutils.gcp.TestingContainer.TEST_RUNNER_HOSTNAME;
import static com.google.cm.testutils.gcp.TestingContainer.TestingImage.JAVA_BASE;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cm.testutils.gcp.CloudFunctionEmulator;
import com.google.cm.testutils.gcp.GcsEmulator;
import com.google.cm.testutils.gcp.GcsEmulatorModule;
import com.google.cm.testutils.gcp.PubSubEmulator;
import com.google.cm.testutils.gcp.PubSubEmulatorModule;
import com.google.cm.testutils.gcp.SpannerEmulator;
import com.google.cm.testutils.gcp.SpannerEmulatorModule;
import com.google.cm.testutils.gcp.TestingContainer;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.MountableFile;

/**
 * Guice module containing bindings for use in MRP integration testing. Intended to work with the
 * mock server JUnit rule by being extended in the test class, to access the mock server port before
 * rule resolution.
 */
public abstract class JobServiceGcpIntegrationTestModule extends AbstractModule {

  /** Get mock server port. */
  public abstract int getMockServerPort();

  /** Configures the bindings for this module. */
  @Override
  protected void configure() {
    install(new GcsEmulatorModule(PROJECT_ID));
    install(new PubSubEmulatorModule(PROJECT_ID, PUBSUB_TOPIC_ID, PUBSUB_SUBSCRIPTION_ID));
    install(
        new SpannerEmulatorModule(
            PROJECT_ID, SPANNER_INSTANCE_NAME, SPANNER_DATABASE_NAME, JOB_DB_DDL));
  }

  @Provides
  @Singleton
  @SuppressWarnings("resource") // new GenericContainer
  GenericContainer<?> provideWorkerGcpEmulator(
      GcsEmulator gcsEmulator,
      PubSubEmulator pubSubEmulator,
      SpannerEmulator spannerEmulator) {
    // Let mock server act as orchestrator, lookup service, and GCE metadata service
    String mockServerEndpoint = "http://" + TEST_RUNNER_HOSTNAME + ":" + getMockServerPort();
    String workerPath = "javatests/com/google/cm/mrp/testutils/gcp/LocalGcpWorker_deploy.jar";
    MountableFile workerJar = MountableFile.forHostPath(workerPath);
    GenericContainer<?> worker =
        new TestingContainer<>(JAVA_BASE)
            .withCopyFileToContainer(workerJar, "LocalGcpWorker_deploy.jar")
            .withCommand("LocalGcpWorker_deploy.jar")
            .dependsOn(gcsEmulator, pubSubEmulator, spannerEmulator)
            .withEnv("GCS_ENDPOINT", "http://" + gcsEmulator.getContainerEndpoint())
            .withEnv("PUBSUB_ENDPOINT", pubSubEmulator.getContainerEndpoint())
            .withEnv("SPANNER_ENDPOINT", spannerEmulator.getContainerGrpcEndpoint())
            .withEnv("ORCHESTRATOR_ENDPOINT", mockServerEndpoint);
    worker.setWaitStrategy(new LogMessageWaitStrategy().withRegEx("(?s).*Worker starting run.*$"));
    worker.start();
    return worker;
  }

  @Provides
  @Singleton
  CloudFunctionEmulator provideFrontendEmulator(
      PubSubEmulator pubSubEmulator, SpannerEmulator spannerEmulator, GenericContainer<?> worker) {
    CloudFunctionEmulator frontend =
        new CloudFunctionEmulator(
                "LocalGcpFrontend_deploy.jar",
                "javatests/com/google/cm/mrp/testutils/gcp/",
                "com.google.cm.mrp.testutils.gcp.LocalGcpFrontendHttpFunction")
            .dependsOn(spannerEmulator, pubSubEmulator, worker)
            .withEnv("PUBSUB_ENDPOINT", pubSubEmulator.getContainerEndpoint())
            .withEnv("SPANNER_ENDPOINT", spannerEmulator.getContainerGrpcEndpoint());
    frontend.start();
    return frontend;
  }

  @Provides
  @Singleton
  HttpRequestFactory createGcpAuthorizedRequestFactory() {
    // Allow testing HTTP failures
    return new NetHttpTransport()
        .createRequestFactory(httpRequest -> httpRequest.setThrowExceptionOnExecuteError(false));
  }
}
