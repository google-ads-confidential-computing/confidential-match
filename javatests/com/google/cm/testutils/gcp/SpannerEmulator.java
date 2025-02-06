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

package com.google.cm.testutils.gcp;

import static com.google.cloud.spanner.SpannerExceptionFactory.asSpannerException;
import static com.google.cm.testutils.gcp.TestingContainer.TestingImage.SPANNER_EMULATOR;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.concurrent.ExecutionException;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/** Class for creating and accessing a container running a Spanner emulator. */
public final class SpannerEmulator extends TestingContainer<SpannerEmulator> {

  private static final int GRPC_PORT = 9010;
  private static final int HTTP_PORT = 9020;
  private static final String EMULATOR_CONFIG = "emulator-config";
  private static final String STARTUP_LOG_MESSAGE_REGEX = ".*Cloud Spanner emulator running\\..*";

  private final String projectId;
  private Spanner spanner; // only available after emulator starts

  /** Constructs a new instance. */
  public SpannerEmulator(String projectId) {
    super(SPANNER_EMULATOR);
    this.projectId = projectId;
    withExposedPorts(HTTP_PORT, GRPC_PORT);
    setWaitStrategy(new LogMessageWaitStrategy().withRegEx(STARTUP_LOG_MESSAGE_REGEX));
  }

  /** Gets the emulator's Spanner interface. */
  public Spanner getSpanner() {
    if (spanner == null) {
      spanner =
          SpannerOptions.newBuilder()
              .setEmulatorHost(getHostGrpcEndpoint())
              .setCredentials(NoCredentials.getInstance())
              .setProjectId(projectId)
              .build()
              .getService();
    }
    return spanner;
  }

  /** Creates a database and runs DDL statements. */
  public void createDatabase(String instance, String database, Iterable<String> statements) {
    try {
      Spanner spanner = getSpanner();
      spanner
          .getInstanceAdminClient()
          .createInstance(
              InstanceInfo.newBuilder(InstanceId.of(projectId, instance))
                  .setNodeCount(1)
                  .setDisplayName("Test instance")
                  .setInstanceConfigId(InstanceConfigId.of(projectId, EMULATOR_CONFIG))
                  .build())
          .get();
      spanner.getDatabaseAdminClient().createDatabase(instance, database, statements).get();
    } catch (ExecutionException ex) {
      throw asSpannerException(ex.getCause());
    } catch (InterruptedException ex) {
      throw asSpannerException(ex);
    }
  }

  /** Returns the project ID. */
  public String getProjectId() { return projectId; }

  /**
   * Returns the external GRPC endpoint. Can be used as the argument for {@link
   * SpannerOptions.Builder#setEmulatorHost}.
   */
  public String getHostGrpcEndpoint() {
    return getHost() + ":" + getMappedPort(GRPC_PORT);
  }

  /** Returns the external HTTP endpoint. */
  public String getHostHttpEndpoint() {
    return getHost() + ":" + getMappedPort(HTTP_PORT);
  }

  /** Returns the container GRPC endpoint within the Docker network. */
  public String getContainerGrpcEndpoint() {
    return getContainerInfo().getNetworkSettings().getNetworks().values().stream()
            .findFirst()
            .orElseThrow()
            .getIpAddress()
        + ":"
        + GRPC_PORT;
  }

  /** Returns the container HTTP endpoint within the Docker network. */
  public String getContainerHttpEndpoint() {
    return getContainerInfo().getNetworkSettings().getNetworks().values().stream()
            .findFirst()
            .orElseThrow()
            .getIpAddress()
        + ":"
        + HTTP_PORT;
  }
}
