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

import static com.google.cm.testutils.gcp.TestingContainer.TestingImage.PUBSUB_EMULATOR;

import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/** Class for creating and accessing a container running a GCP Pub/Sub emulator. */
public final class PubSubEmulator extends TestingContainer<PubSubEmulator> {

  private static final int PORT = 8085;

  private final String projectId;

  /** Constructs a new instance. */
  public PubSubEmulator(String projectId) {
    super(PUBSUB_EMULATOR);
    this.projectId = projectId;
    withExposedPorts(PORT);
    setWaitStrategy(new LogMessageWaitStrategy().withRegEx("(?s).*started.*$"));
    withCommand("gcloud", "beta", "emulators", "pubsub", "start", "--host-port", "0.0.0.0:" + PORT);
  }

  /** Returns the project ID. */
  public String getProjectId() {
    return projectId;
  }

  /** Returns the external HTTP endpoint. */
  public String getHostEndpoint() {
    return getHost() + ":" + getMappedPort(PORT);
  }

  /** Returns the container endpoint within the Docker network. */
  public String getContainerEndpoint() {
    return getContainerInfo().getNetworkSettings().getNetworks().values().stream()
            .findFirst()
            .orElseThrow()
            .getIpAddress()
        + ":"
        + PORT;
  }
}
