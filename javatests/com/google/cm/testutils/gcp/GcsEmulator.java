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

import static com.google.cm.testutils.gcp.TestingContainer.TestingImage.GCS_EMULATOR;

/** Class for creating and accessing a container running a GCS emulator. */
public final class GcsEmulator extends TestingContainer<GcsEmulator> {

  private static final int PORT = 9000;

  /** Creates a new instance. */
  public GcsEmulator() {
    super(GCS_EMULATOR);
    withExposedPorts(PORT);
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
