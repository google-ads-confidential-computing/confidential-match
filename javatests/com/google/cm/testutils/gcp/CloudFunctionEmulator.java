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

import static com.google.cm.testutils.gcp.TestingContainer.TestingImage.JAVA_BASE;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/** Class for creating and accessing a container running a Cloud Function emulator. */
public final class CloudFunctionEmulator extends TestingContainer<CloudFunctionEmulator> {

  private static final String INVOKER_JAR_FILENAME = "java-function-invoker-1.3.1.jar";
  private static final String INVOKER_JAR_PATH =
      "external/maven/v1/https/repo1.maven.org/maven2/"
          + "com/google/cloud/functions/invoker/java-function-invoker/1.3.1/"
          + INVOKER_JAR_FILENAME;
  private static final int INVOKER_PORT = 8080; // default port for the invoker process

  /** Constructs a new instance. */
  public CloudFunctionEmulator(
      String functionFilename, String functionJarPath, String functionClassTarget) {
    super(JAVA_BASE);
    withExposedPorts(INVOKER_PORT);
    withCopyFileToContainer(
        MountableFile.forHostPath(INVOKER_JAR_PATH), INVOKER_JAR_FILENAME);
    withCopyFileToContainer(
        MountableFile.forHostPath(functionJarPath + functionFilename), functionFilename);
    withCommand(
        INVOKER_JAR_FILENAME, "--classpath", functionFilename, "--target", functionClassTarget);
    setWaitStrategy(Wait.forLogMessage(".*INFO: URL: .*", 1));
  }

  /** Returns the external HTTP endpoint. */
  public String getHostEndpoint() {
    return getHost() + ":" + getMappedPort(INVOKER_PORT);
  }

  /** Returns the container endpoint within the Docker network. */
  public String getContainerEndpoint() {
    return getContainerInfo().getNetworkSettings().getNetworks().values().stream()
            .findFirst()
            .orElseThrow()
            .getIpAddress()
        + ":"
        + INVOKER_PORT;
  }
}
