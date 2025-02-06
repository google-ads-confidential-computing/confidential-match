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

import static java.nio.file.Files.newInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.TestcontainersConfiguration;

/** Containers used for testing. */
public class TestingContainer<T extends GenericContainer<T>> extends GenericContainer<T> {

  // Hostname that can be used by containers to access the host
  public static final String HOST_EGRESS_HOSTNAME = "host.docker.internal";

  static {
    // Disable pulling and using the test containers startup check container
    TestcontainersConfiguration.getInstance().updateUserConfig("checks.disable", "true");
  }

  /**
   * A testing image that is loaded without additional pulls. Be aware of other <a
   * href="https://java.testcontainers.org/features/configuration/#customizing-images">implicit
   * image pulls from TestContainers</a>, e.g. <b>testcontainers/ryuk</b>.
   */
  public TestingContainer(TestingImage image) {
    super(image.load());
    // Allow access to host ports without pulling and using the test containers sshd image
    withExtraHost(HOST_EGRESS_HOSTNAME, "host-gateway");
    // Never attempt remote pulls for these testing images during the tests
    withImagePullPolicy(unused -> false);

    // Require tests to disable pulling and using the test containers cleanup container
    // Shutdown hooks are automatically created instead
    if (!"true".equals(System.getenv("TESTCONTAINERS_RYUK_DISABLED"))) {
      throw new RuntimeException(
          "Environment variable TESTCONTAINERS_RYUK_DISABLED must be set to true.");
    }
  }

  /**
   * Represents an image pulled specifically for testing, that is loaded as a tar file. All images
   * here must be in the root WORKSPACE file.
   */
  public enum TestingImage {
    JAVA_BASE,
    PUBSUB_EMULATOR,
    SPANNER_EMULATOR,
    GCS_EMULATOR;

    /** Loads the tar file as a Docker image and returns its name. */
    public DockerImageName load() {
      String imageName = "bazel/javatests/com/google/cm/testutils/gcp:" + name().toLowerCase();
      String tarFile = "javatests/com/google/cm/testutils/gcp/" + name().toLowerCase() + ".tar";
      try (InputStream tarStream = newInputStream(Path.of(tarFile))) {
        DockerClientFactory.instance().client().loadImageCmd(tarStream).exec();
        return DockerImageName.parse(imageName);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
