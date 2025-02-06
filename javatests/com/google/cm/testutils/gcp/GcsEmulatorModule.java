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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.testcontainers.utility.DockerImageName;

/** Guice module providing GCS emulator bindings. */
public final class GcsEmulatorModule extends AbstractModule {

  private final String projectId;

  /** Creates a new instance. */
  public GcsEmulatorModule(String projectId) {
    this.projectId = projectId;
  }

  @Provides
  @Singleton
  public GcsEmulator providesLocalGcsContainer() {
    var emulator = new GcsEmulator();
    emulator.start();
    return emulator;
  }

  @Provides
  @Singleton
  public Storage providesStorage(GcsEmulator localGcsContainer) {
    return StorageOptions.newBuilder()
        .setProjectId(projectId)
        .setHost("http://" + localGcsContainer.getHostEndpoint())
        .build()
        .getService();
  }
}
