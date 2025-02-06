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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.inject.AbstractModule;
import org.testcontainers.utility.DockerImageName;

/** Guice module providing Spanner emulator bindings. */
public final class SpannerEmulatorModule extends AbstractModule {

  private final String projectName;
  private final String instanceName;
  private final String databaseName;
  private final Iterable<String> createTableStatements;

  /** Creates a new instance. */
  public SpannerEmulatorModule(
      String projectName,
      String instanceName,
      String databaseName,
      Iterable<String> createTableStatements) {
    this.projectName = projectName;
    this.instanceName = instanceName;
    this.databaseName = databaseName;
    this.createTableStatements = createTableStatements;
  }

  /** Configures injected dependencies for this module. */
  @Override
  protected void configure() {
    var emulator = new SpannerEmulator(projectName);
    emulator.start();
    emulator.createDatabase(instanceName, databaseName, createTableStatements);
    bind(SpannerEmulator.class).toInstance(emulator);

    var database = DatabaseId.of(projectName, instanceName, databaseName);
    bind(DatabaseClient.class).toInstance(emulator.getSpanner().getDatabaseClient(database));
  }
}
