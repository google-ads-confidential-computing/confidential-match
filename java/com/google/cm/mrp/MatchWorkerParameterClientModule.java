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

import com.google.inject.AbstractModule;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.gcp.Annotations.GcpInstanceIdOverride;
import com.google.scp.shared.clients.configclient.gcp.Annotations.GcpInstanceNameOverride;
import com.google.scp.shared.clients.configclient.gcp.Annotations.GcpProjectIdOverride;
import com.google.scp.shared.clients.configclient.gcp.Annotations.GcpZoneOverride;

/**
 * Module for instantiating a {@link ParameterClient}. The injector should be used to create a child
 * injector with the {@link MatchWorkerModule}.
 */
public final class MatchWorkerParameterClientModule extends AbstractModule {

  private static final String GCP_PROJECT_ID_OVERRIDE = "";
  private static final String GCE_ID_OVERRIDE = "";
  private static final String GCE_NAME_OVERRIDE = "";
  private static final String GCE_ZONE_OVERRIDE = "";

  private final MatchWorkerArgs args;

  /** Constructs a new instance. */
  public MatchWorkerParameterClientModule(MatchWorkerArgs args) {
    this.args = args;
  }

  /** Configures the bindings for this module. */
  @Override
  protected void configure() {
    switch (args.getClientConfigSelector()) {
      case NONE:
        break;
      case GCP:
        bind(String.class)
            .annotatedWith(GcpProjectIdOverride.class)
            .toInstance(GCP_PROJECT_ID_OVERRIDE);
        bind(String.class).annotatedWith(GcpInstanceIdOverride.class).toInstance(GCE_ID_OVERRIDE);
        bind(String.class)
            .annotatedWith(GcpInstanceNameOverride.class)
            .toInstance(GCE_NAME_OVERRIDE);
        bind(String.class).annotatedWith(GcpZoneOverride.class).toInstance(GCE_ZONE_OVERRIDE);
        break;
    }
    install(args.getClientConfigSelector().getClientConfigModule());
    install(args.getParameterClientSelector().getParameterClientModule());
  }
}
