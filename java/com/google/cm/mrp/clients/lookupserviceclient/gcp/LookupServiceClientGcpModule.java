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

package com.google.cm.mrp.clients.lookupserviceclient.gcp;

import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClientModule;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClientModule;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClientModule;
import com.google.cm.mrp.clients.orchestratorclient.gcp.OrchestratorClientGcpModule;

/** Guice module that provides an implementation of {@link LookupServiceClient} for GCP. */
public final class LookupServiceClientGcpModule extends LookupServiceClientModule {

  private final String orchestratorEndpoint;
  private final String lookupServiceAudience;
  private final int maxRequestRetries;

  /** Constructs a new instance. */
  public LookupServiceClientGcpModule(
      String orchestratorEndpoint,
      String lookupServiceAudience,
      int threadCount,
      int maxRecordsPerRequest,
      int maxRequestRetries,
      String clusterGroupId) {
    super(threadCount, maxRecordsPerRequest, clusterGroupId);
    this.orchestratorEndpoint = orchestratorEndpoint;
    this.lookupServiceAudience = lookupServiceAudience;
    this.maxRequestRetries = maxRequestRetries;
  }

  /** {@inheritDoc} */
  @Override
  protected OrchestratorClientModule getOrchestratorClientModule() {
    return new OrchestratorClientGcpModule(orchestratorEndpoint);
  }

  /** {@inheritDoc} */
  @Override
  protected LookupServiceShardClientModule getLookupServiceShardClientModule() {
    return new LookupServiceShardClientGcpModule(lookupServiceAudience, maxRequestRetries);
  }
}
