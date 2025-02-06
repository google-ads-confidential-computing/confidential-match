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

package com.google.cm.mrp.clients.lookupserviceclient;

import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceClientClusterGroupId;
import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceClientExecutor;
import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceClientMaxRecordsPerRequest;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClientModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/** Abstract Guice module that provides an implementation of {@link LookupServiceClient}. */
public abstract class LookupServiceClientModule extends AbstractModule {

  private final int threadCount;
  private final int maxRecordsPerRequest;
  private final String clusterGroupId;

  /** Constructor for {@link LookupServiceClientModule}. */
  public LookupServiceClientModule(
      int threadCount, int maxRecordsPerRequest, String clusterGroupId) {
    this.threadCount = threadCount;
    this.maxRecordsPerRequest = maxRecordsPerRequest;
    this.clusterGroupId = clusterGroupId;
  }

  /** Returns a module that provides an implementation of {@link OrchestratorClient}. */
  protected abstract OrchestratorClientModule getOrchestratorClientModule();

  /** Returns a module that provides an implementation of {@link LookupServiceShardClient}. */
  protected abstract LookupServiceShardClientModule getLookupServiceShardClientModule();

  /** Configures injected dependencies for this module. */
  @Override
  protected void configure() {
    install(getOrchestratorClientModule());
    install(getLookupServiceShardClientModule());
    bind(LookupServiceClient.class).to(LookupServiceClientImpl.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  @LookupServiceClientExecutor
  @SuppressWarnings("UnstableApiUsage") // MoreExecutors::getExitingExecutorService
  Executor provideExecutor() {
    // Threads terminate when JVM exits
    return getExitingExecutorService((ThreadPoolExecutor) newFixedThreadPool(threadCount));
  }

  @Provides
  @Singleton
  @LookupServiceClientClusterGroupId
  String provideClusterGroupId() {
    return clusterGroupId;
  }

  @Provides
  @LookupServiceClientMaxRecordsPerRequest
  int provideMaxRecordsPerRequest() {
    return maxRecordsPerRequest;
  }
}
