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

import com.google.cm.mrp.Annotations.PullWorkService;
import com.google.cm.mrp.Annotations.WorkerServiceManager;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.scp.shared.clients.configclient.ParameterClient;
import java.time.Clock;

/**
 * Provides a {@link ServiceManager} for the {@link Service} annotated with {@link PullWorkService
 * PullWorkService}. The service should be able to run as a worker thread. Also provides {@link
 * Clock}.
 */
public final class WorkerModule extends AbstractModule {
  private final ParameterClient parameterClient;

  public WorkerModule(ParameterClient parameterClient) {
    this.parameterClient = parameterClient;
  }

  @Provides
  @WorkerServiceManager
  ServiceManager provideServiceManager(@PullWorkService Service service) {
    return new ServiceManager(ImmutableList.of(service));
  }

  /** Configures bindings for this module. */
  @Override
  protected void configure() {
    bind(Clock.class).toInstance(Clock.systemDefaultZone());
    bind(Service.class).annotatedWith(PullWorkService.class).to(WorkerPullWorkService.class);
    // StartupConfigProvider requires ParameterClient to be injected, which is provided by
    // MatchWorkerParameterClientModule
    bind(StartupConfigProvider.class).to(StartupConfigProviderImpl.class);
  }
}
