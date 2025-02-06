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

package com.google.cm.mrp.clients.orchestratorclient;

import com.google.api.client.http.HttpRequestFactory;
import com.google.cm.mrp.clients.orchestratorclient.Annotations.OrchestratorClientHttpRequestFactory;
import com.google.cm.mrp.clients.orchestratorclient.Annotations.OrchestratorEndpoint;
import com.google.inject.AbstractModule;

/**
 * Abstract Guice module that provides an implementation of {@link OrchestratorClient}.
 *
 * <p>The following bindings are provided:
 *
 * <ul>
 *   <li>{@link OrchestratorClient}
 *   <li>Singleton {@link String} annotated with {@link OrchestratorEndpoint OrchestratorEndpoint}
 *   <li>{@link HttpRequestFactory} annotated with {@link OrchestratorClientHttpRequestFactory
 *       OrchestratorClientHttpRequestFactory}
 * </ul>
 */
public abstract class OrchestratorClientModule extends AbstractModule {

  private final String orchestratorEndpoint;

  /** Constructor for {@link OrchestratorClientModule}. */
  public OrchestratorClientModule(String orchestratorEndpoint) {
    this.orchestratorEndpoint = orchestratorEndpoint;
  }

  /** Returns an implementation of {@link HttpRequestFactory}. */
  protected abstract HttpRequestFactory getHttpRequestFactory();

  /** Configures injected dependencies for this module. */
  @Override
  protected void configure() {
    bind(String.class).annotatedWith(OrchestratorEndpoint.class).toInstance(orchestratorEndpoint);
    bind(HttpRequestFactory.class)
        .annotatedWith(OrchestratorClientHttpRequestFactory.class)
        .toInstance(getHttpRequestFactory());
    bind(OrchestratorClient.class).to(OrchestratorClientImpl.class);
  }
}
