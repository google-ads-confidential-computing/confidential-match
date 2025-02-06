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

package com.google.cm.mrp.clients.orchestratorclient.gcp;

import static com.google.cm.util.gcp.AuthUtils.getIdTokenCredentials;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cm.mrp.clients.orchestratorclient.Annotations.OrchestratorClientHttpRequestFactory;
import com.google.cm.mrp.clients.orchestratorclient.Annotations.OrchestratorEndpoint;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClientModule;
import java.io.IOException;

/**
 * Guice module that provides an implementation of {@link OrchestratorClient} for use with GCP.
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
public final class OrchestratorClientGcpModule extends OrchestratorClientModule {

  /** Constructor for {@link OrchestratorClientGcpModule}. */
  public OrchestratorClientGcpModule(String orchestratorEndpoint) {
    super(orchestratorEndpoint);
  }

  private static void authInitializer(HttpRequest httpRequest) throws IOException {
    var adapter = new HttpCredentialsAdapter(getIdTokenCredentials(httpRequest.getUrl().build()));
    adapter.initialize(httpRequest); // Sets auth headers and retry handler
  }

  /** Returns an implementation of {@link HttpRequestFactory} for use with GCP. */
  @Override
  protected HttpRequestFactory getHttpRequestFactory() {
    return new NetHttpTransport()
        .createRequestFactory(OrchestratorClientGcpModule::authInitializer);
  }
}
