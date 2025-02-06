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

import com.google.cm.mrp.clients.lookupserviceclient.Annotations.LookupServiceShardClientHttpClient;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;

/** Abstract Guice module that provides an implementation of {@link LookupServiceShardClient}. */
public abstract class LookupServiceShardClientModule extends AbstractModule {

  /** Returns an implementation of {@link CloseableHttpAsyncClient}. */
  protected abstract CloseableHttpAsyncClient getCloseableHttpAsyncClient();

  /** Configures injected dependencies for this module. */
  @Override
  protected void configure() {
    bind(CloseableHttpAsyncClient.class)
        .annotatedWith(LookupServiceShardClientHttpClient.class)
        .toInstance(getCloseableHttpAsyncClient());
    bind(LookupServiceShardClient.class).to(LookupServiceShardClientImpl.class).in(Singleton.class);
  }
}
