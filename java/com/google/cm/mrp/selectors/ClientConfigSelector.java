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

package com.google.cm.mrp.selectors;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.scp.shared.clients.configclient.gcp.GcpClientConfigModule;

/** Enumerates the modules used for providing client configurations. */
public enum ClientConfigSelector {
  NONE(
      new AbstractModule() {
        @Override
        protected void configure() { /* empty module */ }
      }),
  GCP(new GcpClientConfigModule());

  private final Module clientConfigModule;

  ClientConfigSelector(Module module) {
    this.clientConfigModule = module;
  }

  public Module getClientConfigModule() {
    return clientConfigModule;
  }
}
