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
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.gcp.GcpParameterModule;
import com.google.scp.shared.clients.configclient.model.GetParameterRequest;
import java.util.Optional;

/** Enumerates the modules used for instantiating a parameter client. */
public enum ParameterClientSelector {
  LOCAL_ARGS(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ParameterClient.class)
              .toInstance(
                  // A {@link ParameterClient} that never retrieves parameters.
                  // It always defers to command line args.
                  new ParameterClient() {
                    /** Always returns an empty optional. */
                    @Override
                    public Optional<String> getParameter(String param) {
                      return Optional.empty();
                    }

                    /** Always returns an empty optional. */
                    @Override
                    public Optional<String> getParameter(
                        String param,
                        Optional<String> paramPrefix,
                        boolean includeEnvironmentParam,
                        boolean getLatest) {
                      return Optional.empty();
                    }

                    @Override
                    public Optional<String> getParameter(GetParameterRequest getParameterRequest) {
                      return Optional.empty();
                    }

                    @Override
                    public Optional<String> getLatestParameter(String param) {
                      return Optional.empty();
                    }

                    /** Always returns an optional of "LOCAL_ARGS". */
                    @Override
                    public Optional<String> getEnvironmentName() {
                      return Optional.of("LOCAL_ARGS");
                    }

                    @Override
                    public Optional<String> getWorkgroupId() {
                      return Optional.empty();
                    }
                  });
        }
      }),
  GCP(new GcpParameterModule());

  private final Module parameterClientModule;

  ParameterClientSelector(Module module) {
    this.parameterClientModule = module;
  }

  public Module getParameterClientModule() {
    return parameterClientModule;
  }
}
