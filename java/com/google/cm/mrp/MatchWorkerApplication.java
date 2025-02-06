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

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.scp.shared.clients.configclient.ParameterClient;

/**
 * Entrypoint for the MRP worker application. Parses command line arguments and runs the worker in
 * a new thread.
 */
public final class MatchWorkerApplication {

  public static void main(String[] args) {
    // Read args from command line
    MatchWorkerArgs cliArgs = new MatchWorkerArgs();
    JCommander.newBuilder().allowParameterOverwriting(true).addObject(cliArgs).build().parse(args);

    // Get the parameter client, then add it and its dependencies back into the worker module
    Injector paramClientInjector =
        Guice.createInjector(new MatchWorkerParameterClientModule(cliArgs));
    // Will throw if a parameter client is not provided
    ParameterClient parameterClient =
        paramClientInjector.getInstance(Key.get(ParameterClient.class));
    // This merges bindings from MatchWorkerParameterClientModule and MatchWorkerModule
    // It also reuses dependencies that have already been instantiated
    Injector workerInjector =
        paramClientInjector.createChildInjector(new MatchWorkerModule(cliArgs, parameterClient));

    // Run worker
    MatchWorker.create(workerInjector).getServiceManager().startAsync();
  }
}
