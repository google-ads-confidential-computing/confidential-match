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

import com.google.cm.mrp.Annotations.WorkerServiceManager;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

/**
 * This class takes a given {@link Module} or {@link Injector} and provides the {@link
 * ServiceManager} for the worker service.
 */
public final class MatchWorker {
  private final Injector injector;

  private MatchWorker(Injector injector) {
    this.injector = injector;
  }

  /**
   * Create a new {@link MatchWorker} from a Guice {@link Module}. The Module is expected to provide
   * a {@link ServiceManager} annotated with {@link WorkerServiceManager}.
   */
  public static MatchWorker create(Module module) {
    return new MatchWorker(Guice.createInjector(module));
  }

  /**
   * Create a new {@link MatchWorker} from a Guice {@link Injector}. The Injector is expected to
   * provide a {@link ServiceManager} annotated with {@link WorkerServiceManager}.
   */
  public static MatchWorker create(Injector injector) {
    return new MatchWorker(injector);
  }

  /** Returns a {@link ServiceManager} for controlling and monitoring a worker service. */
  public ServiceManager getServiceManager() {
    return injector.getInstance(Key.get(ServiceManager.class, WorkerServiceManager.class));
  }

  public Injector getInjector() {
    return injector;
  }
}
