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

import com.google.inject.Module;
import com.google.scp.operator.cpio.jobclient.gcp.GcpJobHandlerModule;
import com.google.scp.operator.cpio.jobclient.local.LocalFileJobHandlerModule;

/** Enumerates the modules used for instantiating a job client. */
public enum JobClientSelector {
  LOCAL_FILE(new LocalFileJobHandlerModule()),
  GCP(new GcpJobHandlerModule());

  private final Module jobClientModule;

  JobClientSelector(Module module) {
    this.jobClientModule = module;
  }

  public Module getJobClientModule() {
    return jobClientModule;
  }
}
