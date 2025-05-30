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
import com.google.scp.operator.cpio.blobstorageclient.gcp.GcsBlobStorageClientModule;
import com.google.scp.operator.cpio.blobstorageclient.testing.FSBlobStorageClientModule;

/** Enumerates the modules used for instantiating a blob storage client. */
public enum BlobStorageClientSelector {
  LOCAL_FS(new FSBlobStorageClientModule()),
  GCS(new GcsBlobStorageClientModule());

  private final Module blobStorageClientModule;

  BlobStorageClientSelector(Module module) {
    this.blobStorageClientModule = module;
  }

  public Module getBlobStorageClientModule() {
    return blobStorageClientModule;
  }
}
