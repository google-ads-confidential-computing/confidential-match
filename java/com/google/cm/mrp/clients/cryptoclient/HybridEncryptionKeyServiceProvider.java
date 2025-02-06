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

package com.google.cm.mrp.clients.cryptoclient;

import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;

/**
 * Interface to config how to get {@link HybridEncryptionKeyService} from {@link CoordinatorKeyInfo}
 */
public interface HybridEncryptionKeyServiceProvider {

  /** Gets HybridEncryptionKeyService based on the {@link CoordinatorKeyInfo} */
  HybridEncryptionKeyService getHybridEncryptionKeyService(CoordinatorKeyInfo coordinatorKeyInfo)
      throws HybridEncryptionKeyServiceProviderException;

  class HybridEncryptionKeyServiceProviderException extends Exception {

    public HybridEncryptionKeyServiceProviderException(Throwable cause) {
      super(cause);
    }
  }
}
