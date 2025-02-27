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

import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.io.Closeable;

/** Interface to config getting {@link CloudAeadSelector} from different cloud KMS providers. */
public interface AeadProvider extends Closeable {

  /** Gets the selector to retrieve Aeads from Cloud KMS using {@link AeadProviderParameters} */
  CloudAeadSelector getAeadSelector(AeadProviderParameters aeadCloudParameters)
      throws AeadProviderException;

  class AeadProviderException extends Exception {

    public AeadProviderException(Throwable cause) {
      super(cause);
    }

    public AeadProviderException(String message) {
      super(message);
    }

    public AeadProviderException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  class UncheckedAeadProviderException extends RuntimeException {

    public UncheckedAeadProviderException(Throwable cause) {
      super(cause);
    }

    public UncheckedAeadProviderException(String message) {
      super(message);
    }

    public UncheckedAeadProviderException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
