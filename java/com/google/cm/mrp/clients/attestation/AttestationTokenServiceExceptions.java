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

package com.google.cm.mrp.clients.attestation;

/** Exceptions for AttestationTokenService. */
public final class AttestationTokenServiceExceptions {

  /** Creates AttestationTokenServiceException. */
  public static final class AttestationTokenServiceException extends Exception {

    public AttestationTokenServiceException(String message, Throwable cause) {
      super(message, cause);
    }

    public AttestationTokenServiceException(String message) {
      super(message);
    }
  }

  /** Creates UncheckedAttestationTokenServiceException. */
  public static final class UncheckedAttestationTokenServiceException extends RuntimeException {

    public UncheckedAttestationTokenServiceException(String message, Throwable cause) {
      super(message, cause);
    }

    public UncheckedAttestationTokenServiceException(String message) {
      super(message);
    }
  }
}
