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

import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientRequest;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientResponse;

/** Interface for lookup service clients, which send lookup requests to the lookup service. */
public interface LookupServiceClient {

  /** Sends lookup requests to the lookup service, and returns the responses. */
  LookupServiceClientResponse lookupRecords(LookupServiceClientRequest request)
      throws LookupServiceClientException;

  /** Wrapper for exceptions thrown by a {@link LookupServiceClient}. */
  class LookupServiceClientException extends Exception {
    private final String errorCode;

    /** Constructs a new instance. */
    public LookupServiceClientException(String errorCode, String message) {
      super(message);
      this.errorCode = errorCode;
    }

    /** Constructs a new instance. */
    public LookupServiceClientException(String errorCode, Throwable cause) {
      super(cause);
      this.errorCode = errorCode;
    }

    /** Constructs a new instance. */
    public LookupServiceClientException(String errorCode, String message, Throwable cause) {
      super(message, cause);
      this.errorCode = errorCode;
    }

    /** Returns the error code. */
    public String getErrorCode() {
      return errorCode;
    }

    /** Returns a short description of the exception. */
    @Override
    public String toString() {
      return "LookupServiceClient HTTP error: " + errorCode + "\n" + super.toString();
    }
  }
}
