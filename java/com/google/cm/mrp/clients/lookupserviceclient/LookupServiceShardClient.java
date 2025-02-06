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

import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;

/** Interface for lookup service shard clients, which send lookup requests to the lookup service. */
public interface LookupServiceShardClient {

  /** Sends a lookup request to a lookup service shard endpoint, and returns the response. */
  LookupResponse lookupRecords(String shardEndpoint, LookupRequest lookupRequest)
      throws LookupServiceShardClientException;

  /** Wrapper for exceptions thrown by a {@link LookupServiceShardClient}. */
  class LookupServiceShardClientException extends Exception {
    // TODO(b/309460063): Switch to Code enum
    private final String errorCode;
    private final String errorReason;

    /** Constructs a new instance. */
    public LookupServiceShardClientException(String errorCode, String errorReason, String message) {
      super(message);
      this.errorCode = errorCode;
      this.errorReason = errorReason;
    }

    /** Constructs a new instance. */
    public LookupServiceShardClientException(String errorCode, Throwable cause) {
      super(cause);
      this.errorCode = errorCode;
      errorReason = null;
    }

    /** Constructs a new instance. */
    public LookupServiceShardClientException(String errorCode, String message, Throwable cause) {
      super(message, cause);
      this.errorCode = errorCode;
      errorReason = null;
    }

    /** Returns the error code. */
    public String getErrorCode() {
      return errorCode;
    }

    /** Returns the error Reason. */
    public String getErrorReason() {
      return errorReason;
    }

    /** Returns a short description of the exception. */
    @Override
    public String toString() {
      return "LookupServiceShardClient HTTP error: " + errorCode + "\n" + super.toString();
    }
  }
}
