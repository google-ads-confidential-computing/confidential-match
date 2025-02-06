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

package com.google.cm.mrp.clients.orchestratorclient;

import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse;

/**
 * Interface for orchestrator clients that get the current sharding scheme from the orchestrator
 * service.
 */
public interface OrchestratorClient {

  /** Gets the sharding scheme for the given cluster group from the orchestrator. */
  GetCurrentShardingSchemeResponse getCurrentShardingScheme(String clusterGroupId)
      throws OrchestratorClientException;

  /** Wrapper for exceptions thrown by an {@link OrchestratorClient}. */
  class OrchestratorClientException extends Exception {
    private final String errorCode;

    /** Constructs a new instance. */
    public OrchestratorClientException(String errorCode, String message) {
      super(message);
      this.errorCode = errorCode;
    }

    /** Constructs a new instance. */
    public OrchestratorClientException(String errorCode, Throwable cause) {
      super(cause);
      this.errorCode = errorCode;
    }

    /** Constructs a new instance. */
    public OrchestratorClientException(String errorCode, String message, Throwable cause) {
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
      return "OrchestratorClient HTTP error: " + errorCode + "\n" + super.toString();
    }
  }
}
