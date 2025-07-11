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

package com.google.cm.mrp.clients.cryptoclient.exceptions;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;

import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.models.JobResultCodeException;

/** Checked exception to contain AeadProvider errors */
public class AeadProviderException extends Exception implements JobResultCodeException {
  private final JobResultCode jobResultCode;

  public AeadProviderException(Throwable cause) {
    super(cause);
    this.jobResultCode = DEK_DECRYPTION_ERROR;
  }

  public AeadProviderException(String message) {
    super(message);
    this.jobResultCode = DEK_DECRYPTION_ERROR;
  }

  public AeadProviderException(String message, Throwable cause) {
    super(message, cause);
    this.jobResultCode = DEK_DECRYPTION_ERROR;
  }

  public AeadProviderException(String message, Throwable cause, JobResultCode jobResultCode) {
    super(message, cause);
    this.jobResultCode = jobResultCode;
  }

  public JobResultCode getJobResultCode() {
    return jobResultCode;
  }
}
