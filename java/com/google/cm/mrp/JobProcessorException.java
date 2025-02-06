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

import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;

public class JobProcessorException extends RuntimeException {

  private JobResultCode errorCode;

  public JobProcessorException(Throwable cause) {
    super(cause);
    errorCode = JobResultCode.JOB_RESULT_CODE_UNKNOWN;
  }

  public JobProcessorException(String message) {
    super(message);
    errorCode = JobResultCode.JOB_RESULT_CODE_UNKNOWN;
  }

  public JobProcessorException(String message, Throwable cause) {
    this(message, cause, JobResultCode.JOB_RESULT_CODE_UNKNOWN);
  }

  public JobProcessorException(String message, JobResultCode errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public JobProcessorException(String message, Throwable cause, JobResultCode errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public boolean isRetriable() {
    return errorCode.getNumber() < 100;
  }

  public JobResultCode getErrorCode() {
    return errorCode;
  }
}
