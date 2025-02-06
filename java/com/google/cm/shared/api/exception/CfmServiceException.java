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

package com.google.cm.shared.api.exception;

import com.google.cm.shared.api.errors.CodeProto.Code;
import com.google.common.base.Enums;

/** Thrown for errors inside of services. */
public final class CfmServiceException extends Exception {
  private final Code code;
  private final String reason;

  /** Constructs a new instance. */
  public CfmServiceException(
      Code code, ErrorReasonInterface reason, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.reason = reason.name();
  }

  /** Constructs a new instance. */
  public CfmServiceException(Code code, ErrorReasonInterface reason, String message) {
    super(message);
    this.code = code;
    this.reason = reason.name();
  }

  /** Constructs a new instance. */
  // Provides a simple default message, instead of printing the call stack
  public CfmServiceException(Code code, ErrorReasonInterface reason, Throwable cause) {
    super(cause.getClass().getSimpleName() + " was thrown.", cause);
    this.code = code;
    this.reason = reason.name();
  }

  /**
   * Returns a String converted to {@link Code} by name, or returns {@link Code#UNKNOWN} if not
   * possible.
   */
  public static Code toProtoCode(String code) {
    return Enums.getIfPresent(Code.class, code).or(Code.UNKNOWN);
  }

  /** Gets the error reason. */
  public String getErrorReason() {
    return reason;
  }

  /** Gets the error code. */
  public Code getErrorCode() {
    return code;
  }
}
