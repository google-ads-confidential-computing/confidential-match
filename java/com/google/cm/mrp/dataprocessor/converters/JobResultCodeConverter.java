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

package com.google.cm.mrp.dataprocessor.converters;

import static com.google.cm.mrp.api.JobResultCodeProto.JobResultCode.RETRY_IN_PROGRESS;

import com.google.cm.mrp.api.JobResultCodeProto.JobResultCode;

/** Converts JobResultCodes between API and Backend versions */
public final class JobResultCodeConverter {

  /** Converts a backend JobResultCode to its api version */
  public static JobResultCode convert(
      com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode resultCode) {
    if (resultCode.getNumber() > 1 && resultCode.getNumber() < 100) {
      return RETRY_IN_PROGRESS;
    } else {
      return JobResultCode.valueOf(resultCode.name());
    }
  }
}
