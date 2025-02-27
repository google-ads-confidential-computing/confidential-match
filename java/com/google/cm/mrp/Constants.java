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

/** Reusable constants for MRP */
public interface Constants {

  /** Define custom log levels. Must match log4j2 properties file. */
  enum CustomLogLevel {
    /** Custom log level name between INFO and DEBUG. */
    DETAIL(450);

    public final int value;

    CustomLogLevel(int value) {
      this.value = value;
    }
  }

  /** Log prefix to use for MRP log-based alerting. */
  String INTERNAL_ERROR_ALERT_LOG = "INTERNAL_ERROR";
}
