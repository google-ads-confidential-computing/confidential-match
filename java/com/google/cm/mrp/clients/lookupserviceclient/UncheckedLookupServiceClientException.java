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

/** Exception class for Lookup Service Client Runtime Exceptions. */
public class UncheckedLookupServiceClientException extends RuntimeException {

  /** Creates a new instance of UncheckedLookupServiceClientException with message and cause. */
  public UncheckedLookupServiceClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
