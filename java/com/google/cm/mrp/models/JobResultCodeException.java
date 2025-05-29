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

package com.google.cm.mrp.models;

import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;

/**
 * Defines exceptions that contain JobResultCodes. Useful to define how certain exceptions can
 * affect how a job failure is handled. TODO(b/419649443): add to all respective exceptions in MRP
 */
public interface JobResultCodeException {

  /** Gets the JobResultCode (Error code) for a job component exception. */
  JobResultCode getJobResultCode();
}
