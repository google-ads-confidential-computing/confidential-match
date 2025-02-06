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

import com.google.auto.value.AutoValue;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;

/** Interface for consuming received jobs. */
public interface JobProcessor {

  /** Processes a {@link Job}, then returns a {@link JobResult} with details on the outcome. */
  JobResult process(Job job) throws JobProcessorException;

  /** Simple data object to hold match statistics. */
  @AutoValue
  abstract class MatchJobResult {
    /** Creates a new instance. */
    public static MatchJobResult create(JobResultCode jobCode, MatchStatistics stats) {
      return new AutoValue_JobProcessor_MatchJobResult(jobCode, stats);
    }

    public abstract JobResultCode jobCode();

    public abstract MatchStatistics stats();
  }
}
