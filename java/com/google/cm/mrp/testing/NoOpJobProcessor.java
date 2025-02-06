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

package com.google.cm.mrp.testing;

import com.google.cm.mrp.JobProcessor;
import com.google.cm.mrp.JobProcessorException;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;
import java.util.Optional;

/**
 * Fake processor that does nothing. Can get the last job processed. Processing can be configured to
 * return a provided result or throw an exception.
 */
public final class NoOpJobProcessor implements JobProcessor {

  private Optional<Job> mostRecentProcessedJob = Optional.empty();
  private JobResult jobResultToReturn = null;
  private boolean shouldThrowException = false;

  @Override
  public JobResult process(Job job) throws JobProcessorException {
    if (shouldThrowException) {
      IllegalStateException ex = new IllegalStateException("NoOpJobProcessor was set to throw.");
      throw new JobProcessorException(ex);
    }
    mostRecentProcessedJob = Optional.of(job);
    return jobResultToReturn;
  }

  public Optional<Job> getMostRecentProcessedJob() {
    return mostRecentProcessedJob;
  }

  public void setJobResultToReturn(JobResult jobResultToReturn) {
    this.jobResultToReturn = jobResultToReturn;
  }

  public void setShouldThrowException(boolean shouldThrowException) {
    this.shouldThrowException = shouldThrowException;
  }
}
