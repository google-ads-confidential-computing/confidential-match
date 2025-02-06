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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.JobProcessorException;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;
import com.google.scp.operator.cpio.jobclient.testing.FakeJobGenerator;
import com.google.scp.operator.cpio.jobclient.testing.FakeJobResultGenerator;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class NoOpJobProcessorTest {

  private NoOpJobProcessor jobProcessor;

  @Before
  public void setUp() {
    jobProcessor = new NoOpJobProcessor();
  }

  @Test
  public void getMostRecentProcessedJob_noJobEverProcessed_returnsEmpty() {
    Optional<Job> lastProcessed = jobProcessor.getMostRecentProcessedJob();

    assertThat(lastProcessed).isEmpty();
  }

  @Test
  public void process_returnsJobResultAndStoresMostRecentProcessedJob()
      throws JobProcessorException {
    Job item = FakeJobGenerator.generate("foo");
    JobResult jobResult = FakeJobResultGenerator.fromJob(item);
    jobProcessor.setJobResultToReturn(jobResult);

    JobResult jobResultReturned = jobProcessor.process(item);

    assertThat(jobProcessor.getMostRecentProcessedJob()).isPresent();
    assertThat(jobProcessor.getMostRecentProcessedJob()).hasValue(item);
    assertThat(jobResultReturned).isEqualTo(jobResult);
  }

  @Test
  public void process_whenProcessorIsSetToThrowException_throwsException() {
    Job item = FakeJobGenerator.generate("foo");
    jobProcessor.setShouldThrowException(true);

    assertThrows(JobProcessorException.class, () -> jobProcessor.process(item));
  }
}
