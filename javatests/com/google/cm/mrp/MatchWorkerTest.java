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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.google.common.util.concurrent.Service.State.TERMINATED;

import com.google.cm.mrp.Annotations.JobQueueRetryDelaySec;
import com.google.cm.mrp.selectors.MetricClientSelector;
import com.google.cm.mrp.testing.NoOpJobProcessor;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.scp.operator.cpio.jobclient.JobClient;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;
import com.google.scp.operator.cpio.jobclient.testing.ConstantJobClient;
import com.google.scp.operator.cpio.jobclient.testing.FakeJobGenerator;
import com.google.scp.operator.cpio.jobclient.testing.FakeJobResultGenerator;
import com.google.scp.shared.clients.configclient.ParameterClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class MatchWorkerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private ConstantJobClient jobClient;
  private NoOpJobProcessor jobProcessor;
  private ServiceManager serviceManager;
  @Mock private ParameterClient mockParameterClient;

  @Before
  public void setUp() {
    jobClient = new ConstantJobClient();
    jobProcessor = new NoOpJobProcessor();
    serviceManager =
        MatchWorker.create(
                new AbstractModule() {
                  @Override
                  protected void configure() {
                    bind(ParameterClient.class).toInstance(mockParameterClient);
                    install(new WorkerModule(mockParameterClient));
                    install(MetricClientSelector.LOCAL.getMetricClientModule());
                    bind(JobClient.class).toInstance(jobClient);
                    bind(JobProcessor.class).toInstance(jobProcessor);
                    bind(Integer.class).annotatedWith(JobQueueRetryDelaySec.class).toInstance(10);
                  }
                })
            .getServiceManager();
  }

  @Test
  public void startAsync_jobIsProcessed() {
    Job job = FakeJobGenerator.generate("foo");
    JobResult jobResult = FakeJobResultGenerator.fromJob(job);
    jobClient.setReturnConstant(job);
    jobProcessor.setJobResultToReturn(jobResult);

    runWorker();

    assertThat(jobProcessor.getMostRecentProcessedJob()).hasValue(job);
    assertThat(jobClient.getLastJobResultCompleted()).isEqualTo(jobResult);
    assertThat(serviceManager.servicesByState().keySet()).containsExactly(TERMINATED);
  }

  @Test
  public void startAsync_jobClientGetJobException_jobIsNotProcessed() {
    jobClient.setReturnEmpty();
    jobClient.setShouldThrowOnGetJob(true);

    runWorker();

    assertThat(jobProcessor.getMostRecentProcessedJob()).isEmpty();
    assertThat(jobClient.getLastJobResultCompleted()).isNull();
    assertThat(serviceManager.servicesByState().keySet()).containsExactly(TERMINATED);
  }

  @Test
  public void startAsync_jobClientMarkJobCompletedException_jobIsProcessed() {
    Job job = FakeJobGenerator.generate("foo");
    jobClient.setReturnConstant(job);
    jobClient.setShouldThrowOnMarkJobCompleted(true);

    runWorker();

    assertThat(jobProcessor.getMostRecentProcessedJob()).hasValue(job);
    assertThat(serviceManager.servicesByState().keySet()).containsExactly(TERMINATED);
  }

  @Test
  public void startAsync_jobProcessorException_noJobResultReturned() {
    Job job = FakeJobGenerator.generate("foo");
    jobClient.setReturnConstant(job);
    jobProcessor.setShouldThrowException(true);

    runWorker();

    assertThat(jobClient.getLastJobResultCompleted()).isNull();
    assertThat(serviceManager.servicesByState().keySet()).containsExactly(TERMINATED);
  }

  private void runWorker() {
    serviceManager.startAsync().awaitStopped();
  }
}
