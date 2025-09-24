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

import static com.google.cm.mrp.Parameter.CONSCRYPT_ENABLED;
import static com.google.cm.mrp.Parameter.NOTIFICATION_TOPIC_PREFIX;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.Annotations.JobQueueRetryDelaySec;
import com.google.cm.mrp.selectors.MetricClientSelector;
import com.google.cm.mrp.testing.NoOpJobProcessor;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.scp.operator.cpio.jobclient.JobClient;
import com.google.scp.operator.cpio.jobclient.JobClient.JobClientException;
import com.google.scp.operator.cpio.jobclient.model.GetJobRequest;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.protos.shared.backend.JobKeyProto.JobKey;
import com.google.scp.operator.protos.shared.backend.JobStatusProto.JobStatus;
import com.google.scp.operator.protos.shared.backend.RequestInfoProto.RequestInfo;
import com.google.scp.shared.clients.configclient.ParameterClient;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class WorkerPullWorkServiceTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final Instant REQUEST_RECEIVED_AT = Instant.parse("2019-10-01T08:25:24.00Z");
  private static final Instant REQUEST_PROCESSING_STARTED_AT =
      Instant.parse("2019-10-01T08:29:24.00Z");
  private static final Instant REQUEST_UPDATED_AT = Instant.parse("2019-10-01T08:29:24.00Z");
  private static final String DATA_HANDLE = "dataHandle";
  private static final String DATA_HANDLE_BUCKET = "bucket";
  private static final String POSTBACK_URL = "http://postback.com";
  private static final String ATTRIBUTION_REPORT_TO = "foo.com";
  private static final Integer DEBUG_PRIVACY_BUDGET_LIMIT = 5;
  private static final String JOB_PARAM_ATTRIBUTION_REPORT_TO = "attribution_report_to";
  private static final String APPLICATION_ID = "application_id";
  private static final String JOB_PARAM_DEBUG_PRIVACY_BUDGET_LIMIT = "debug_privacy_budget_limit";

  private NoOpJobProcessor jobProcessor;
  private ServiceManager serviceManager;
  private ParameterClient parameterClient;
  @Mock private FeatureFlagProvider mockFeatureFlagProvider;
  @Mock private JobClient mockJobClient;

  @Captor ArgumentCaptor<GetJobRequest> getJobRequestArgumentCaptor;

  @Before
  public void setUp() {
    parameterClient = createParameterClient();
    jobProcessor = new NoOpJobProcessor();
    serviceManager =
        MatchWorker.create(
                new AbstractModule() {
                  @Override
                  protected void configure() {
                    bind(ParameterClient.class).toInstance(parameterClient);
                    install(new WorkerModule(parameterClient));
                    install(MetricClientSelector.LOCAL.getMetricClientModule());
                    bind(JobClient.class).toInstance(mockJobClient);
                    bind(JobProcessor.class).toInstance(jobProcessor);
                    bind(Integer.class).annotatedWith(JobQueueRetryDelaySec.class).toInstance(10);
                    bind(FeatureFlagProvider.class).toInstance(mockFeatureFlagProvider);
                  }
                })
            .getServiceManager();
  }

  @Test
  public void startAsync_micJob_notificationTopicIdFuncReturnsExpectedTopic()
      throws JobClientException {
    Job job = generateJob("foo", "mic");
    when(mockJobClient.getJob(any())).thenReturn(Optional.of(job)).thenReturn(Optional.empty());
    when(mockFeatureFlagProvider.getFeatureFlags()).thenReturn(FeatureFlags.builder().build());

    runWorker();

    verify(mockJobClient, times(2)).getJob(getJobRequestArgumentCaptor.capture());
    for (GetJobRequest getJobRequest : getJobRequestArgumentCaptor.getAllValues()) {
      assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
      var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
      assertThat(getTopicIdFunc.get().apply(jobProcessor.getMostRecentProcessedJob().get()))
          .isEqualTo("fake_mic_topic");
    }
  }

  @Singleton
  private ParameterClient createParameterClient() {
    return new ParameterClient() {
      /** Always returns an empty optional. */
      @Override
      public Optional<String> getParameter(String param) {
        return Optional.empty();
      }

      /** Returns an empty optional except notification_topic_mic flag */
      @Override
      public Optional<String> getParameter(
          String param,
          Optional<String> paramPrefix,
          boolean includeEnvironmentParam,
          boolean getLatest) {
        if (param.equalsIgnoreCase(NOTIFICATION_TOPIC_PREFIX + "MIC")) {
          return Optional.of("fake_mic_topic");
        }
        if (param.equalsIgnoreCase(CONSCRYPT_ENABLED.name())) {
          return Optional.of("true");
        }
        return Optional.empty();
      }

      /** Always returns an empty optional. */
      @Override
      public Optional<String> getLatestParameter(String param) {
        return Optional.empty();
      }

      /** Always returns an optional of "LOCAL_ARGS". */
      @Override
      public Optional<String> getEnvironmentName() {
        return Optional.of("LOCAL_ARGS");
      }
    };
  }

  private void runWorker() {
    serviceManager.startAsync().awaitStopped();
  }

  // TODO: Change the generateJob methods in SCP repo to set application_id in job parameters
  private static Job generateJob(String id, String applicationId) {
    return generateBuilder(id, applicationId).build();
  }

  private static Job.Builder generateBuilder(String id, String applicationId) {
    return Job.builder()
        .setJobKey(JobKey.newBuilder().setJobRequestId(id).build())
        .setJobProcessingTimeout(Duration.ofSeconds(3600))
        .setRequestInfo(createFakeRequestInfo(id, applicationId))
        .setCreateTime(REQUEST_RECEIVED_AT)
        .setUpdateTime(REQUEST_UPDATED_AT)
        .setProcessingStartTime(Optional.of(REQUEST_PROCESSING_STARTED_AT))
        .setJobStatus(JobStatus.IN_PROGRESS)
        .setNumAttempts(0);
  }

  private static RequestInfo createFakeRequestInfo(String requestId, String applicationId) {
    RequestInfo requestInfo =
        RequestInfo.newBuilder()
            .setJobRequestId(requestId)
            .setInputDataBlobPrefix(DATA_HANDLE)
            .setInputDataBucketName(DATA_HANDLE_BUCKET)
            .setOutputDataBlobPrefix(DATA_HANDLE)
            .setOutputDataBucketName(DATA_HANDLE_BUCKET)
            .setPostbackUrl(POSTBACK_URL)
            .putAllJobParameters(
                ImmutableMap.of(
                    APPLICATION_ID,
                    applicationId,
                    JOB_PARAM_ATTRIBUTION_REPORT_TO,
                    ATTRIBUTION_REPORT_TO,
                    JOB_PARAM_DEBUG_PRIVACY_BUDGET_LIMIT,
                    DEBUG_PRIVACY_BUDGET_LIMIT.toString()))
            .build();
    return requestInfo;
  }
}
