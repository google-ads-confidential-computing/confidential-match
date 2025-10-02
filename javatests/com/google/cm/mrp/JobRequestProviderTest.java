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
import static org.mockito.Mockito.when;

import com.google.cm.mrp.api.JobResultCodeProto.JobResultCode;
import com.google.common.collect.ImmutableMap;
import com.google.scp.operator.cpio.jobclient.model.GetJobRequest;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.protos.shared.backend.JobKeyProto.JobKey;
import com.google.scp.operator.protos.shared.backend.JobStatusProto.JobStatus;
import com.google.scp.operator.protos.shared.backend.RequestInfoProto.RequestInfo;
import java.time.Duration;
import java.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class JobRequestProviderTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String JOB_ID = "testJob";
  private static final String TEST_APPLICATION_ID = "mic";
  private static final String DEFAULT_GROUP = "default";
  private static final ImmutableMap<String, String> APPLICATION_ID_WORKGROUPS =
      ImmutableMap.of(TEST_APPLICATION_ID, "fake_group");
  private static final ImmutableMap<String, String> NOTIFICATION_TOPIC_IDS =
      ImmutableMap.of(TEST_APPLICATION_ID, "fake_mic_topic");

  @Mock private StartupConfigProvider mockStartupConfigProvider;
  @Mock private FeatureFlagProvider featureFlagProvider;

  private JobRequestProvider jobRequestProvider;

  @Before
  public void setUp() {
    when(featureFlagProvider.getFeatureFlags())
        .thenReturn(FeatureFlags.builder().setWorkgroupsEnabled(true).build());
    jobRequestProvider = new JobRequestProvider(mockStartupConfigProvider, featureFlagProvider);
  }

  @Test
  public void getJobRequest_micJob_notificationTopicIdFuncReturnsExpectedTopic() {
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(buildStartupConfig());

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
    assertThat(getTopicIdFunc.get().apply(createFakeJob(TEST_APPLICATION_ID)))
        .isEqualTo("fake_mic_topic");
  }

  @Test
  public void getJobRequest_emptyApplicationId_notificationTopicIdFuncReturnsEmptyTopic() {
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(buildStartupConfig());
    String applicationId = ""; // blank

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
    assertThat(getTopicIdFunc.get().apply(createFakeJob(applicationId))).isEmpty();
  }

  @Test
  public void getJobRequest_customerMatchJob_notificationTopicIdFuncReturnsEmptyTopic() {
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(buildStartupConfig());
    String applicationId = "customer_match";

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
    assertThat(getTopicIdFunc.get().apply(createFakeJob(applicationId))).isEmpty();
  }

  @Test
  public void getJobRequest_micJob_workgroupFuncReturnsExpectedName() {
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(buildStartupConfig());

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse = workgroupFunc.get().apply(createFakeJob(TEST_APPLICATION_ID));
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo("fake_group");
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  @Test
  public void getJobRequest_emptyApplicationId_emptyApplicationIdReturnsError() {
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(buildStartupConfig());
    String applicationId = ""; // blank

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse = workgroupFunc.get().apply(createFakeJob(applicationId));
    assertThat(workgroupFunResponse.workgroupId()).isEmpty();
    assertThat(workgroupFunResponse.resultInfo()).isPresent();
    assertThat(workgroupFunResponse.resultInfo().get().getReturnCode())
        .isEqualTo(JobResultCode.INVALID_PARAMETERS.name());
  }

  @Test
  public void getJobRequest_customerMatchJob_emptyApplicationIdReturnsEmpty() {
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(buildStartupConfig());
    String applicationId = "customer_match";

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse = workgroupFunc.get().apply(createFakeJob(applicationId));
    assertThat(workgroupFunResponse.workgroupId()).isPresent();
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo(DEFAULT_GROUP);
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  private static Job createFakeJob(String applicationId) {
    return Job.builder()
        .setJobKey(JobKey.newBuilder().setJobRequestId(JOB_ID).build())
        .setRequestInfo(
            RequestInfo.newBuilder()
                .setJobRequestId(JOB_ID)
                .putAllJobParameters(ImmutableMap.of("application_id", applicationId))
                .build())
        .setJobStatus(JobStatus.RECEIVED)
        .setJobProcessingTimeout(Duration.ofHours(1))
        .setCreateTime(Instant.now())
        .setUpdateTime(Instant.now())
        .setNumAttempts(0)
        .build();
  }

  private static StartupConfig buildStartupConfig() {
    return StartupConfig.builder()
        .setNotificationTopics(NOTIFICATION_TOPIC_IDS)
        .setApplicationIdWorkgroups(APPLICATION_ID_WORKGROUPS)
        .build();
  }
}
