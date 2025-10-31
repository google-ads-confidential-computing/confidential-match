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

import static com.google.cm.mrp.api.JobResultCodeProto.JobResultCode.INTERNAL_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.BLOBSTORE_PERMISSIONS_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.util.ProtoUtils.getJsonFromProto;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
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
  private static final String LARGE_JOB_WORKGROUP = "largeJobs";
  private static final ImmutableMap<String, String> APPLICATION_ID_WORKGROUPS =
      ImmutableMap.of(TEST_APPLICATION_ID, "fake_group");
  private static final ImmutableMap<String, String> NOTIFICATION_TOPIC_IDS =
      ImmutableMap.of(TEST_APPLICATION_ID, "fake_mic_topic");

  @Mock private StartupConfigProvider startupConfigProvider;
  @Mock private FeatureFlagProvider featureFlagProvider;
  @Mock private DataSourceSizeProvider dataSourceSizeProvider;

  private JobRequestProvider jobRequestProvider;

  @Before
  public void setUp() {
    when(startupConfigProvider.getStartupConfig())
        .thenReturn(
            StartupConfig.builder()
                .setLargeJobWorkgroupName(LARGE_JOB_WORKGROUP)
                .setNotificationTopics(NOTIFICATION_TOPIC_IDS)
                .build());
    jobRequestProvider =
        new JobRequestProvider(dataSourceSizeProvider, startupConfigProvider, featureFlagProvider);
  }

  @Test
  public void getJobRequest_micJob_notificationTopicIdFuncReturnsExpectedTopic() {
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlags());

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
    assertThat(getTopicIdFunc.get().apply(createFakeJob(TEST_APPLICATION_ID)))
        .isEqualTo("fake_mic_topic");
  }

  @Test
  public void getJobRequest_emptyApplicationId_notificationTopicIdFuncReturnsEmptyTopic() {
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlags());
    String applicationId = ""; // blank

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
    assertThat(getTopicIdFunc.get().apply(createFakeJob(applicationId))).isEmpty();
  }

  @Test
  public void getJobRequest_customerMatchJob_notificationTopicIdFuncReturnsEmptyTopic() {
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlags());
    String applicationId = "customer_match";

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var getTopicIdFunc = getJobRequest.getJobCompletionNotificationTopicIdFunc();
    assertThat(getTopicIdFunc.get().apply(createFakeJob(applicationId))).isEmpty();
  }

  @Test
  public void getJobRequest_micJob_workgroupFuncReturnsExpectedName() {
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlags());

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse = workgroupFunc.get().apply(createFakeJob(TEST_APPLICATION_ID));
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo("fake_group");
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  @Test
  public void getJobRequest_emptyApplicationId_emptyApplicationIdReturnsError() {
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlags());
    String applicationId = ""; // blank

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse = workgroupFunc.get().apply(createFakeJob(applicationId));
    assertThat(workgroupFunResponse.workgroupId()).isEmpty();
    assertThat(workgroupFunResponse.resultInfo()).isPresent();
    assertThat(workgroupFunResponse.resultInfo().get().getReturnCode())
        .isEqualTo(INVALID_PARAMETERS.name());
  }

  @Test
  public void getJobRequest_customerMatchJob_emptyApplicationIdReturnsEmpty() {
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlags());
    String applicationId = "customer_match";

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getJobCompletionNotificationTopicIdFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse = workgroupFunc.get().apply(createFakeJob(applicationId));
    assertThat(workgroupFunResponse.workgroupId()).isPresent();
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo(DEFAULT_GROUP);
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  @Test
  public void getJobRequest_applicationIdInLargeJobIds_assignedToWorkgroup() throws Exception {
    long threshold = 1000L;
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlagsForLargeJob(threshold));
    var dataOwners =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataLocation.newBuilder()
                            .setInputDataBucketName("test")
                            .setInputDataBlobPrefix("testPrefix")
                            .setIsStreamed(true)))
            .build();
    when(dataSourceSizeProvider.isAtLeastSize(eq(threshold), eq(dataOwners), any()))
        .thenReturn(true);

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse =
        workgroupFunc.get().apply(createFakeJob(TEST_APPLICATION_ID, getJsonFromProto(dataOwners)));
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo(LARGE_JOB_WORKGROUP);
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  @Test
  public void getJobRequest_applicationIdInLargeJobIds_notLeastSizeFallsBackToApplicationWorkgroup()
      throws Exception {
    long threshold = 1000L;
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlagsForLargeJob(threshold));
    var dataOwners =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataLocation.newBuilder()
                            .setInputDataBucketName("test")
                            .setInputDataBlobPrefix("testPrefix")
                            .setIsStreamed(true)))
            .build();
    when(dataSourceSizeProvider.isAtLeastSize(eq(threshold), eq(dataOwners), any()))
        .thenReturn(false);

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse =
        workgroupFunc.get().apply(createFakeJob(TEST_APPLICATION_ID, getJsonFromProto(dataOwners)));
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo("fake_group");
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  @Test
  public void
      getJobRequest_applicationIdInLargeJobIds_noDedicatedLargeJobWorkgroupFallBackToApplicationId()
          throws Exception {
    long threshold = 1000L;
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlagsForLargeJob(threshold));
    when(startupConfigProvider.getStartupConfig())
        .thenReturn(
            StartupConfig.builder()
                .setLargeJobWorkgroupName("default")
                .setNotificationTopics(NOTIFICATION_TOPIC_IDS)
                .build());
    var dataOwners = DataOwnerList.getDefaultInstance();

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse =
        workgroupFunc.get().apply(createFakeJob(TEST_APPLICATION_ID, getJsonFromProto(dataOwners)));
    assertThat(workgroupFunResponse.workgroupId().get()).isEqualTo("fake_group");
    assertThat(workgroupFunResponse.resultInfo()).isEmpty();
  }

  @Test
  public void getJobRequest_applicationIdInLargeJobIds_invalidDataOwners() {
    long threshold = 1000L;
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlagsForLargeJob(threshold));

    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse =
        workgroupFunc.get().apply(createFakeJob(TEST_APPLICATION_ID, "invalid"));
    assertThat(workgroupFunResponse.workgroupId()).isEmpty();
    assertThat(workgroupFunResponse.resultInfo()).isPresent();
    assertThat(workgroupFunResponse.resultInfo().get().getReturnCode())
        .isEqualTo(INVALID_PARAMETERS.name());
  }

  @Test
  public void getJobRequest_applicationIdInLargeJobIds_handlesJobProcessorException()
      throws Exception {
    long threshold = 1000L;
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlagsForLargeJob(threshold));
    when(dataSourceSizeProvider.isAtLeastSize(eq(threshold), any(), any()))
        .thenThrow(new JobProcessorException("error", BLOBSTORE_PERMISSIONS_ERROR));
    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse =
        workgroupFunc
            .get()
            .apply(
                createFakeJob(
                    TEST_APPLICATION_ID, getJsonFromProto(DataOwnerList.getDefaultInstance())));
    assertThat(workgroupFunResponse.workgroupId()).isEmpty();
    assertThat(workgroupFunResponse.resultInfo()).isPresent();
    assertThat(workgroupFunResponse.resultInfo().get().getReturnCode())
        .isEqualTo(BLOBSTORE_PERMISSIONS_ERROR.name());
  }

  @Test
  public void getJobRequest_applicationIdInLargeJobIds_handlesRuntimeException() throws Exception {
    long threshold = 1000L;
    when(featureFlagProvider.getFeatureFlags()).thenReturn(buildFeatureFlagsForLargeJob(threshold));
    when(dataSourceSizeProvider.isAtLeastSize(eq(threshold), any(), any()))
        .thenThrow(new NullPointerException());
    GetJobRequest getJobRequest = jobRequestProvider.getJobRequest();

    assertThat(getJobRequest.getWorkgroupAllocationFunc()).isPresent();
    var workgroupFunc = getJobRequest.getWorkgroupAllocationFunc();
    var workgroupFunResponse =
        workgroupFunc
            .get()
            .apply(
                createFakeJob(
                    TEST_APPLICATION_ID, getJsonFromProto(DataOwnerList.getDefaultInstance())));
    assertThat(workgroupFunResponse.workgroupId()).isEmpty();
    assertThat(workgroupFunResponse.resultInfo()).isPresent();
    assertThat(workgroupFunResponse.resultInfo().get().getReturnCode())
        .isEqualTo(INTERNAL_ERROR.name());
  }

  private static Job createFakeJob(String applicationId) {
    return createFakeJob(applicationId, "");
  }

  private static Job createFakeJob(String applicationId, String dataOwnerList) {
    ImmutableMap.Builder<String, String> jobParamsMap = ImmutableMap.builder();
    jobParamsMap.put("application_id", applicationId);

    if (!dataOwnerList.isBlank()) {
      jobParamsMap.put("data_owner_list", dataOwnerList);
    }
    return Job.builder()
        .setJobKey(JobKey.newBuilder().setJobRequestId(JOB_ID).build())
        .setRequestInfo(
            RequestInfo.newBuilder()
                .setJobRequestId(JOB_ID)
                .putAllJobParameters(jobParamsMap.build())
                .build())
        .setJobStatus(JobStatus.RECEIVED)
        .setJobProcessingTimeout(Duration.ofHours(1))
        .setCreateTime(Instant.now())
        .setUpdateTime(Instant.now())
        .setNumAttempts(0)
        .build();
  }

  private static FeatureFlags buildFeatureFlags() {
    return FeatureFlags.builder()
        .setWorkgroupsEnabled(true)
        .setApplicationIdWorkgroups(APPLICATION_ID_WORKGROUPS)
        .build();
  }

  private static FeatureFlags buildFeatureFlagsForLargeJob(long threshold) {
    return FeatureFlags.builder()
        .setWorkgroupsEnabled(true)
        .setApplicationIdWorkgroups(APPLICATION_ID_WORKGROUPS)
        .setLargeJobApplicationIds(APPLICATION_ID_WORKGROUPS.keySet())
        .setLargeJobThresholdBytes(threshold)
        .build();
  }
}
