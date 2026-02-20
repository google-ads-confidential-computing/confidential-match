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

import static com.google.cm.mrp.MatchJobProcessor.RESULT_FAILURE_MESSAGE;
import static com.google.cm.mrp.MatchJobProcessor.RESULT_PARTIAL_SUCCESS_MESSAGE;
import static com.google.cm.mrp.MatchJobProcessor.RESULT_SUCCESS_MESSAGE;
import static com.google.cm.mrp.MatchJobProcessor.TOTAL_ERRORS_PREFIX;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.FAILED_WITH_ROW_ERRORS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_LIST_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_DATA_LOCATION_CONFIGURATION;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_SCHEMA_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static com.google.cm.mrp.backend.SchemaProto.Schema.DataFormat.CSV;
import static com.google.cm.util.ProtoUtils.getProtoFromJson;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.ApplicationProto.ApplicationId;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.dataprocessor.DataProcessor;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.models.JobParameters;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import com.google.scp.operator.cpio.metricclient.model.CustomMetric;
import com.google.scp.operator.protos.frontend.api.v1.CreateJobRequestProto.CreateJobRequest;
import com.google.scp.operator.protos.shared.backend.ErrorCountProto.ErrorCount;
import com.google.scp.operator.protos.shared.backend.ErrorSummaryProto.ErrorSummary;
import com.google.scp.operator.protos.shared.backend.JobKeyProto.JobKey;
import com.google.scp.operator.protos.shared.backend.JobStatusProto.JobStatus;
import com.google.scp.operator.protos.shared.backend.RequestInfoProto.RequestInfo;
import com.google.scp.operator.protos.shared.backend.ResultInfoProto.ResultInfo;
import com.google.scp.shared.proto.ProtoUtil;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
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
public final class MatchJobProcessorTest {

  private static final Instant FIXED_TIME = Instant.parse("2023-12-01T00:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_TIME, ZoneId.systemDefault());
  private static final JobKey FAKE_JOB_KEY = JobKey.newBuilder().setJobRequestId("job_id").build();
  private static final Instant REQUEST_RECEIVED_AT = Instant.parse("2019-10-01T08:25:24.00Z");
  private static final Optional<Instant> REQUEST_PROCESSING_STARTED_AT =
      Optional.of(Instant.parse("2019-10-01T08:29:24.00Z"));
  private static final Instant REQUEST_UPDATED_AT = Instant.parse("2019-10-01T08:29:24.00Z");
  private static final int NUM_RETRIES = 3;
  private static final Duration PROCESSING_TIMEOUT = Duration.ofSeconds(3600);
  private static final Map<String, String> DEFAULT_STATS_MAP =
      Map.of(
          "jobdurationseconds",
          "0",
          "jobattempts",
          "1",
          "matchpercentage",
          "0",
          "numdatarecords",
          "0",
          "numdatarecordswithmatch",
          "0",
          "datarecordswithmatchpercentage",
          "0",
          "numfiles",
          "0",
          "nummatches",
          "0",
          "numpii",
          "0",
          "fileformat",
          CSV.name());

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataProcessor mockDataProcessor;
  @Mock private FeatureFlagProvider mockFeatureFlagProvider;
  @Mock private StartupConfigProvider mockStartupConfigProvider;
  @Mock private MetricClient mockMetricClient;
  @Captor private ArgumentCaptor<JobParameters> jobParametersCaptor;
  @Captor private ArgumentCaptor<CustomMetric> metricCaptor;
  private MatchJobProcessor processor;

  @Before
  public void setUp() {
    processor =
        new MatchJobProcessor(
            FIXED_CLOCK,
            mockDataProcessor,
            NUM_RETRIES,
            mockMetricClient,
            mockFeatureFlagProvider,
            mockStartupConfigProvider);

    when(mockFeatureFlagProvider.getFeatureFlags())
        .thenReturn(FeatureFlags.builder().setEnableMIC(false).build());
  }

  @Test
  public void process_whenValidEncryptedJobParametersThenReturnsJobResult() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_encrypted.json");
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(SUCCESS.name())
                    .setReturnMessage(RESULT_SUCCESS_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();
    EncryptionMetadata expectedEncryptionMetadataProto =
        EncryptionMetadata.newBuilder()
            .setEncryptionKeyInfo(
                EncryptionKeyInfo.newBuilder()
                    .setWrappedKeyInfo(
                        WrappedKeyInfo.newBuilder()
                            .setKeyType(KeyType.XCHACHA20_POLY1305)
                            .setGcpWrappedKeyInfo(
                                GcpWrappedKeyInfo.newBuilder().setWipProvider("testWip"))))
            .build();
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata())
        .isEqualTo(Optional.of(expectedEncryptionMetadataProto));
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenValidHashedJobParametersThenReturnsJobResult() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(SUCCESS.name())
                    .setReturnMessage(RESULT_SUCCESS_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenInvalidModeThrowThenReturnsInvalid() throws Exception {
    Job job = generateFakeJob("testdata/invalid_job_params_invalid_mode.json");
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INVALID_PARAMETERS.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expectedJobResult, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenMatchStatisticsCorrectlyFormatted() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    Map<String, String> resultsMap = new HashMap<>();
    resultsMap.put("jobdurationseconds", "0");
    resultsMap.put("jobattempts", "1");
    resultsMap.put("matchpercentage", "55.55578");
    resultsMap.put("matchpercentagepercondition email", "55.55555");
    resultsMap.put("matchpercentagepercondition phone", "100");
    resultsMap.put("numdatarecords", "20000000");
    resultsMap.put("numdatarecordswithmatch", "11111111");
    resultsMap.put("datarecordswithmatchpercentage", "55.55555");
    resultsMap.put("numdatasource2matchespercondition email", "11111111");
    resultsMap.put("numdatasource2matchespercondition phone", "200");
    resultsMap.put("numfiles", "20");
    resultsMap.put("nummatches", "11111211");
    resultsMap.put("numpii", "20000100");
    resultsMap.put("nummatchespercondition email", "11111111");
    resultsMap.put("nummatchespercondition phone", "100");
    resultsMap.put("datasource2matchpercentagepercondition email", "55.55555");
    resultsMap.put("datasource2matchpercentagepercondition phone", "100");
    resultsMap.put("fileformat", "CSV");
    ImmutableMap<String, Long> conditionMatchCounts = ImmutableMap.of("email", 11111111L,"phone", 100L);
    ImmutableMap<String, Long> validConditionMatches = ImmutableMap.of("email", 20000000L,"phone", 100L);
    ImmutableMap<String, Long> dataSource2Matches = ImmutableMap.of("email", 11111111L,"phone", 200L);
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(SUCCESS.name())
                    .setReturnMessage(RESULT_SUCCESS_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(resultsMap)
                    .build())
            .build();
    var stats =
        MatchStatistics.create(
            20,
            20000000L,
            11111111L,
            conditionMatchCounts,
            validConditionMatches,
            ImmutableMap.of(),
            dataSource2Matches,
            CSV);
    when(mockDataProcessor.process(any(), any(), any())).thenReturn(stats);

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_jobFinishesNormally_writeStatsMetrics() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(
            MatchStatistics.create(
                20,
                20000000L,
                11111111L,
                Map.of("email", 11111111L),
                Map.of("email", 20000000L),
                Map.of(),
                Map.of("email", 11111111L),
                CSV));

    processor.process(job);

    verify(mockMetricClient, times(14)).recordMetric(metricCaptor.capture());
    var statsLabels =
        ImmutableMap.ofEntries(
            Map.entry("CustomerServiceAccount", "Default"),
            Map.entry("ApplicationId", "customer_match"),
            Map.entry("JobId", "job_id"),
            Map.entry("EncryptionType", "UNENCRYPTED"),
            Map.entry("FileFormat", "CSV"));
    var jobLabels =
        ImmutableMap.<String, String>builder()
            .putAll(statsLabels)
            .put("ReturnCode", "SUCCESS")
            .put("ReturnCodeType", "SUCCESS")
            .put("Attempts", "1")
            .build();
    var conditionLabels =
        ImmutableMap.<String, String>builder()
            .putAll(statsLabels)
            .put("MatchCondition", "email")
            .build();
    assertThat(metricCaptor.getAllValues())
        .containsExactly(
            makeMetric("jobcount", "Count", 1.0, jobLabels),
            makeMetric("jobdurationseconds", "Seconds", 0.0, jobLabels),
            makeMetric("jobattempts", "Count", 1.0, jobLabels),
            makeMetric("numfiles", "Count", 20.0, statsLabels),
            makeMetric("numdatarecords", "Count", 20000000.0, statsLabels),
            makeMetric("numdatarecordswithmatch", "Count", 11111111.0, statsLabels),
            makeMetric("datarecordswithmatchpercentage", "Percent", 55.555555, statsLabels),
            makeMetric("numpii", "Count", 20000000.0, statsLabels),
            makeMetric("nummatches", "Count", 11111111.0, statsLabels),
            makeMetric("matchpercentage", "Percent", 55.555555, statsLabels),
            makeMetric("nummatchespercondition", "Count", 11111111.0, conditionLabels),
            makeMetric("numdatasource2matchespercondition", "Count", 11111111.0, conditionLabels),
            makeMetric("matchpercentagepercondition", "Percent", 55.555555, conditionLabels),
            makeMetric(
                "datasource2matchpercentagepercondition", "Percent", 55.555555, conditionLabels));
  }

  @Test
  public void process_jobHasErrorCount_writeErrorMetrics() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(
            MatchStatistics.create(
                0,
                4L,
                1L,
                Map.of(),
                Map.of(),
                Map.of(DECRYPTION_ERROR.name(), 2L, DEK_DECRYPTION_ERROR.name(), 2L),
                Map.of(),
                CSV));

    processor.process(job);

    verify(mockMetricClient, times(12)).recordMetric(metricCaptor.capture());
    var statsLabels =
        ImmutableMap.ofEntries(
            Map.entry("CustomerServiceAccount", "Default"),
            Map.entry("ApplicationId", "customer_match"),
            Map.entry("JobId", "job_id"),
            Map.entry("EncryptionType", "UNENCRYPTED"),
            Map.entry("FileFormat", "CSV"));
    var jobLabels =
        ImmutableMap.<String, String>builder()
            .putAll(statsLabels)
            .put("ReturnCode", "FAILED_WITH_ROW_ERRORS")
            .put("ReturnCodeType", "NON_RETRYABLE_ERROR")
            .put("Attempts", "1")
            .build();
    var errorLabels1 =
        ImmutableMap.<String, String>builder()
            .putAll(statsLabels)
            .put("ErrorCode", "DECRYPTION_ERROR")
            .build();
    var errorLabels2 =
        ImmutableMap.<String, String>builder()
            .putAll(statsLabels)
            .put("ErrorCode", "DEK_DECRYPTION_ERROR")
            .build();
    assertThat(metricCaptor.getAllValues())
        .containsExactly(
            makeMetric("jobcount", "Count", 1.0, jobLabels),
            makeMetric("jobdurationseconds", "Seconds", 0.0, jobLabels),
            makeMetric("jobattempts", "Count", 1.0, jobLabels),
            makeMetric("numfiles", "Count", 0.0, statsLabels),
            makeMetric("numdatarecords", "Count", 4.0, statsLabels),
            makeMetric("numdatarecordswithmatch", "Count", 1.0, statsLabels),
            makeMetric("datarecordswithmatchpercentage", "Percent", 25.0, statsLabels),
            makeMetric("numpii", "Count", 0.0, statsLabels),
            makeMetric("nummatches", "Count", 0.0, statsLabels),
            makeMetric("matchpercentage", "Percent", 0.0, statsLabels),
            makeMetric("numerrorspererrorcode", "Count", 2.0, errorLabels1),
            makeMetric("numerrorspererrorcode", "Count", 2.0, errorLabels2));
  }

  @Test
  public void process_throwsException_writeOnlyJobMetrics() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    doThrow(new JobProcessorException("unused", INPUT_FILE_LIST_READ_ERROR))
        .when(mockDataProcessor)
        .process(any(), any(), any());

    processor.process(job);

    verify(mockMetricClient, times(3)).recordMetric(metricCaptor.capture());
    var jobLabels =
        ImmutableMap.ofEntries(
            Map.entry("CustomerServiceAccount", "Default"),
            Map.entry("ApplicationId", "customer_match"),
            Map.entry("JobId", "job_id"),
            Map.entry("EncryptionType", "UNENCRYPTED"),
            Map.entry("ReturnCode", "INPUT_FILE_LIST_READ_ERROR"),
            Map.entry("ReturnCodeType", "RETRYABLE_ERROR"),
            Map.entry("Attempts", "1"),
            Map.entry("FileFormat", "CSV"));
    assertThat(metricCaptor.getAllValues())
        .containsExactly(
            makeMetric("jobcount", "Count", 1.0, jobLabels),
            makeMetric("jobdurationseconds", "Seconds", 20.0, jobLabels),
            makeMetric("jobattempts", "Count", 1.0, jobLabels));
  }

  @Test
  public void process_whenJobWithErrorCountsReturnsThenReturnPartialSuccess() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    ImmutableMap<String, Long> errorsMap =
        ImmutableMap.of(DECRYPTION_ERROR.name(), 2L, DEK_DECRYPTION_ERROR.name(), 2L);
    Map<String, String> resultsMap =
        Map.of(
            "jobdurationseconds",
            "0",
            "jobattempts",
            "1",
            "matchpercentage",
            "0",
            "numdatarecords",
            "20",
            "numdatarecordswithmatch",
            "1",
            "datarecordswithmatchpercentage",
            "5",
            "numfiles",
            "0",
            "nummatches",
            "0",
            "numpii",
            "0",
            "fileformat",
            CSV.name());
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(PARTIAL_SUCCESS.name())
                    .setReturnMessage(RESULT_PARTIAL_SUCCESS_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .setErrorSummary(
                        ErrorSummary.newBuilder()
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(DECRYPTION_ERROR.name())
                                    .setCount(2))
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(DEK_DECRYPTION_ERROR.name())
                                    .setCount(2L))
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(TOTAL_ERRORS_PREFIX)
                                    .setCount(4L)))
                    .putAllResultMetadata(resultsMap)
                    .build())
            .build();
    var stats =
        MatchStatistics.create(
            0, 20L, 1L, ImmutableMap.of(), ImmutableMap.of(), errorsMap, ImmutableMap.of(), CSV);
    when(mockDataProcessor.process(any(), any(), any())).thenReturn(stats);

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenJobWithOneErrorCountEqualsNumRecordsReturnsErrorCountError()
      throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    ImmutableMap<String, Long> errorsMap = ImmutableMap.of(DECRYPTION_ERROR.name(), 4L);
    Map<String, String> resultsMap =
        Map.of(
            "jobdurationseconds",
            "0",
            "jobattempts",
            "1",
            "matchpercentage",
            "0",
            "numdatarecords",
            "4",
            "numdatarecordswithmatch",
            "1",
            "datarecordswithmatchpercentage",
            "25",
            "numfiles",
            "0",
            "nummatches",
            "0",
            "numpii",
            "0",
            "fileformat",
            CSV.name());
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(DECRYPTION_ERROR.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .setErrorSummary(
                        ErrorSummary.newBuilder()
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(DECRYPTION_ERROR.name())
                                    .setCount(4L))
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(TOTAL_ERRORS_PREFIX)
                                    .setCount(4L)))
                    .putAllResultMetadata(resultsMap)
                    .build())
            .build();
    var stats =
        MatchStatistics.create(
            0, 4L, 1L, ImmutableMap.of(), ImmutableMap.of(), errorsMap, ImmutableMap.of());
    when(mockDataProcessor.process(any(), any(), any())).thenReturn(stats);

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenJobWithMultipleErrorCountEqualsNumRecordsReturnsNewMultipleErrorCode()
      throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    ImmutableMap<String, Long> errorsMap =
        ImmutableMap.of(DECRYPTION_ERROR.name(), 2L, DEK_DECRYPTION_ERROR.name(), 2L);
    Map<String, String> resultsMap =
        Map.of(
            "jobdurationseconds",
            "0",
            "jobattempts",
            "1",
            "matchpercentage",
            "0",
            "numdatarecords",
            "4",
            "numdatarecordswithmatch",
            "1",
            "datarecordswithmatchpercentage",
            "25",
            "numfiles",
            "0",
            "nummatches",
            "0",
            "numpii",
            "0",
            "fileformat",
            CSV.name());
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(FAILED_WITH_ROW_ERRORS.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .setErrorSummary(
                        ErrorSummary.newBuilder()
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(DECRYPTION_ERROR.name())
                                    .setCount(2))
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(DEK_DECRYPTION_ERROR.name())
                                    .setCount(2L))
                            .addErrorCounts(
                                ErrorCount.newBuilder()
                                    .setCategory(TOTAL_ERRORS_PREFIX)
                                    .setCount(4L)))
                    .putAllResultMetadata(resultsMap)
                    .build())
            .build();
    var stats =
        MatchStatistics.create(
            0, 4L, 1L, ImmutableMap.of(), ImmutableMap.of(), errorsMap, ImmutableMap.of());
    when(mockDataProcessor.process(any(), any(), any())).thenReturn(stats);

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenTwoDataOwnersThenThrowsException() throws Exception {
    Job job = generateFakeJob("testdata/invalid_job_params_multiple_data_owners.json");
    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INVALID_PARAMETERS.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenLookupSourceThenThrowsException() throws Exception {
    Job job = generateFakeJob("testdata/invalid_job_params_lookup_endpoint.json");
    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INVALID_PARAMETERS.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_invalidEncryptionMetadataThrowsException() throws Exception {
    Job job = generateFakeJob("testdata/invalid_job_params_invalid_encryption_metadata.json");
    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INVALID_PARAMETERS.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_invalidMultipleWrappedKeysThrowsException() throws Exception {
    Job job = generateFakeJob("testdata/invalid_job_params_multiple_wrapped_keys.json");
    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INVALID_PARAMETERS.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenRetriableExceptionIsThrownRetryThenThrowException() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_hashed.json");
    doThrow(new JobProcessorException("test", INPUT_FILE_LIST_READ_ERROR))
        .when(mockDataProcessor)
        .process(any(), any(), any());

    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INPUT_FILE_LIST_READ_ERROR.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(Map.of("jobdurationseconds", "20", "jobattempts", "1"))
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    verify(mockDataProcessor, times(NUM_RETRIES))
        .process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenInputDataPrefixAndSchemaPathThenReturnsJobResult() throws Exception {
    Job job =
        generateFakeJob("testdata/valid_job_params_with_input_data_prefix_and_schema_path.json");
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(SUCCESS.name())
                    .setReturnMessage(RESULT_SUCCESS_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenInputDataPathsAndSchemaPathThenReturnsJobResult() throws Exception {
    Job job =
        generateFakeJob("testdata/valid_job_params_with_input_data_paths_and_schema_path.json");
    JobResult expectedJobResult =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(SUCCESS.name())
                    .setReturnMessage(RESULT_SUCCESS_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());

    JobResult jobResult = processor.process(job);

    assertThat(jobResult).isEqualTo(expectedJobResult);
    verify(mockDataProcessor).process(any(), any(), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().encryptionMetadata()).isEqualTo(Optional.empty());
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenInputDataPathsAndPrefixThenThrowsException() throws Exception {
    Job job = generateFakeJob("testdata/invalid_job_params_input_data_paths_and_prefix.json");
    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(INVALID_DATA_LOCATION_CONFIGURATION.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenInputDataPathsWithoutSchemaPathThenThrowsException() throws Exception {
    Job job =
        generateFakeJob("testdata/invalid_job_params_input_data_paths_without_schema_path.json");
    JobResult expected =
        JobResult.builder()
            .setJobKey(job.jobKey())
            .setResultInfo(
                ResultInfo.newBuilder()
                    .setReturnCode(MISSING_SCHEMA_ERROR.name())
                    .setReturnMessage(RESULT_FAILURE_MESSAGE)
                    .setFinishedAt(ProtoUtil.toProtoTimestamp(FIXED_TIME))
                    .putAllResultMetadata(DEFAULT_STATS_MAP)
                    .build())
            .build();

    JobResult jobResult = processor.process(job);

    assertEquals(expected, jobResult);
    verifyNoMoreInteractions(mockDataProcessor);
  }

  @Test
  public void process_whenApplicationIdMicAndMicNotEnabledThrowException() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_mic.json");

    JobResult result = processor.process(job);

    assertEquals(INVALID_PARAMETERS.name(), result.resultInfo().getReturnCode());
  }

  @Test
  public void process_whenApplicationIdMicAndMicEnabledSuccess() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_mic.json");
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());
    when(mockFeatureFlagProvider.getFeatureFlags())
        .thenReturn(FeatureFlags.builder().setEnableMIC(true).build());
    when(mockStartupConfigProvider.getStartupConfig()).thenReturn(StartupConfig.builder().build());

    JobResult result = processor.process(job);

    assertEquals(SUCCESS.name(), result.resultInfo().getReturnCode());
  }

  @Test
  public void process_micNotEnabled_getStartupConfigNotCalled() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_mic.json");
    lenient()
        .when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());

    processor.process(job);

    verify(mockStartupConfigProvider, never()).getStartupConfig();
  }

  @Test
  public void process_micEnabledNotificationTopicForMicNotAdded_emptyTopicId() throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_mic.json");
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());
    when(mockFeatureFlagProvider.getFeatureFlags())
        .thenReturn(FeatureFlags.builder().setEnableMIC(true).build());
    when(mockStartupConfigProvider.getStartupConfig())
        .thenReturn(
            StartupConfig.builder()
                .addNotificationTopic(
                    ApplicationId.CUSTOMER_MATCH.name().toLowerCase(), "fake_customer_match_topic")
                .build());

    JobResult actual = processor.process(job);

    verify(mockStartupConfigProvider, times(1)).getStartupConfig();
    assertEquals(Optional.empty(), actual.topicId());
  }

  @Test
  public void process_micEnabledAndNotificationTopicsContainsMicApplicationId_success()
      throws Exception {
    Job job = generateFakeJob("testdata/valid_job_params_mic.json");
    when(mockDataProcessor.process(any(), any(), any()))
        .thenReturn(MatchStatistics.emptyInstance());
    when(mockFeatureFlagProvider.getFeatureFlags())
        .thenReturn(FeatureFlags.builder().setEnableMIC(true).build());
    when(mockStartupConfigProvider.getStartupConfig())
        .thenReturn(
            StartupConfig.builder()
                .addNotificationTopic(ApplicationId.MIC.name().toLowerCase(), "fake_mic_topic")
                .addNotificationTopic(
                    ApplicationId.CUSTOMER_MATCH.name().toLowerCase(), "fake_customer_match_topic")
                .build());

    JobResult actual = processor.process(job);

    verify(mockStartupConfigProvider, times(1)).getStartupConfig();
    assertEquals("fake_mic_topic", actual.topicId().orElseThrow());
  }

  private static Job generateFakeJob(String jobParamsFile) throws Exception {
    String json =
        Resources.toString(
            requireNonNull(MatchJobProcessorTest.class.getResource(jobParamsFile)), UTF_8);
    Map<String, String> params =
        getProtoFromJson(json, CreateJobRequest.class).getJobParametersMap();
    return Job.builder()
        .setJobKey(FAKE_JOB_KEY)
        .setJobProcessingTimeout(PROCESSING_TIMEOUT)
        .setRequestInfo(RequestInfo.newBuilder().putAllJobParameters(params).build())
        .setCreateTime(REQUEST_RECEIVED_AT)
        .setUpdateTime(REQUEST_UPDATED_AT)
        .setProcessingStartTime(REQUEST_PROCESSING_STARTED_AT)
        .setJobStatus(JobStatus.IN_PROGRESS)
        .setNumAttempts(0)
        .build();
  }

  private static CustomMetric makeMetric(
      String name, String unit, double value, ImmutableMap<String, String> labels) {
    return CustomMetric.builder()
        .setNameSpace("cfm/mrp")
        .setName(name)
        .setUnit(unit)
        .setValue(value)
        .setLabels(labels)
        .build();
  }
}
