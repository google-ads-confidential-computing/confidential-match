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

import static com.google.cm.mrp.EncryptionType.COORDINATOR_KEY;
import static com.google.cm.mrp.EncryptionType.INVALID_ENCRYPTION_TYPE;
import static com.google.cm.mrp.EncryptionType.UNENCRYPTED;
import static com.google.cm.mrp.EncryptionType.WRAPPED_KEY;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.FAILED_WITH_ROW_ERRORS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_DATA_LOCATION_CONFIGURATION;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARTIAL_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_SCHEMA_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.PARTIAL_SUCCESS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.cm.mrp.Annotations.JobProcessorMaxRetries;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.api.EncryptionMetadataProto;
import com.google.cm.mrp.backend.ApplicationProto.ApplicationId;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.dataprocessor.DataProcessor;
import com.google.cm.mrp.dataprocessor.converters.EncryptionMetadataConverter;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.models.JobParameters;
import com.google.cm.mrp.models.JobParameters.OutputDataLocation;
import com.google.cm.util.ProtoUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import com.google.scp.operator.cpio.metricclient.MetricClient.MetricClientException;
import com.google.scp.operator.cpio.metricclient.model.CustomMetric;
import com.google.scp.operator.protos.shared.backend.ErrorCountProto.ErrorCount;
import com.google.scp.operator.protos.shared.backend.ErrorSummaryProto.ErrorSummary;
import com.google.scp.operator.protos.shared.backend.ResultInfoProto.ResultInfo;
import com.google.scp.shared.proto.ProtoUtil;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.text.DecimalFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class providing processing for {@link Job} objects. Throws {@link JobProcessorException
 * JobProcessorException} on failure.
 */
public final class MatchJobProcessor implements JobProcessor {

  /** Return message for a successful job. */
  static final String RESULT_SUCCESS_MESSAGE = "Job successfully processed.";

  /** Return message for a failed job. */
  static final String RESULT_FAILURE_MESSAGE = "Job failed.";

  /** Return message for a partially successful job. */
  static final String RESULT_PARTIAL_SUCCESS_MESSAGE =
      "Job successfully processed with partial errors.";

  /** Prefix for total errorCounts. */
  static final String TOTAL_ERRORS_PREFIX = "TOTAL_ERRORS";

  private static final Logger logger = LoggerFactory.getLogger(MatchJobProcessor.class);
  private static final String JOB_PROCESSOR_RETRY_NAME = "job_processor_retry";
  private static final Duration WAIT_DURATION = Duration.of(10, SECONDS);
  private static final String DATA_OWNER_LIST = "data_owner_list";
  private static final String APPLICATION_ID = "application_id";
  private static final String ENCRYPTION_METADATA = "encryption_metadata";
  private static final String ENCODING_TYPE = "encoding_type";

  // Format stats to five significant digits after decimal
  private static final DecimalFormat FORMATTER = new DecimalFormat("0.#####");

  private final Clock clock;
  private final DataProcessor dataProcessor;
  private final Retry jobProcessorRetry;
  private final MetricClient metricClient;
  private final FeatureFlagProvider featureFlagProvider;
  private final StartupConfigProvider startupConfigProvider;

  @Inject
  MatchJobProcessor(
      Clock clock,
      DataProcessor dataProcessor,
      @JobProcessorMaxRetries int jobProcessorMaxRetries,
      MetricClient metricClient,
      FeatureFlagProvider featureFlagProvider,
      StartupConfigProvider startupConfigProvider) {
    this.clock = clock;
    this.dataProcessor = dataProcessor;
    this.jobProcessorRetry =
        Retry.of(
            JOB_PROCESSOR_RETRY_NAME,
            RetryConfig.custom()
                .maxAttempts(jobProcessorMaxRetries)
                .waitDuration(WAIT_DURATION)
                .retryOnException(MatchJobProcessor::isRetryable)
                .failAfterMaxAttempts(true)
                .build());
    this.metricClient = metricClient;
    this.featureFlagProvider = featureFlagProvider;
    this.startupConfigProvider = startupConfigProvider;
  }

  /** This method is responsible for processing match requests */
  @Override
  public JobResult process(Job job) throws JobProcessorException {
    Stopwatch timer = Stopwatch.createStarted();
    MatchJobResult matchJobResult = processInternal(job);
    MatchStatistics stats = matchJobResult.stats();
    Optional<ErrorSummary> errorSummaryOptional = Optional.empty();
    JobResultCode returnCode;

    // Handle row-level errors
    if (!stats.datasource1Errors().isEmpty() && matchJobResult.jobCode() == SUCCESS) {
      ErrorSummary errorSummary = writeAndGetErrorMetrics(job, stats);
      errorSummaryOptional = Optional.of(errorSummary);
      List<ErrorCount> errorCountList = errorSummary.getErrorCountsList();
      // Get total count from existing value
      long totalCount =
          errorCountList.stream()
              .filter(err -> err.getCategory().equals(TOTAL_ERRORS_PREFIX))
              .mapToLong(ErrorCount::getCount)
              .findFirst()
              .orElse(0L);

      // Determine if should be converted to job-level failure
      if (totalCount != stats.numDataRecords()) {
        returnCode = PARTIAL_SUCCESS;
      } else {
        if (errorCountList.size() > 2) {
          returnCode = FAILED_WITH_ROW_ERRORS;
        } else {
          Optional<ErrorCount> error =
              errorCountList.stream()
                  .filter(err -> !err.getCategory().equals(TOTAL_ERRORS_PREFIX))
                  .findFirst();
          if (error.isEmpty()) {
            logger.error("Partial row-level error not found");
            returnCode = INVALID_PARTIAL_ERROR;
          } else {
            returnCode = JobResultCode.valueOf(error.get().getCategory());
          }
        }
      }
    } else {
      returnCode = matchJobResult.jobCode();
    }

    // Get metrics map, overwriting all previous metrics entries
    HashMap<String, String> metricsMap =
        writeAndGetMetrics(job, returnCode, timer.elapsed(TimeUnit.SECONDS), stats);

    var resultInfoBuilder =
        ResultInfo.newBuilder()
            .setReturnCode(returnCode.name())
            .setReturnMessage(getReturnMessage(returnCode))
            .setFinishedAt(ProtoUtil.toProtoTimestamp(Instant.now(clock)))
            .putAllResultMetadata(metricsMap);
    errorSummaryOptional.ifPresent(resultInfoBuilder::setErrorSummary);

    JobResult.Builder builder =
        JobResult.builder().setJobKey(job.jobKey()).setResultInfo(resultInfoBuilder.build());
    // TODO(b/335663716): Remove check on enableMIC after integration testing
    if (featureFlagProvider.getFeatureFlags().enableMIC()) {
      getTopicId(job).ifPresent(topicId -> builder.setTopicId(Optional.of(topicId)));
    }
    return builder.build();
  }

  private MatchJobResult processInternal(Job job) {
    try {
      logger.info("Starting to process job: {}", job.jobKey());

      FeatureFlags featureFlags = featureFlagProvider.getFeatureFlags();
      Map<String, String> params = job.requestInfo().getJobParametersMap();
      validateJobParameters(params, featureFlags);
      DataOwnerList dataOwnersList = validateAndGetDataOwnersList(params);
      DataLocation dataLocation =
          dataOwnersList.getDataOwnersList().stream()
              .filter(dataOwner -> dataOwner.getDataLocation().getIsStreamed())
              .findAny()
              .orElseThrow()
              .getDataLocation();

      Optional<EncryptionMetadata> encryptionMetadata = validateAndGetEncryptionMetadata(params);
      EncodingType encodingType = validateAndGetEncodingType(params);

      MatchConfig matchConfig = MatchConfigProvider.getMatchConfig(params.get(APPLICATION_ID));
      Optional<String> dataOwnerIdentity =
          Optional.of(job.requestInfo().getAccountIdentity())
              .filter(Predicate.not(String::isBlank));
      logger.info(
          "Job {} validated. ApplicationId: {}, encrypted: {}",
          job.jobKey(),
          matchConfig.getApplicationId(),
          encryptionMetadata.isPresent());

      AtomicInteger jobProcessorRetryCount = new AtomicInteger();

      MatchStatistics stats =
          jobProcessorRetry.executeCallable(
              () -> {
                logger.info("JobProcessorRetryCount: {}", jobProcessorRetryCount.getAndIncrement());
                return dataProcessor.process(
                    featureFlags,
                    matchConfig,
                    JobParameters.builder()
                        .setJobId(job.jobKey().getJobRequestId())
                        .setDataLocation(dataLocation)
                        .setDataOwnerIdentity(dataOwnerIdentity)
                        .setOutputDataLocation(
                            OutputDataLocation.forNameAndPrefix(
                                job.requestInfo().getOutputDataBucketName(),
                                job.requestInfo().getOutputDataBlobPrefix()))
                        .setEncryptionMetadata(encryptionMetadata)
                        .setEncodingType(encodingType)
                        .build());
              });

      return MatchJobResult.create(SUCCESS, stats);
    } catch (JobProcessorException e) {
      logger.info("Job failure caught in MatchJobProcessor", e);
      return MatchJobResult.create(e.getErrorCode(), MatchStatistics.emptyInstance());
    } catch (Throwable e) {
      Throwable internalEx = e.getCause();
      logger.error("Unknown error caught in MatchJobProcessor. Job will be failed.", internalEx);
      if (internalEx instanceof JobProcessorException) {
        JobResultCode jobCode = ((JobProcessorException) internalEx).getErrorCode();
        return MatchJobResult.create(jobCode, MatchStatistics.emptyInstance());
      }
      return MatchJobResult.create(
          JobResultCode.JOB_RESULT_CODE_UNKNOWN, MatchStatistics.emptyInstance());
    }
  }

  private String getReturnMessage(JobResultCode returnCode) {
    switch (returnCode) {
      case SUCCESS:
        return RESULT_SUCCESS_MESSAGE;
      case PARTIAL_SUCCESS:
        return RESULT_PARTIAL_SUCCESS_MESSAGE;
      default:
        return RESULT_FAILURE_MESSAGE;
    }
  }

  private static boolean isRetryable(Throwable e) {
    boolean retry = e instanceof JobProcessorException && ((JobProcessorException) e).isRetriable();
    if (retry) {
      // Log retried exceptions. If not retried, they are logged right before failing
      logger.info("Retrying JobProcessorException", e);
    }
    return retry;
  }

  private static void validateJobParameters(
      Map<String, String> parameters, FeatureFlags featureFlags) {
    if (parameters == null
        || parameters.size() == 0
        || parameters.getOrDefault(APPLICATION_ID, "").isBlank()
        || parameters.getOrDefault(DATA_OWNER_LIST, "").isBlank()) {
      String message = "Empty or invalid job parameters";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_PARAMETERS);
    }

    if (parameters.get(APPLICATION_ID).equalsIgnoreCase(ApplicationId.MIC.name())
        && !featureFlags.enableMIC()) {
      String message = "Invalid application id as MIC isn't enabled.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_PARAMETERS);
    }
  }

  private static DataOwnerList validateAndGetDataOwnersList(Map<String, String> parameters)
      throws JobProcessorException {
    DataOwnerList dataOwnerList =
        getProtoFromJobParams(parameters, DATA_OWNER_LIST, DataOwnerList.class);

    if (!isSingleOrDoubleDataSource(dataOwnerList)) {
      String message = "Only one or two data sources are supported.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_PARAMETERS);
    }

    if (dataOwnerList.getDataOwnersList().stream()
        .noneMatch(dataOwner -> dataOwner.getDataLocation().getIsStreamed())) {
      String message = "Streamed data source is missing.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_PARAMETERS);
    }

    if (dataOwnerList.getDataOwnersList().stream()
            .filter(dataOwner -> dataOwner.getDataLocation().getIsStreamed())
            .count()
        > 1) {
      String message = "Only one streamed data source is supported.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_PARAMETERS);
    }

    dataOwnerList
        .getDataOwnersList()
        .forEach(dataOwner -> validateDataLocation(dataOwner.getDataLocation()));
    return dataOwnerList;
  }

  private static void validateDataLocation(DataLocation dataLocation) {
    if (dataLocation.getInputDataBlobPathsCount() > 0
        && !dataLocation.getInputDataBlobPrefix().isEmpty()) {
      String message =
          "Data location should specify one of input data blob paths or input data blob prefix.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_DATA_LOCATION_CONFIGURATION);
    } else if (dataLocation.getInputDataBlobPathsCount() > 0
        && dataLocation.getInputSchemaPath().isEmpty()) {
      String message = "Using input data blob paths requires specifying input schema path.";
      logger.error(message);
      throw new JobProcessorException(message, MISSING_SCHEMA_ERROR);
    }
  }

  private static Optional<EncryptionMetadata> validateAndGetEncryptionMetadata(
      Map<String, String> jobParamsMap) {
    if (!jobParamsMap.containsKey(ENCRYPTION_METADATA)) return Optional.empty();
    var apiEncryptionMetadata =
        getProtoFromJobParams(
            jobParamsMap,
            ENCRYPTION_METADATA,
            com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.class);
    if (!apiEncryptionMetadata.hasEncryptionKeyInfo()) {
      String message = "EncryptionKeyInfo is required.";
      logger.error(message);
      throw new JobProcessorException(message, INVALID_PARAMETERS);
    }
    return Optional.of(
        EncryptionMetadataConverter.convertToBackendEncryptionMetadata(apiEncryptionMetadata));
  }

  private static EncodingType validateAndGetEncodingType(Map<String, String> jobParamsMap) {
    if (!jobParamsMap.containsKey(ENCODING_TYPE)) return EncodingType.BASE64;
    String apiEncodingType = jobParamsMap.get(ENCODING_TYPE);
    try {
      return EncodingType.valueOf(apiEncodingType);
    } catch (IllegalArgumentException e) {
      String message = "Invalid encoding type.";
      logger.warn(message, e);
      throw new JobProcessorException(message, e, INVALID_PARAMETERS);
    }
  }

  private static <T extends Message> T getProtoFromJobParams(
      Map<String, String> map, String parameterName, Class<T> protoClass) {
    try {
      return ProtoUtils.getProtoFromJson(map.getOrDefault(parameterName, ""), protoClass);
    } catch (InvalidProtocolBufferException e) {
      String message = "Invalid job request parameters";
      logger.error(message);
      throw new JobProcessorException(message, e, INVALID_PARAMETERS);
    }
  }

  private static boolean isSingleOrDoubleDataSource(DataOwnerList dataOwnerList) {
    return dataOwnerList.getDataOwnersCount() >= 1
        && dataOwnerList.getDataOwnersCount() <= 2
        && dataOwnerList.getDataOwnersList().size() >= 1
        && dataOwnerList.getDataOwnersList().size() <= 2;
  }

  private static CustomMetric createMetric(
      String name, String unit, double value, ImmutableMap<String, String> labels) {
    return CustomMetric.builder()
        .setNameSpace("cfm/mrp")
        .setName(name)
        .setUnit(unit)
        .setValue(value)
        .setLabels(labels)
        .build();
  }

  private void writeMetric(CustomMetric metric) {
    try {
      metricClient.recordMetric(metric);
    } catch (MetricClientException ex) {
      logger.error("Unable to write metric.", ex);
    }
  }

  private void writeAndLogMetric(
      String name,
      String unit,
      double value,
      ImmutableMap<String, String> labels,
      HashMap<String, String> result) {
    CustomMetric metric = createMetric(name, unit, value, labels);
    logger.info("Writing metric: {}", metric);
    writeMetric(metric);

    // Add metric to metadata map, with condition when applicable
    String condition = metric.labels().get("MatchCondition");
    String nameSuffix = condition == null ? "" : " " + condition;
    result.put(name + nameSuffix, FORMATTER.format(value));
  }

  private ImmutableMap<String, String> getMetricLabels(Job job) {
    // Variables related to the job
    String jobId = job.jobKey().getJobRequestId();
    String rawAccountIdentity = job.requestInfo().getAccountIdentity();
    String customerId = rawAccountIdentity.isBlank() ? "Default" : rawAccountIdentity;
    String applicationId =
        Optional.ofNullable(job.requestInfo().getJobParametersMap().get("application_id"))
            .filter(Predicate.not(String::isBlank))
            .orElse("Default");
    String encryptionType = getEncryptionType(job).name();
    return ImmutableMap.ofEntries(
        Map.entry("CustomerServiceAccount", customerId),
        Map.entry("ApplicationId", applicationId),
        Map.entry("JobId", jobId),
        Map.entry("EncryptionType", encryptionType));
  }

  /** Record statistics as logs, send as metrics to monitoring, and return in a list. */
  private HashMap<String, String> writeAndGetMetrics(
      Job job, JobResultCode jobCode, long jobDurationSeconds, MatchStatistics stats) {

    HashMap<String, String> result = new HashMap<>();

    // Matching statistics
    ImmutableMap<String, String> matchLabels =
        ImmutableMap.<String, String>builder()
            .putAll(getMetricLabels(job))
            .put("FileFormat", stats.fileDataFormat().name())
            .build();
    // At least partial success for this purpose
    boolean isSuccess = jobCode == SUCCESS || jobCode == PARTIAL_SUCCESS;
    boolean isNonRetryableError = jobCode.getNumber() >= 100;
    String resultType =
        isSuccess
            ? jobCode.name()
            : isNonRetryableError ? "NON_RETRYABLE_ERROR" : "RETRYABLE_ERROR";

    // Job Statistics
    ImmutableMap<String, String> jobLabels =
        ImmutableMap.<String, String>builder()
            .putAll(matchLabels)
            .put("ReturnCode", jobCode.name())
            .put("ReturnCodeType", resultType)
            .put("Attempts", Integer.toString(job.numAttempts() + 1))
            .build();
    writeMetric(createMetric("jobcount", "Count", 1.0, jobLabels)); // Don't log or save to metadata
    writeAndLogMetric("jobdurationseconds", "Seconds", jobDurationSeconds, jobLabels, result);
    // Add 1 to number of attempts to include this attempt
    writeAndLogMetric("jobattempts", "Count", job.numAttempts() + 1, jobLabels, result);

    // If this job has a retryable error, do not process matching statistics
    if (!isSuccess && !isNonRetryableError) {
      return result;
    }

    ImmutableMap<String, Long> validChecksPerCondition = stats.validConditionChecks();
    ImmutableMap<String, Long> matchesPerCondition = stats.conditionMatches();
    ImmutableMap<String, Long> datasource2MatchesPerCondition = stats.datasource2ConditionMatches();
    long validConditionChecksTotal =
        validChecksPerCondition.values().stream().mapToLong(Long::longValue).sum();
    long matchesTotal = matchesPerCondition.values().stream().mapToLong(Long::longValue).sum();
    double matchPercentage =
        validConditionChecksTotal == 0L ? 0.0 : (100.0 * matchesTotal) / validConditionChecksTotal;
    writeAndLogMetric("numfiles", "Count", stats.numFiles(), matchLabels, result);
    writeAndLogMetric("numdatarecords", "Count", stats.numDataRecords(), matchLabels, result);
    writeAndLogMetric(
        "numdatarecordswithmatch", "Count", stats.numDataRecordsWithMatch(), matchLabels, result);
    writeAndLogMetric("numpii", "Count", validConditionChecksTotal, matchLabels, result);
    writeAndLogMetric("nummatches", "Count", matchesTotal, matchLabels, result);
    writeAndLogMetric("matchpercentage", "Percent", matchPercentage, matchLabels, result);

    // Matching statistics for specific conditions
    for (Map.Entry<String, Long> checksForCondition : validChecksPerCondition.entrySet()) {
      ImmutableMap<String, String> typeLabels =
          ImmutableMap.<String, String>builder()
              .putAll(matchLabels)
              .put("MatchCondition", checksForCondition.getKey())
              .build();
      long conditionMatches =
          Optional.ofNullable(matchesPerCondition.get(checksForCondition.getKey())).orElse(0L);
      long datasource2Matches =
          Optional.ofNullable(datasource2MatchesPerCondition.get(checksForCondition.getKey()))
              .orElse(0L);
      long conditionChecks = Optional.ofNullable(checksForCondition.getValue()).orElse(0L);
      double conditionMatchPercentage =
          conditionChecks == 0L ? 0.0 : (100.0 * conditionMatches) / conditionChecks;
      writeAndLogMetric("nummatchespercondition", "Count", conditionMatches, typeLabels, result);
      writeAndLogMetric(
          "numdatasource2matchespercondition", "Count", datasource2Matches, typeLabels, result);
      writeAndLogMetric(
          "matchpercentagepercondition", "Percent", conditionMatchPercentage, typeLabels, result);
    }

    // Add the DataFormat to the result without recording it as a metric
    result.put("fileformat", stats.fileDataFormat().name());
    return result;
  }

  /** Record errors as logs, send as metrics to monitoring, and return in an ErrorSummary. */
  private ErrorSummary writeAndGetErrorMetrics(Job job, MatchStatistics stats) {
    ImmutableMap<String, Long> datasource1Errors = stats.datasource1Errors();
    ErrorSummary.Builder errorSummary = ErrorSummary.newBuilder();
    long totalCounts = 0L;
    // Statistics for datasource1Errors
    for (Map.Entry<String, Long> errorEntry : datasource1Errors.entrySet()) {
      String errorCode = errorEntry.getKey();
      ImmutableMap<String, String> typeLabels =
          ImmutableMap.<String, String>builder()
              .putAll(getMetricLabels(job))
              .put("FileFormat", stats.fileDataFormat().name())
              .put("ErrorCode", errorCode)
              .build();
      long errorCount = Optional.ofNullable(datasource1Errors.get(errorEntry.getKey())).orElse(0L);
      CustomMetric metric = createMetric("numerrorspererrorcode", "Count", errorCount, typeLabels);
      logger.info("Writing metric: {}", metric);
      writeMetric(metric);
      errorSummary.addErrorCounts(
          ErrorCount.newBuilder().setCategory(errorCode).setCount(errorCount));
      totalCounts += errorCount;
    }
    errorSummary.addErrorCounts(
        ErrorCount.newBuilder().setCategory(TOTAL_ERRORS_PREFIX).setCount(totalCounts));
    return errorSummary.build();
  }

  /** Returns the encryption type used for a given job */
  private EncryptionType getEncryptionType(Job job) {
    var jobParamsMap = job.requestInfo().getJobParametersMap();
    if (!jobParamsMap.containsKey(ENCRYPTION_METADATA)) return UNENCRYPTED;
    EncryptionMetadataProto.EncryptionMetadata apiEncryptionMetadata;
    try {
      apiEncryptionMetadata =
          getProtoFromJobParams(
              jobParamsMap,
              ENCRYPTION_METADATA,
              com.google.cm.mrp.api.EncryptionMetadataProto.EncryptionMetadata.class);
      if (apiEncryptionMetadata.hasEncryptionKeyInfo()) {
        var encryptionKeyInfo = apiEncryptionMetadata.getEncryptionKeyInfo();
        if (encryptionKeyInfo.hasCoordinatorKeyInfo()) return COORDINATOR_KEY;
        if (encryptionKeyInfo.hasWrappedKeyInfo() || encryptionKeyInfo.hasAwsWrappedKeyInfo()) {
          return WRAPPED_KEY;
        }
      }
    } catch (Exception e) {
      return INVALID_ENCRYPTION_TYPE;
    }
    return INVALID_ENCRYPTION_TYPE;
  }

  private Optional<String> getTopicId(Job job) {
    ImmutableMap<String, String> notificationTopics =
        startupConfigProvider.getStartupConfig().notificationTopics();
    String applicationId =
        job.requestInfo().getJobParametersMap().get(APPLICATION_ID).toLowerCase();
    return Optional.ofNullable(notificationTopics.get(applicationId));
  }
}
