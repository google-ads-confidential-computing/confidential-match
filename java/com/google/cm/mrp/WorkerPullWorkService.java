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

import static com.google.cm.mrp.Constants.INTERNAL_ERROR_ALERT_LOG;
import static com.google.cm.mrp.api.JobResultCodeProto.JobResultCode.INTERNAL_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SUCCESS;

import com.google.cm.mrp.Annotations.JobQueueRetryDelaySec;
import com.google.cm.mrp.api.JobResultCodeProto;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.dataprocessor.converters.JobResultCodeConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.scp.operator.cpio.jobclient.JobClient;
import com.google.scp.operator.cpio.jobclient.model.GetJobRequest;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.JobResult;
import com.google.scp.operator.cpio.jobclient.model.JobRetryRequest;
import com.google.scp.operator.protos.shared.backend.ResultInfoProto.ResultInfo;
import java.security.Security;
import java.time.Duration;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.conscrypt.Conscrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service for pulling and processing job requests. */
public class WorkerPullWorkService extends AbstractExecutionThreadService {

  private static final Logger logger = LoggerFactory.getLogger(WorkerPullWorkService.class);
  private static final String APPLICATION_ID = "application_id";
  private final JobClient jobClient;
  private final JobProcessor jobProcessor;
  private final int jobRetryDelaySec;
  private final StartupConfigProvider startupConfigProvider;
  private volatile boolean isRunning;

  @Inject
  WorkerPullWorkService(
      JobClient jobClient,
      JobProcessor jobProcessor,
      @JobQueueRetryDelaySec int jobRetryDelaySec,
      StartupConfigProvider startupConfigProvider) {
    this.jobClient = jobClient;
    this.jobProcessor = jobProcessor;
    this.jobRetryDelaySec = jobRetryDelaySec;
    this.startupConfigProvider = startupConfigProvider;
    isRunning = true;
  }

  @Override
  protected void run() {
    logger.info("Worker starting run.");

    try {
      startupConfigProvider
          .getStartupConfig()
          .loggingLevel()
          .ifPresent(
              logLevel -> {
                logger.info("Setting MRP log level to: {}", logLevel);
                Configurator.setAllLevels(
                    LogManager.getRootLogger().getName(), Level.getLevel(logLevel));
              });
    } catch (Exception e) {
      logger.warn("Could not set logging level", e);
    }

    if (startupConfigProvider.getStartupConfig().conscryptEnabled()) {
      // Initialize conscrypt
      Conscrypt.ProviderBuilder builder = Conscrypt.newProviderBuilder();
      builder.setName("CfmConscryptProvider");
      Security.insertProviderAt(builder.build(), 1);
      logger.info("Conscrypt has been initialized for worker.");
    }

    while (isRunning) {
      try {
        Optional<Job> job = jobClient.getJob(buildGetJobRequest(startupConfigProvider));
        if (job.isEmpty()) {
          // This is only reached when the job client backoff is exhausted
          logger.info("No job pulled.");
          isRunning = false;
          continue;
        }

        logger.info("Item pulled by worker.");
        JobResult jobResult = jobProcessor.process(job.get());
        try {
          JobResultCode jobCode = JobResultCode.valueOf(jobResult.resultInfo().getReturnCode());
          // Complete job if success or non-retriable error (number >= 100)
          if (SUCCESS == jobCode || jobCode.getNumber() >= 100) {
            jobResult = addPreviousErrorToNewJobResult(job.get(), jobResult);
            jobClient.markJobCompleted(jobResult);
          } else {
            jobResult =
                addPreviousErrorToNewJobResult(job.get(), addErrorToErrorSummary(jobResult));
            var jobRetryRequest =
                JobRetryRequest.builder()
                    .setJobKey(jobResult.jobKey())
                    .setDelay(Duration.ofSeconds(jobRetryDelaySec))
                    .setResultInfo(jobResult.resultInfo())
                    .build();
            jobClient.returnJobForRetry(jobRetryRequest);
          }
        } catch (Exception e) {
          // TODO(b/378605133): send alert in this case
          logger.error(
              INTERNAL_ERROR_ALERT_LOG
                  + " Failed to finalize jobResult for job: "
                  + job.get().jobKey(),
              e);
          var jobRetryRequest =
              JobRetryRequest.builder()
                  .setJobKey(jobResult.jobKey())
                  .setDelay(Duration.ofSeconds(jobRetryDelaySec))
                  .setResultInfo(
                      ResultInfo.newBuilder().setReturnCode(INTERNAL_ERROR.name()).build())
                  .build();
          jobClient.returnJobForRetry(jobRetryRequest);
        }
      } catch (Exception e) {
        logger.error("Exception during worker run.", e);
      }
    }

    logger.info("Worker shutting down.");
  }

  private GetJobRequest buildGetJobRequest(StartupConfigProvider configProvider) {
    GetJobRequest.Builder getJobRequest = GetJobRequest.builder();
    getJobRequest.setJobCompletionNotificationTopicIdFunc(
        (Job job) -> {
          ImmutableMap<String, String> notificationTopics =
              configProvider.getStartupConfig().notificationTopics();
          Optional<String> applicationId =
              Optional.ofNullable(job.requestInfo().getJobParametersMap().get(APPLICATION_ID))
                  .map(String::toLowerCase);
          return applicationId
              .flatMap(id -> Optional.ofNullable(notificationTopics.get(id)))
              .orElse("");
        });
    return getJobRequest.build();
  }

  /* Adds previous error to errorSummary   */
  private static JobResult addPreviousErrorToNewJobResult(Job previousJob, JobResult newJobResult) {
    JobResultCode backendResultCode =
        JobResultCode.valueOf(newJobResult.resultInfo().getReturnCode());
    // Don't add if job is success
    if (backendResultCode.equals(SUCCESS)) {
      return newJobResult;
    }
    JobResultCodeProto.JobResultCode apiResultCode =
        JobResultCodeConverter.convert(backendResultCode);
    return previousJob
        .resultInfo()
        .map(
            existingResult ->
                newJobResult.toBuilder()
                    .setResultInfo(
                        addErrorToExistingJobResult(existingResult, newJobResult.resultInfo()))
                    .build())
        .orElseGet(
            () ->
                newJobResult.toBuilder()
                    .setResultInfo(
                        newJobResult.resultInfo().toBuilder()
                            .setReturnCode(apiResultCode.name())
                            .build())
                    .build());
  }

  /* Add error to jobResult Error summary  */
  private static JobResult addErrorToErrorSummary(JobResult jobResult) {
    // Don't add if job is success
    if (jobResult.resultInfo().getReturnCode().equals(SUCCESS.name())) {
      return jobResult;
    }
    return jobResult.toBuilder()
        .setResultInfo(
            jobResult.resultInfo().toBuilder()
                .setErrorSummary(
                    jobResult.resultInfo().getErrorSummary().toBuilder()
                        .addErrorMessages(jobResult.resultInfo().getReturnCode()))
                .build())
        .build();
  }

  private static ResultInfo addErrorToExistingJobResult(
      ResultInfo existingResultInfo, ResultInfo newResultInfo) {
    var existingErrors = existingResultInfo.getErrorSummary().getErrorMessagesList();
    return newResultInfo.toBuilder()
        .setErrorSummary(
            newResultInfo.getErrorSummary().toBuilder().addAllErrorMessages(existingErrors))
        .build();
  }

  @Override
  @SuppressWarnings("UnstableApiUsage")
  protected void triggerShutdown() {
    isRunning = false;
  }
}
