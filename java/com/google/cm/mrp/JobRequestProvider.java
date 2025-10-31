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

import static com.google.cm.mrp.Constants.INITIAL_WORKGROUP_KEY;
import static com.google.cm.mrp.api.JobResultCodeProto.JobResultCode.INTERNAL_ERROR;
import static com.google.cm.mrp.api.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;

import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.api.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.dataprocessor.converters.JobResultCodeConverter;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.scp.operator.cpio.jobclient.model.GetJobRequest;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.WorkgroupAllocationFuncResponse;
import com.google.scp.operator.protos.shared.backend.ResultInfoProto.ResultInfo;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Prepares requests to send to JobClient, adding required functions as needed. */
public final class JobRequestProvider {

  private static final Logger logger = LoggerFactory.getLogger(JobRequestProvider.class);

  private static final String APPLICATION_ID = "application_id";
  private static final String DATA_OWNER_LIST = "data_owner_list";

  private final DataSourceSizeProvider dataSourceSizeProvider;
  private final StartupConfigProvider startupConfigProvider;
  private final FeatureFlagProvider featureFlagProvider;

  @Inject
  JobRequestProvider(
      DataSourceSizeProvider dataSourceSizeProvider,
      StartupConfigProvider startupConfigProvider,
      FeatureFlagProvider featureFlagProvider) {
    this.dataSourceSizeProvider = dataSourceSizeProvider;
    this.startupConfigProvider = startupConfigProvider;
    this.featureFlagProvider = featureFlagProvider;
  }

  /** Builds GetJobRequest with necessary functions, derived from startup configs as necessary */
  public GetJobRequest getJobRequest() {
    GetJobRequest.Builder getJobRequest = GetJobRequest.builder();

    if (featureFlagProvider.getFeatureFlags().workgroupsEnabled()) {
      var applicationIdWorkgroups = featureFlagProvider.getFeatureFlags().applicationIdWorkgroups();
      getJobRequest.setWorkgroupAllocationFunc(
          buildWorkgroupAllocationFunction(applicationIdWorkgroups));
    }

    // Set function for job completion notifications
    getJobRequest.setJobCompletionNotificationTopicIdFunc(
        (Job job) -> {
          ImmutableMap<String, String> notificationTopics =
              startupConfigProvider.getStartupConfig().notificationTopics();

          return getApplicationId(job)
              .flatMap(id -> Optional.ofNullable(notificationTopics.get(id)))
              .orElse("");
        });

    return getJobRequest.build();
  }

  private Function<Job, WorkgroupAllocationFuncResponse> buildWorkgroupAllocationFunction(
      Map<String, String> applicationIdWorkgroups) {
    return (Job job) -> {
      try {
        var applicationIdOpt = getApplicationId(job);
        if (applicationIdOpt.isEmpty() || applicationIdOpt.get().isBlank()) {
          logger.warn("Missing application ID job parameter");
          return buildErrorWorkgroupAllocationResponse(INVALID_PARAMETERS);
        }
        String applicationId = applicationIdOpt.get();
        String fallbackWorkgroup = INITIAL_WORKGROUP_KEY;
        // Check if there's a dedicated workgroup for the application
        if (applicationIdWorkgroups.containsKey(applicationId)) {
          fallbackWorkgroup = applicationIdWorkgroups.get(applicationId);
        }
        // Check if job could be allocation to large-job workgroup
        return tryLargeJobAllocation(job, applicationId, fallbackWorkgroup);
      } catch (Throwable e) {
        logger.error("Failed to determine workgroup: ", e);
        return buildErrorWorkgroupAllocationResponse(INTERNAL_ERROR);
      }
    };
  }

  private WorkgroupAllocationFuncResponse tryLargeJobAllocation(
      Job job, String applicationId, String fallbackWorkgroup) {
    var largeJobWorkgroupName = startupConfigProvider.getStartupConfig().largeJobWorkgroupName();
    var largeJobApplications = featureFlagProvider.getFeatureFlags().largeJobApplicationIds();
    var fallbackResponse =
        WorkgroupAllocationFuncResponse.builder().setWorkgroupId(fallbackWorkgroup).build();

    // If there's no dedicated workgroup for large jobs, then just use fallback
    if (INITIAL_WORKGROUP_KEY.equals(largeJobWorkgroupName)) {
      return fallbackResponse;
    }

    // Also use fallback if applicationId is not in allowlist
    if (!largeJobApplications.contains(applicationId)) {
      return fallbackResponse;
    }

    // verify data owners first
    var dataOwnersListOpt = getDataOwnersList(job);
    if (dataOwnersListOpt.isEmpty()) {
      logger.warn("Missing or invalid data owner list job parameter");
      return buildErrorWorkgroupAllocationResponse(INVALID_PARAMETERS);
    }

    long threshold = featureFlagProvider.getFeatureFlags().largeJobThresholdBytes();
    var accountIdentity =
        Optional.of(job.requestInfo().getAccountIdentity()).filter(Predicate.not(String::isBlank));
    try {
      var passesThreshold =
          dataSourceSizeProvider.isAtLeastSize(threshold, dataOwnersListOpt.get(), accountIdentity);
      // If greater than threshold, then assign it to the large-job workgroup
      if (passesThreshold) {
        return WorkgroupAllocationFuncResponse.builder()
            .setWorkgroupId(largeJobWorkgroupName)
            .build();
      } else {
        return fallbackResponse;
      }
    } catch (JobProcessorException e) {
      logger.warn("Failed to calculate size of data source: ", e);
      var apiErrorCode = JobResultCodeConverter.convert(e.getErrorCode());
      return buildErrorWorkgroupAllocationResponse(apiErrorCode);
    }
  }

  private WorkgroupAllocationFuncResponse buildErrorWorkgroupAllocationResponse(
      JobResultCode errorCode) {
    return WorkgroupAllocationFuncResponse.builder()
        .setResultInfo(ResultInfo.newBuilder().setReturnCode(errorCode.name()).build())
        .build();
  }

  private Optional<String> getApplicationId(Job job) {
    return Optional.ofNullable(job.requestInfo().getJobParametersMap().get(APPLICATION_ID))
        .map(String::toLowerCase);
  }

  private Optional<DataOwnerList> getDataOwnersList(Job job) {
    try {
      var map = job.requestInfo().getJobParametersMap();
      return Optional.ofNullable(
          ProtoUtils.getProtoFromJson(map.getOrDefault(DATA_OWNER_LIST, ""), DataOwnerList.class));
    } catch (InvalidProtocolBufferException e) {
      return Optional.empty();
    }
  }
}
