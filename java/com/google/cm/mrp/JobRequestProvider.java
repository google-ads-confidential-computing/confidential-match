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

import static com.google.cm.mrp.api.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.scp.operator.cpio.jobclient.model.GetJobRequest;
import com.google.scp.operator.cpio.jobclient.model.Job;
import com.google.scp.operator.cpio.jobclient.model.WorkgroupAllocationFuncResponse;
import com.google.scp.operator.protos.shared.backend.ResultInfoProto.ResultInfo;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Prepares requests to send to JobClient, adding required functions as needed. */
public final class JobRequestProvider {

  private static final Logger logger = LoggerFactory.getLogger(JobRequestProvider.class);

  private static final String APPLICATION_ID = "application_id";

  private static final String INITIAL_WORKGROUP_KEY = "default";

  private final StartupConfigProvider startupConfigProvider;
  private final FeatureFlagProvider featureFlagProvider;

  @Inject
  JobRequestProvider(
      StartupConfigProvider startupConfigProvider, FeatureFlagProvider featureFlagProvider) {
    this.startupConfigProvider = startupConfigProvider;
    this.featureFlagProvider = featureFlagProvider;
  }

  /** Builds GetJobRequest with necessary functions, derived from startup configs as necessary */
  public GetJobRequest getJobRequest() {
    GetJobRequest.Builder getJobRequest = GetJobRequest.builder();

    if (featureFlagProvider.getFeatureFlags().workgroupsEnabled()) {
      var applicationIdWorkgroups =
          startupConfigProvider.getStartupConfig().applicationIdWorkgroups();
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
      var functionBuilder = WorkgroupAllocationFuncResponse.builder();

      var applicationIdOpt = getApplicationId(job);
      if (applicationIdOpt.isEmpty() || applicationIdOpt.get().isBlank()) {
        logger.warn("Missing application ID job parameter");
        functionBuilder.setResultInfo(
            ResultInfo.newBuilder().setReturnCode(INVALID_PARAMETERS.name()).build());
        return functionBuilder.build();
      }

      String applicationId = applicationIdOpt.get();
      String workgroupId = INITIAL_WORKGROUP_KEY;
      if (applicationIdWorkgroups.containsKey(applicationId)) {
        workgroupId = applicationIdWorkgroups.get(applicationId);
      }
      return functionBuilder.setWorkgroupId(workgroupId).build();
    };
  }

  private Optional<String> getApplicationId(Job job) {
    return Optional.ofNullable(job.requestInfo().getJobParametersMap().get(APPLICATION_ID))
        .map(String::toLowerCase);
  }
}
