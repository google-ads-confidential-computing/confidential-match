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

import com.google.cm.mrp.backend.ApplicationProto.ApplicationId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.ParameterClient.ParameterClientException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete class implementing {@link FeatureFlagProvider} interface. TODO(b/352391448): consider
 * renaming to signify runtime parameter provider
 */
public final class FeatureFlagProviderImpl implements FeatureFlagProvider {
  private static final String FEATURE_FLAGS = "FEATURE_FLAGS";
  private static final String LIST_DELIMITER = ",";

  private static final int DEFAULT_MAX_RECORDS_PER_PROTO_FILE = 100_000;
  // Threshold in bytes
  private static final long DEFAULT_LARGE_JOB_THRESHOLD = 1024 * 1024 * 1024; // 1GiB
  private static final int MAX_CACHE_SIZE = 1;
  private static final long CACHE_TIME_TO_LIVE = 60 * 5;
  private static final int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();
  private static final Logger logger = LoggerFactory.getLogger(FeatureFlagProviderImpl.class);
  // Parameter client used to fetch the parameter from cloud.
  private final ParameterClient parameterClient;

  @Inject
  FeatureFlagProviderImpl(ParameterClient parameterClient) {
    this.parameterClient = parameterClient;
  }

  private final LoadingCache<String, FeatureFlags> cache =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_CACHE_SIZE)
          .expireAfterWrite(CACHE_TIME_TO_LIVE, TimeUnit.SECONDS)
          .concurrencyLevel(CONCURRENCY_LEVEL)
          .build(getCacheLoader());

  /** Gets {@link FeatureFlags} from cloud parameter store with {@link ParameterClient} */
  @Override
  public FeatureFlags getFeatureFlags() {
    try {
      return cache.get(FEATURE_FLAGS);
    } catch (ExecutionException | UncheckedExecutionException e) {
      String message = "Unable to get feature flags from cache";
      logger.info(message);
      throw new RuntimeException(message, e);
    }
  }

  private CacheLoader<String, FeatureFlags> getCacheLoader() {
    return new CacheLoader<>() {
      @Override
      public FeatureFlags load(final String key) {
        boolean micFeatureEnabled =
            getValue(Parameter.MIC_FEATURE_ENABLED).map(Boolean::parseBoolean).orElse(false);
        boolean coordinatorBatchEncryptionEnabled =
            getValue(Parameter.COORDINATOR_BATCH_ENCRYPTION_ENABLED)
                .map(Boolean::parseBoolean)
                .orElse(false);
        boolean workgroupsEnabled =
            getValue(Parameter.WORKGROUPS_ENABLED).map(Boolean::parseBoolean).orElse(false);
        boolean protoPassthroughMetadataEnabled =
            getValue(Parameter.PROTO_PASSTHROUGH_METADATA_ENABLED).map(Boolean::parseBoolean).orElse(false);
        int maxRecordsPerProtoOutputFile =
            getValue(Parameter.MAX_RECORDS_PER_PROTO_OUTPUT_FILE)
                .map(Integer::parseInt)
                .orElse(DEFAULT_MAX_RECORDS_PER_PROTO_FILE);
        long largeJobThresholdBytes =
            getValue(Parameter.LARGE_JOB_THRESHOLD)
                .map(Long::parseLong)
                .orElse(DEFAULT_LARGE_JOB_THRESHOLD);
        var featureFlagsBuilder =
            FeatureFlags.builder()
                .setEnableMIC(micFeatureEnabled)
                .setWorkgroupsEnabled(workgroupsEnabled)
                .setCoordinatorBatchEncryptionEnabled(coordinatorBatchEncryptionEnabled)
                .setProtoPassthroughMetadataEnabled(protoPassthroughMetadataEnabled)
                .setMaxRecordsPerProtoOutputFile(maxRecordsPerProtoOutputFile)
                .setLargeJobThresholdBytes(largeJobThresholdBytes);

        addApplicationIdWorkgroups(featureFlagsBuilder);
        addLargeJobWorkgroupApplicationIds(featureFlagsBuilder);

        return featureFlagsBuilder.build();
      }
    };
  }

  /** Go through each application ID and see if a parameter exists that defines a workgroup name */
  private void addApplicationIdWorkgroups(FeatureFlags.Builder builder) {
    for (ApplicationId applicationId : ApplicationId.values()) {
      if (applicationId == ApplicationId.APPLICATION_ID_UNSPECIFIED) {
        continue;
      }
      String applicationIdName = applicationId.name().toLowerCase(Locale.US);
      Optional<String> workgroupName =
          getValue(Parameter.ASSIGNED_WORKGROUP_PREFIX + applicationIdName);
      workgroupName
          .filter(name -> !name.isEmpty())
          .ifPresent(name -> builder.addWorkgroupApplicationId(applicationIdName, name));
    }
  }

  /**
   * Go through each application ID and see if it is included in the list of IDs to consider for
   * large-job allocation.
   */
  private void addLargeJobWorkgroupApplicationIds(FeatureFlags.Builder builder) {
    Optional<String> applicationIdsParam = getValue(Parameter.LARGE_JOB_APPLICATION_IDS);
    applicationIdsParam.ifPresent(
        applicationIds -> {
          var applicationIdsSet = ImmutableSet.copyOf(applicationIds.split(LIST_DELIMITER));

          for (ApplicationId applicationId : ApplicationId.values()) {
            if (applicationId == ApplicationId.APPLICATION_ID_UNSPECIFIED) {
              continue;
            }
            String applicationIdName = applicationId.name().toLowerCase(Locale.US);
            if (applicationIdsSet.contains(applicationIdName)) {
              builder.addLargeJobApplicationId(applicationIdName);
            }
          }
        });
  }

  private Optional<String> getValue(Parameter parameter) {
    return getValue(parameter.name());
  }

  private Optional<String> getValue(String parameter) {
    try {
      // Parameters are stored in the format CFM-{environment}-{name}
      return parameterClient.getParameter(
          parameter, Optional.of(Parameter.CFM_PREFIX), /* includeEnvironmentParam */ true);
    } catch (ParameterClientException ex) {
      String message = String.format("Unable to fetch flag %s", parameter);
      logger.info(message);
      throw new RuntimeException(message, ex);
    }
  }
}
