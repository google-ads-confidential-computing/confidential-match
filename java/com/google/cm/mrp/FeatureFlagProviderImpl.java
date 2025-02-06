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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.ParameterClient.ParameterClientException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete class implementing {@link FeatureFlagProvider} interface. */
public final class FeatureFlagProviderImpl implements FeatureFlagProvider {
  private static final String FEATURE_FLAGS = "FEATURE_FLAGS";
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
          .build(
              new CacheLoader<>() {
                @Override
                public FeatureFlags load(final String key) {
                  boolean micFeatureEnabled =
                      getValue(Parameter.MIC_FEATURE_ENABLED)
                          .map(Boolean::parseBoolean)
                          .orElse(false);
                  boolean serializedProtoEnabled =
                      getValue(Parameter.SERIALIZED_PROTO_FEATURE_ENABLED)
                          .map(Boolean::parseBoolean)
                          .orElse(false);
                  boolean coordinatorBatchEncryptionEnabled =
                      getValue(Parameter.COORDINATOR_BATCH_ENCRYPTION_ENABLED)
                          .map(Boolean::parseBoolean)
                          .orElse(false);
                  return FeatureFlags.builder()
                      .setEnableMIC(micFeatureEnabled)
                      .setEnableSerializedProto(serializedProtoEnabled)
                      .setCoordinatorBatchEncryptionEnabled(coordinatorBatchEncryptionEnabled)
                      .build();
                }
              });

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

  private Optional<String> getValue(Parameter parameter) {
    try {
      // Parameters are stored in the format CFM-{environment}-{name}
      return parameterClient.getParameter(
          parameter.name(), Optional.of(Parameter.CFM_PREFIX), /* includeEnvironmentParam */ true);
    } catch (ParameterClientException ex) {
      String message = String.format("Unable to fetch flag %s", parameter.name());
      logger.info(message);
      throw new RuntimeException(message, ex);
    }
  }
}
