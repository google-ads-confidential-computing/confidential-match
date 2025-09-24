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
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.ParameterClient.ParameterClientException;
import java.util.Locale;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete class implementing {@link StartupConfigProvider} interface. */
// TODO(b/347031972): Look into removing the interface, which is needed only for mocking.
public final class StartupConfigProviderImpl implements StartupConfigProvider {

  private static final Logger logger = LoggerFactory.getLogger(StartupConfigProviderImpl.class);
  // Parameter client used to fetch the parameter from cloud.
  private final ParameterClient parameterClient;

  @Inject
  StartupConfigProviderImpl(ParameterClient parameterClient) {
    this.parameterClient = parameterClient;
  }

  /** Gets {@link StartupConfig} from cloud parameter store with {@link ParameterClient} */
  @Override
  public StartupConfig getStartupConfig() {
    StartupConfig.Builder builder = StartupConfig.builder();

    boolean conscryptEnabled =
        getValue(Parameter.CONSCRYPT_ENABLED.name()).map(Boolean::parseBoolean).orElse(false);
    builder.setConscryptEnabled(conscryptEnabled);
    builder.setLoggingLevel(getValue(Parameter.LOGGING_LEVEL.name()));

    addNotificationTopics(builder);
    addApplicationIdWorkgroups(builder);
    return builder.build();
  }

  private void addNotificationTopics(StartupConfig.Builder builder) {
    for (ApplicationId applicationId : ApplicationId.values()) {
      if (applicationId == ApplicationId.APPLICATION_ID_UNSPECIFIED) {
        continue;
      }
      Optional<String> notificationTopic =
          getValue(
              Parameter.NOTIFICATION_TOPIC_PREFIX + applicationId.name().toUpperCase(Locale.US));
      notificationTopic
          .filter(topic -> !topic.isEmpty())
          .ifPresent(
              topic ->
                  builder.addNotificationTopic(applicationId.name().toLowerCase(Locale.US), topic));
    }
  }

  private void addApplicationIdWorkgroups(StartupConfig.Builder builder) {
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
