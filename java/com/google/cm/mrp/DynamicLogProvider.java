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

import com.google.cm.mrp.Constants.CustomLogLevel;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gets logging configs to set log levels from a parameter at runtime, rather than the properties
 * file.
 */
public final class DynamicLogProvider {

  private static final Logger logger = LoggerFactory.getLogger(DynamicLogProvider.class);

  // These should correspond exactly with the Logging backend
  private final Set<String> DEFAULT_LEVELS = ImmutableSet.of("DEBUG", "INFO", "WARN", "ERROR");

  private final Set<String> CUSTOM_LEVELS =
      Arrays.stream(CustomLogLevel.values()).map(Enum::name).collect(ImmutableSet.toImmutableSet());

  private final StartupConfigProvider startupConfigProvider;

  @Inject
  DynamicLogProvider(StartupConfigProvider startupConfigProvider) {
    this.startupConfigProvider = startupConfigProvider;
  }

  /** Checks if there is a log to set from parameters and sets it for the entire application. */
  public void getAndSetDynamicLogLevel() {
    Optional<String> logLevelParam = startupConfigProvider.getStartupConfig().loggingLevel();

    if (logLevelParam.isPresent()) {
      String proposedLevel = logLevelParam.get();
      if (DEFAULT_LEVELS.contains(proposedLevel)) {
        setLogLevel(proposedLevel);
      } else if (CUSTOM_LEVELS.contains(proposedLevel)) {
        CustomLogLevel level = CustomLogLevel.valueOf(proposedLevel);
        // Register level
        Level.forName(proposedLevel, level.value);

        setLogLevel(proposedLevel);
      } else {
        logger.warn(
            "Log level {} config does not match any of the default levels: {} . Or custom"
                + " levels: {}",
            proposedLevel,
            DEFAULT_LEVELS,
            CUSTOM_LEVELS);
      }
    }
  }

  private void setLogLevel(String logLevel) {
    logger.info("Setting MRP log level to: {}", logLevel);
    Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.getLevel(logLevel));
  }
}
