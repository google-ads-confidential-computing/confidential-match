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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a method for selecting the match and output configuration based on application ID. Each
 * application has a specific hard-coded configuration that will be included with MRP.
 */
public class MatchConfigProvider {
  private static final Logger logger = LoggerFactory.getLogger(MatchConfigProvider.class);

  /** Returns a configuration object for a given application ID */
  public static MatchConfig getMatchConfig(String application_id) {
    try {
      return ProtoUtils.getProtoFromJson(
          Resources.toString(
              Objects.requireNonNull(
                  MatchConfigProvider.class.getResource(
                      "configs/" + application_id + "_config.json")),
              UTF_8),
          MatchConfig.class);
    } catch (Exception e) {
      String message =
          String.format("Failed to get MatchConfig for application %s", application_id);
      logger.error(message);
      throw new JobProcessorException(message, e, INVALID_PARAMETERS);
    }
  }
}
