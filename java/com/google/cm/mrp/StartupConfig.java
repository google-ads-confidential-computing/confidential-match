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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;

/** Encapsulates startup config. */
// TODO(b/347031972): Consider moving the existing startup parameters to this class.
@AutoValue
public abstract class StartupConfig {

  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_StartupConfig.Builder()
        .setNotificationTopics(ImmutableMap.of())
        .setLargeJobWorkgroupName("default")
        .setConscryptEnabled(false);
  }

  /**
   * Returns a map of (application id, job completion notification topic, if set for the
   * application)
   */
  public abstract ImmutableMap<String, String> notificationTopics();

  /** Returns the name of the workgroup for large jobs */
  public abstract String largeJobWorkgroupName();

  /** Returns if conscrypt is enabled. */
  public abstract boolean conscryptEnabled();

  /** Returns desired logging level for MRP. */
  public abstract Optional<String> loggingLevel();

  /** Builder for {@link StartupConfig}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Adds a pair of (applicationId, notificationTopic) to the notificationTopics map */
    public final Builder addNotificationTopic(String applicationId, String notificationTopic) {
      notificationTopicsBuilder().put(applicationId, notificationTopic);
      return this;
    }

    /** Set the notificationTopics map */
    public abstract StartupConfig.Builder setNotificationTopics(
        Map<String, String> notificationTopics);

    /** Sets the name of the workgroup for large jobs */
    public abstract Builder setLargeJobWorkgroupName(String largeJobWorkgroupName);

    /** Sets conscrypt enabled. */
    public abstract StartupConfig.Builder setConscryptEnabled(boolean conscryptEnabled);

    /** Sets desired logging level for MRP. */
    public abstract Builder setLoggingLevel(Optional<String> loggingLevel);

    /** Create a new {@link StartupConfig} from the builder. */
    public abstract StartupConfig build();

    /** Builder for notificationTopics map */
    public abstract ImmutableMap.Builder<String, String> notificationTopicsBuilder();
  }
}
