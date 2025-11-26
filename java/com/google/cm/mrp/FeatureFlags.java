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
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;

/** Encapsulates add-on feature flags. */
@AutoValue
public abstract class FeatureFlags {

  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_FeatureFlags.Builder()
        .setEnableMIC(false)
        .setWorkgroupsEnabled(false)
        .setApplicationIdWorkgroups(ImmutableMap.of())
        .setLargeJobApplicationIds(ImmutableSet.of())
        .setLargeJobThresholdBytes(1000000)
        .setCoordinatorBatchEncryptionEnabled(false)
        .setMaxRecordsPerProtoOutputFile(100000)
        .setProtoPassthroughMetadataEnabled(false);
  }

  /** Returns the flag for MIC feature. */
  public abstract boolean enableMIC();

  /** Returns the coordinator batch encryption feature. */
  public abstract boolean coordinatorBatchEncryptionEnabled();

  /** Returns the multiple workgroups feature. */
  public abstract boolean workgroupsEnabled();

  /** Returns the metadata passthrough feature flag for proto format. */
  public abstract boolean protoPassthroughMetadataEnabled();

  /** Returns a mapping of: applicationId -> workgroupName, if set for the application */
  public abstract ImmutableMap<String, String> applicationIdWorkgroups();

  /** Returns which applicationIds to consider for allocation to the large job workgroup. */
  public abstract ImmutableSet<String> largeJobApplicationIds();

  /** Returns the threshold in bytes above which jobs are allocated as "large". */
  public abstract long largeJobThresholdBytes();

  /** Returns the max records to output for proto files. */
  public abstract int maxRecordsPerProtoOutputFile();

  /** Builder for {@link FeatureFlags}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Creates a new {@link FeatureFlags} from the builder. */
    public abstract FeatureFlags build();

    /** Sets MIC feature flag. */
    public abstract Builder setEnableMIC(boolean enableMIC);

    /** Sets the coordinator batch encryption feature flag. */
    public abstract Builder setCoordinatorBatchEncryptionEnabled(
        boolean coordinatorBatchEncryptionEnabled);

    /** Sets the multiple workgroups feature flag. */
    public abstract Builder setWorkgroupsEnabled(boolean workgroupsEnabled);

    /** Sets the metadata passthrough feature flag for proto format. */
    public abstract Builder setProtoPassthroughMetadataEnabled(boolean protoPassthroughMetadataEnabled);

    /** Set the applicationIdWorkgroups map */
    public abstract Builder setApplicationIdWorkgroups(Map<String, String> applicationIdWorkgroups);

    /** Adds a pair of (applicationId, workgroupName) to the applicationIdWorkgroups map */
    public final Builder addWorkgroupApplicationId(String applicationId, String workgroupName) {
      applicationIdWorkgroupsBuilder().put(applicationId, workgroupName);
      return this;
    }

    /** Sets which applicationIds to consider for allocation to the large job workgroup. */
    public abstract Builder setLargeJobApplicationIds(Set<String> largeJobApplicationIds);

    /** Adds an applicationId to the large job workgroup set. */
    public final Builder addLargeJobApplicationId(String applicationId) {
      largeJobApplicationIdsBuilder().add(applicationId);
      return this;
    }

    /** Sets the threshold in bytes above which jobs are allocated as "large". */
    public abstract Builder setLargeJobThresholdBytes(long largeJobBytesThreshold);

    /** Sets the max records to output for proto files. */
    public abstract Builder setMaxRecordsPerProtoOutputFile(int maxRecordsPerProtoOutputFile);

    /** Builder for applicationIdWorkgroups map */
    public abstract ImmutableMap.Builder<String, String> applicationIdWorkgroupsBuilder();

    /** Builder for largeJobApplicationIds set */
    public abstract ImmutableSet.Builder<String> largeJobApplicationIdsBuilder();
  }
}
