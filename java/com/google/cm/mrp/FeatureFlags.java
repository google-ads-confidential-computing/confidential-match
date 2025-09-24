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

/** Encapsulates add-on feature flags. */
@AutoValue
public abstract class FeatureFlags {

  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_FeatureFlags.Builder()
        .setEnableMIC(false)
        .setWorkgroupsEnabled(false)
        .setCoordinatorBatchEncryptionEnabled(false)
        .setMaxRecordsPerProtoOutputFile(100000);
  }

  /** Returns the flag for MIC feature. */
  public abstract boolean enableMIC();

  /** Returns the coordinator batch encryption feature. */
  public abstract boolean coordinatorBatchEncryptionEnabled();

  /** Returns the multiple workgroups feature. */
  public abstract boolean workgroupsEnabled();

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

    /** Sets the max records to output for proto files. */
    public abstract Builder setMaxRecordsPerProtoOutputFile(int maxRecordsPerProtoOutputFile);
  }
}
