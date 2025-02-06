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

package com.google.cm.mrp.clients.cryptoclient.models;

import com.google.auto.value.AutoValue;
import java.util.Optional;

/** Contains parameters necessary to perform WrappedKey Aead operations with a Cloud KMS provider */
@AutoValue
public abstract class AeadProviderParameters {

  /** Returns a new builder. */
  public static AeadProviderParameters.Builder builder() {
    return new AutoValue_AeadProviderParameters.Builder();
  }

  /** Returns a new instance with {@link GcpParameters} using a given WIP Provider. */
  public static AeadProviderParameters forWipProvider(String wipProvider) {
    return builder()
        .setGcpParameters(GcpParameters.builder().setWipProvider(wipProvider).build())
        .build();
  }

  /**
   * Returns a new instance with {@link GcpParameters} using a given WIP Provider.and
   * ServiceAccountToImpersonate Optional.
   */
  public static AeadProviderParameters forWipProviderAndOptionalServiceAccount(
      String wipProvider, Optional<String> serviceAccountToImpersonate) {
    return builder()
        .setGcpParameters(
            GcpParameters.builder()
                .setWipProvider(wipProvider)
                .setServiceAccountToImpersonate(serviceAccountToImpersonate)
                .build())
        .build();
  }

  /** {@link GcpParameters}. */
  public abstract Optional<AeadProviderParameters.GcpParameters> gcpParameters();

  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets {@link GcpParameters}. */
    public abstract AeadProviderParameters.Builder setGcpParameters(
        AeadProviderParameters.GcpParameters value);

    /** Creates new instance of {@link AeadProviderParameters}. */
    public abstract AeadProviderParameters build();
  }

  @AutoValue
  public abstract static class GcpParameters {

    /** Returns a new builder. */
    public static AeadProviderParameters.GcpParameters.Builder builder() {
      return new AutoValue_AeadProviderParameters_GcpParameters.Builder();
    }

    /** WIP Provider. */
    public abstract String wipProvider();

    /** Optional value for ServiceAccountToImpersonate. */
    public abstract Optional<String> serviceAccountToImpersonate();

    @AutoValue.Builder
    public abstract static class Builder {

      /** Sets WIP Provider. */
      public abstract Builder setWipProvider(String wipProvider);

      /** Sets Optional for ServiceAccountToImpersonate. */
      public abstract Builder setServiceAccountToImpersonate(
          Optional<String> serviceAccountToImpersonate);

      /** Sets value for ServiceAccountToImpersonate. */
      public abstract Builder setServiceAccountToImpersonate(String serviceAccountToImpersonate);

      /** Creates new instance of {@link GcpParameters}. */
      public abstract GcpParameters build();
    }
  }
}
