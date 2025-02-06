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

package com.google.cm.mrp.clients.attestation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import java.util.List;

/** Autovalue representation of the request sent to fetch a Confidential Space Token */
@AutoValue
@JsonDeserialize(builder = ConfidentialSpaceTokenRequest.Builder.class)
@JsonSerialize(as = ConfidentialSpaceTokenRequest.class)
public abstract class ConfidentialSpaceTokenRequest {

  /** Returns a new builder. */
  public static Builder builder() {
    return Builder.builder();
  }

  /** Returns a new request with an audience, tokenType, and a list of key IDs. */
  public static ConfidentialSpaceTokenRequest forAudienceTokenTypeAndKeyIds(
      String audience, String tokenType, List<String> keyIds) {
    return Builder.builder()
        .setAudience(audience)
        .setTokenType(tokenType)
        .setTokenTypeOptions(
            TokenTypeOptions.builder()
                .setAllowedPrincipalTags(
                    AllowedPrincipalTags.builder()
                        .setContainerImageSignatures(
                            ContainerImageSignatures.builder().setKeyIds(keyIds).build())
                        .build())
                .build())
        .build();
  }

  /** Audience. */
  @JsonProperty("audience")
  public abstract String audience();

  /** Token type. */
  @JsonProperty("token_type")
  public abstract String tokenType();

  /** Token type options. */
  @JsonProperty("token_type_options")
  public abstract TokenTypeOptions tokenTypeOptions();

  /** Builder class */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Returns a new instance of the builder. */
    @JsonCreator
    public static Builder builder() {
      return new AutoValue_ConfidentialSpaceTokenRequest.Builder();
    }

    /** Set audience. */
    @JsonProperty("audience")
    public abstract Builder setAudience(String audience);

    /** Set token type. */
    @JsonProperty("token_type")
    public abstract Builder setTokenType(String tokenType);

    /** Token type options. */
    @JsonProperty("token_type_options")
    public abstract Builder setTokenTypeOptions(TokenTypeOptions tokenTypeOptions);

    /** Constructs new instance. */
    public abstract ConfidentialSpaceTokenRequest build();
  }

  @AutoValue
  @JsonDeserialize(builder = TokenTypeOptions.Builder.class)
  @JsonSerialize(as = TokenTypeOptions.class)
  public abstract static class TokenTypeOptions {

    /** Returns a new builder. */
    public static Builder builder() {
      return Builder.builder();
    }

    /** Allowed principal tags. */
    @JsonProperty("allowed_principal_tags")
    public abstract AllowedPrincipalTags allowedPrincipalTags();

    /** Builder class */
    @AutoValue.Builder
    public abstract static class Builder {

      /** Returns a new instance of the builder. */
      @JsonCreator
      public static Builder builder() {
        return new AutoValue_ConfidentialSpaceTokenRequest_TokenTypeOptions.Builder();
      }

      /** Sets allowed principal tags. */
      @JsonProperty("allowed_principal_tags")
      public abstract Builder setAllowedPrincipalTags(AllowedPrincipalTags allowedPrincipalTags);

      /** Constructs new instance. */
      public abstract TokenTypeOptions build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AllowedPrincipalTags.Builder.class)
  @JsonSerialize(as = AllowedPrincipalTags.class)
  public abstract static class AllowedPrincipalTags {

    /** Returns a new builder. */
    public static Builder builder() {
      return Builder.builder();
    }

    /** Container image signatures. */
    @JsonProperty("container_image_signatures")
    public abstract ContainerImageSignatures containerImageSignatures();

    /** Builder class */
    @AutoValue.Builder
    public abstract static class Builder {

      /** Returns a new instance of the builder. */
      @JsonCreator
      public static Builder builder() {
        return new AutoValue_ConfidentialSpaceTokenRequest_AllowedPrincipalTags.Builder();
      }

      /** Sets container image signatures. */
      @JsonProperty("container_image_signatures")
      public abstract Builder setContainerImageSignatures(
          ContainerImageSignatures containerImageSignatures);

      /** Constructs new instance. */
      public abstract AllowedPrincipalTags build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = ContainerImageSignatures.Builder.class)
  @JsonSerialize(as = ContainerImageSignatures.class)
  public abstract static class ContainerImageSignatures {

    /** Returns a new builder. */
    public static Builder builder() {
      return Builder.builder();
    }

    /** Key Ids */
    @JsonProperty("key_ids")
    public abstract List<String> keyIds();

    /** Builder class */
    @AutoValue.Builder
    public abstract static class Builder {

      /** Returns a new instance of the builder. */
      @JsonCreator
      public static Builder builder() {
        return new AutoValue_ConfidentialSpaceTokenRequest_ContainerImageSignatures.Builder();
      }

      /** Sets key Ids */
      @JsonProperty("key_ids")
      public abstract Builder setKeyIds(List<String> keyIds);

      /** Constructs new instance. */
      public abstract ContainerImageSignatures build();
    }
  }
}
