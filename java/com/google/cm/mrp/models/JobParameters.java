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

package com.google.cm.mrp.models;

import com.google.auto.value.AutoValue;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.ModeProto.Mode;
import java.util.List;
import java.util.Optional;

@AutoValue
public abstract class JobParameters {
  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_JobParameters.Builder().setMode(Mode.REDACT);
  }

  /** Returns jobId for current job */
  public abstract String jobId();

  /** Returns dataLocation for the data sources */
  // TODO(b/406643831): replace with autoValue
  public abstract DataLocation dataLocation();

  /** Returns identity for permissions to use to read data sources in `dataOwnerList` */
  public abstract Optional<String> dataOwnerIdentity();

  /** Returns encryptionMetadata to use to decrypt encrypted data */
  public abstract Optional<EncryptionMetadata> encryptionMetadata();

  /** Returns encodingType to use to decode data if needed */
  public abstract Optional<EncodingType> encodingType();

  /** Returns mode to use for data matching operations */
  public abstract Mode mode();

  /** Returns outputDataLocation where to write outputs */
  public abstract OutputDataLocation outputDataLocation();

  /** Builder for {@link JobParameters}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets jobId */
    public abstract Builder setJobId(String jobId);

    /** Sets dataLocation */
    public abstract Builder setDataLocation(DataLocation dataLocation);

    /** Sets encodingType */
    public abstract Builder setEncodingType(EncodingType encodingType);

    /** Sets mode */
    public abstract Builder setMode(Mode mode);

    /** Sets outputDataLocation */
    public abstract Builder setOutputDataLocation(OutputDataLocation outputDataLocation);

    /** Sets dataOwnerIdentity */
    public abstract Builder setDataOwnerIdentity(Optional<String> dataOwnerIdentity);

    /** Sets encryptionMetadata optional */
    public abstract Builder setEncryptionMetadata(Optional<EncryptionMetadata> encryptionMetadata);

    /** Sets encryptionMetadata */
    public abstract Builder setEncryptionMetadata(EncryptionMetadata encryptionMetadata);

    /** Creates a new {@link JobParameters} from the builder. */
    public abstract JobParameters build();
  }

  @AutoValue
  public abstract static class DataOwnerListParameter {

    /** Returns a new builder. */
    public static Builder builder() {
      return new AutoValue_JobParameters_DataOwnerListParameter.Builder();
    }

    /** Returns inputDataBlobPrefix */
    public abstract String inputDataBlobPrefix();

    /** Returns inputDataBucketName */
    public abstract String inputDataBucketName();

    /** Returns if data should be read as a stream */
    public abstract boolean isStreamed();

    /** Returns inputDataBlobPathsList if data is in different locations */
    public abstract List<String> inputDataBlobPathsList();

    /** Returns inputSchemaPath if not using default schema location */
    public abstract String inputSchemaPath();

    @AutoValue.Builder
    public abstract static class Builder {

      /** Sets inputDataBlobPrefix */
      public abstract Builder setInputDataBlobPrefix(String inputDataBlobPrefix);

      /** Sets inputDataBucketName */
      public abstract Builder setInputDataBucketName(String inputDataBucketName);

      /** Sets isStreamed */
      public abstract Builder setIsStreamed(boolean isStreamed);

      /** Sets inputDataBlobPathsList */
      public abstract Builder setInputDataBlobPathsList(List<String> inputDataBlobPathsList);

      /** Sets inputSchemaPath */
      public abstract Builder setInputSchemaPath(String inputSchemaPath);

      /** Creates a new {@link DataOwnerListParameter} from the builder. */
      public abstract DataOwnerListParameter build();
    }
  }

  @AutoValue
  public abstract static class OutputDataLocation {

    /** Returns a new builder. */
    public static Builder builder() {
      return new AutoValue_JobParameters_OutputDataLocation.Builder();
    }

    /** Returns a new instance for a bucket name and prefix. */
    public static OutputDataLocation forNameAndPrefix(
        String outputBucketName, String outputBlobPrefix) {
      return OutputDataLocation.builder()
          .setOutputDataBucketName(outputBucketName)
          .setOutputDataBlobPrefix(outputBlobPrefix)
          .build();
    }

    /** Returns outputDataBucketName */
    public abstract String outputDataBucketName();

    /** Returns outputDataBlobPrefix */
    public abstract String outputDataBlobPrefix();

    @AutoValue.Builder
    public abstract static class Builder {

      /** Sets outputDataBucketName */
      public abstract Builder setOutputDataBucketName(String outputDataBucketName);

      /** Sets outputDataBlobPrefix */
      public abstract Builder setOutputDataBlobPrefix(String outputDataBlobPrefix);

      /** Creates a new {@link OutputDataLocation} from the builder. */
      public abstract OutputDataLocation build();
    }
  }
}
