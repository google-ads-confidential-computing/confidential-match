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

package com.google.cm.mrp.dataprocessor;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import java.util.Optional;

/** Main data processing interface for dependency injection. */
public interface DataProcessor {

  /**
   * An abstract method that takes a list of data owner configuration items, performs a match, and
   * writes the output to an output bucket.
   */
  MatchStatistics process(
      FeatureFlags featureFlags,
      DataOwnerList dataOwnerList,
      String outputBucket,
      String outputPrefix,
      String jobRequestId,
      MatchConfig matchConfig,
      Optional<EncryptionMetadata> encryptionMetadata,
      Optional<String> dataOwnerIdentity)
      throws JobProcessorException;
}
