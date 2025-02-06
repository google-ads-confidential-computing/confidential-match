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
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner.DataLocation;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import java.util.Optional;

/** Factory interface for {@link StreamDataSource}. */
public interface StreamDataSourceFactory {
  /** Factory method for constructing {@link StreamDataSource} objects. */
  StreamDataSource create(
      DataLocation dataLocation,
      MatchConfig matchConfig,
      Optional<String> dataOwnerIdentity,
      FeatureFlags featureFlags);

  /** Factory method for constructing {@link StreamDataSource} objects with encryption. */
  StreamDataSource create(
      DataLocation dataLocation,
      MatchConfig matchConfig,
      Optional<String> dataOwnerIdentity,
      FeatureFlags featureFlags,
      EncryptionMetadata encryptionMetadata,
      CryptoClient cryptoClient);
}
