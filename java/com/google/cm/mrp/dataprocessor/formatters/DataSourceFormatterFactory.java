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

package com.google.cm.mrp.dataprocessor.formatters;

import com.google.cm.mrp.FeatureFlags;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;

/** Factory interface for {@link DataSourceFormatter}. */
public interface DataSourceFormatterFactory {

  /** Factory method for creating {@link DataSourceFormatter} objects. */
  DataSourceFormatter create(
      MatchConfig matchConfig,
      Schema inputSchema,
      FeatureFlags featureFlags);

  /** Factory method for creating {@link DataSourceFormatter} objects. */
  DataSourceFormatter create(
      MatchConfig matchConfig,
      Schema inputSchema,
      FeatureFlags featureFlags,
      EncryptionMetadata encryptionMetadata,
      CryptoClient cryptoClient);
}
