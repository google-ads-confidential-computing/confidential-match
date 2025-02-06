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

import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.LookupDataSourceResult;
import java.util.Optional;

/**
 * Interface representing lookup data sources that can be passed a {@link DataChunk} for lookup.
 * Lookup data sources are backed by a lookup service.
 *
 * <p>Lookup service based lookup data sources will use a {@link LookupServiceClient} to contact the
 * service on an endpoint.
 */
public interface LookupDataSource {

  /**
   * This method returns the lookup results after retrieving data from the underlying {@link
   * LookupDataSource}.
   *
   * @return {@link DataChunk}
   */
  LookupDataSourceResult lookup(
      DataChunk dataChunk, Optional<EncryptionMetadata> encryptionMetadata)
      throws LookupServiceClient.LookupServiceClientException;
}
