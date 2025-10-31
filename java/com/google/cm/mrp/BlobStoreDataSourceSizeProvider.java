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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DATA_SOURCE_SIZE_PROVIDER_ERROR;

import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.clients.blobstoreclient.BlobStoreClient;
import com.google.cm.mrp.clients.blobstoreclient.BlobStoreClientException;
import com.google.inject.Inject;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides size of a data source in blob storage. TODO(b/376719449): refactor to separate package
 */
public class BlobStoreDataSourceSizeProvider implements DataSourceSizeProvider {

  private static final Logger logger =
      LoggerFactory.getLogger(BlobStoreDataSourceSizeProvider.class);

  private final BlobStoreClient blobStoreClient;

  @Inject
  BlobStoreDataSourceSizeProvider(BlobStoreClient blobStoreClient) {
    this.blobStoreClient = blobStoreClient;
  }

  /** Determines whether any one data source is at least a given size in bytes */
  @Override
  public boolean isAtLeastSize(
      long bytesSize, DataOwnerList dataSourceList, Optional<String> accountIdentity) {
    for (DataOwner owner : dataSourceList.getDataOwnersList()) {
      if (owner.hasLookupEndpoint()) {
        return false;
      }
      boolean isAtLeastSize;
      var dataLocation = owner.getDataLocation();
      String inputBucket = dataLocation.getInputDataBucketName();
      try {
        if (owner.getDataLocation().getInputDataBlobPathsCount() > 0) {
          var blobPaths = dataLocation.getInputDataBlobPathsList();
          isAtLeastSize =
              blobStoreClient.isAtLeastSize(bytesSize, inputBucket, blobPaths, accountIdentity);
        } else {
          String inputPrefix = dataLocation.getInputDataBlobPrefix();
          isAtLeastSize =
              blobStoreClient.isAtLeastSize(bytesSize, inputBucket, inputPrefix, accountIdentity);
        }
      } catch (BlobStoreClientException e) {
        String msg = "Cloud not read blob store data to determine size.";
        logger.warn(msg, e);
        throw new JobProcessorException(msg, e, e.getErrorCode());
      } catch (RuntimeException e) {
        String msg = "Failed when sending request to blobStoreClient.";
        logger.warn(msg, e);
        throw new JobProcessorException(msg, e, DATA_SOURCE_SIZE_PROVIDER_ERROR);
      }

      // if threshold was passed, then return. Else try next owner
      if (isAtLeastSize) {
        return true;
      }
    }
    return false;
  }
}
