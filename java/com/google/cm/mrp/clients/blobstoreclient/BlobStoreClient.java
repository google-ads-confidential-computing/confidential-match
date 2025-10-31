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

package com.google.cm.mrp.clients.blobstoreclient;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.BLOBSTORE_PERMISSIONS_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_LIST_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_BLOBSTORE_FILE_ERROR;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient.BlobStorageClientException;
import com.google.scp.operator.cpio.blobstorageclient.model.BlobMetadata;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation.BlobStoreDataLocation;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Internal abstraction of BlobStorageClient to condense blob store operations across MRP */
public final class BlobStoreClient {
  private static final Logger logger = LoggerFactory.getLogger(BlobStoreClient.class);

  static final String FOLDER_DELIMITER = "/";

  private final BlobStorageClient blobStorageClient;

  @Inject
  BlobStoreClient(BlobStorageClient blobStorageClient) {
    this.blobStorageClient = blobStorageClient;
  }

  /**
   * Returns if the contents in a given bucket/prefix have a size equal to or greater than the given
   * size parameter.
   */
  public boolean isAtLeastSize(
      long size, String bucketName, String prefix, Optional<String> accountIdentity)
      throws BlobStoreClientException {
    ImmutableList<String> blobs =
        getBlobList(bucketName, prefix, accountIdentity, /* suffixToExclude= */ Optional.empty());
    return isAtLeastSize(size, bucketName, blobs, accountIdentity);
  }

  /**
   * Returns if the contents in blobPaths in the given bucket have a size equal to or greater than
   * the given size parameter.
   */
  public boolean isAtLeastSize(
      long size, String bucketName, List<String> blobPaths, Optional<String> accountIdentity)
      throws BlobStoreClientException {
    long curSize = 0;
    for (String blob : blobPaths) {
      DataLocation blobLocation = getLocation(bucketName, blob);
      try {
        BlobMetadata metadata = blobStorageClient.getBlobMetadata(blobLocation, accountIdentity);
        curSize += metadata.size();
        if (curSize >= size) {
          return true;
        }
      } catch (StorageException ex) {
        throw convertToBlobStoreException(ex);
      } catch (BlobStorageClientException | RuntimeException e) {
        String message = "Unable to get Blob metadata";
        logger.error(message, e);
        throw new BlobStoreClientException(message, e, INPUT_FILE_READ_ERROR);
      }
    }
    return false;
  }

  private ImmutableList<String> getBlobList(
      String bucketName,
      String prefix,
      Optional<String> accountIdentity,
      Optional<String> suffixToExclude)
      throws BlobStoreClientException {
    try {
      String prefixWithDelimiter =
          prefix.endsWith(FOLDER_DELIMITER) ? prefix : prefix + FOLDER_DELIMITER;
      DataLocation blobsLocation = getLocation(bucketName, prefixWithDelimiter);
      ImmutableList<String> list =
          blobStorageClient.listBlobs(blobsLocation, accountIdentity).stream()
              .filter(blob -> !blob.endsWith(FOLDER_DELIMITER))
              .filter(
                  blob -> suffixToExclude.map(s -> !blob.toLowerCase().endsWith(s)).orElse(true))
              .collect(ImmutableList.toImmutableList());
      logger.info("BlobList: {}", list.toString());
      return list;
    } catch (StorageException ex) {
      throw convertToBlobStoreException(ex);
    } catch (BlobStorageClientException | RuntimeException e) {
      String message = "Unable to list input files";
      logger.error(message, e);
      throw new BlobStoreClientException(message, e, INPUT_FILE_LIST_READ_ERROR);
    }
  }

  private DataLocation getLocation(String bucket, String key) {
    return DataLocation.ofBlobStoreDataLocation(BlobStoreDataLocation.create(bucket, key));
  }

  private BlobStoreClientException convertToBlobStoreException(StorageException ex) {
    if (ex.getCause() != null && ex.getCause() instanceof GoogleJsonResponseException) {
      int errorCode = ((GoogleJsonResponseException) ex.getCause()).getDetails().getCode();
      if (errorCode == 403) {
        String message = "Missing permission to open blob store file.";
        logger.warn(message, ex);
        return new BlobStoreClientException(message, ex, BLOBSTORE_PERMISSIONS_ERROR);
      } else if (errorCode == 404) {
        String message = "Unable to open the blob store file";
        logger.error(message, ex);
        return new BlobStoreClientException(message, ex, MISSING_BLOBSTORE_FILE_ERROR);
      }
    }
    String message = "Unable to read the blob store file. Will retry.";
    logger.info(message, ex);
    return new BlobStoreClientException(message, ex, INPUT_FILE_READ_ERROR);
  }
}
