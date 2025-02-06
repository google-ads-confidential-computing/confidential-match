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

package com.google.cm.mrp.dataprocessor.destinations;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DestinationInfoProto.DestinationInfo;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient.BlobStorageClientException;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation.BlobStoreDataLocation;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import com.google.scp.operator.cpio.metricclient.MetricClient.MetricClientException;
import com.google.scp.operator.cpio.metricclient.model.CustomMetric;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of a blob store data destination that can be used to upload processed
 * data files by the DataProcessor.
 */
public final class BlobStoreDataDestination implements DataDestination {

  private static final Logger logger = LoggerFactory.getLogger(BlobStoreDataDestination.class);

  private final BlobStorageClient blobStorageClient;

  private final MetricClient metricClient;
  private final String outputBucket;
  private final String outputPrefix;
  private final Optional<String> dataOwnerIdentity;
  private boolean existingBLobsDeleted;

  /** Constructor for {@link BlobStoreDataDestination}. */
  @Inject
  public BlobStoreDataDestination(
      BlobStorageClient blobStorageClient,
      MetricClient metricClient,
      @Assisted DestinationInfo destinationInfo) {
    this.blobStorageClient = blobStorageClient;
    this.metricClient = metricClient;
    this.outputBucket = destinationInfo.getGcsDestination().getOutputBucket();
    this.outputPrefix = destinationInfo.getGcsDestination().getOutputPrefix();
    this.dataOwnerIdentity =
        destinationInfo.getGcsDestination().hasDataOwnerIdentity()
            ? Optional.of(destinationInfo.getGcsDestination().getDataOwnerIdentity())
            : Optional.empty();
    existingBLobsDeleted = false;
  }

  /*
   * This is needed in case of a job retry. Previously created objects should be deleted
   * to avoid jumbling blobs from the previous execution and the current one.
   */
  private void deleteExistingBlobs() {
    try {
      blobStorageClient
          .listBlobs(getLocation(outputBucket, outputPrefix), dataOwnerIdentity)
          .forEach(
              blob -> {
                try {
                  blobStorageClient.deleteBlob(
                      getLocation(outputBucket, outputPrefix + File.separator + blob),
                      dataOwnerIdentity);
                } catch (BlobStorageClientException e) {
                  String message = "Unable to delete output files";
                  logger.error(message, e);
                  throw new JobProcessorException(
                      message, e, JobResultCode.OUTPUT_FILE_DELETE_ERROR);
                }
              });
    } catch (BlobStorageClientException e) {
      String message = "Unable to list output files";
      logger.error(message, e);
      throw new JobProcessorException(message, e, JobResultCode.OUTPUT_FILE_LIST_ERROR);
    }
  }

  private DataLocation getLocation(String bucket, String key) {
    return DataLocation.ofBlobStoreDataLocation(BlobStoreDataLocation.create(bucket, key));
  }

  /** {@inheritDoc} */
  @Override
  public void write(File file, String name) throws IOException {

    /*
     * Delete existing blobs on the first call write()
     * Synchronized block is needed to prevent a race condition, when two threads are trying to
     * delete blobs simultaneously, or one thread is trying to upload a blob, while the other one
     * is trying to delete existing blobs.
     */
    synchronized (this) {
      if (!existingBLobsDeleted) {
        deleteExistingBlobs();
      }
      existingBLobsDeleted = true;
    }

    String blobName = outputPrefix + File.separator + name;
    DataLocation dataLocation =
        DataLocation.ofBlobStoreDataLocation(
            DataLocation.BlobStoreDataLocation.create(
                outputBucket, blobName));

    logger.info("Next blob to write: {}", blobName);
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      blobStorageClient.putBlob(dataLocation, file.toPath(), dataOwnerIdentity);
      CustomMetric metric =
          CustomMetric.builder()
              .setNameSpace("cfm/mrp")
              .setName("gcswriteduration")
              .setValue(stopwatch.elapsed(MILLISECONDS))
              .setUnit("Milliseconds")
              .addLabel("blob", blobName)
              .build();
      try {
        logger.trace("Writing metric: {}", metric);
        metricClient.recordMetric(metric);
      } catch (MetricClientException e) {
        logger.warn("Unable to record gcswriteduration metric", e);
      }
    } catch (BlobStorageClientException e) {
      String message = "BlobStoreDataDestination threw an exception while uploading the file.";
      logger.error(message);
      throw new JobProcessorException(message, e, JobResultCode.OUTPUT_FILE_UPLOAD_ERROR);
    }
  }
}
