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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_BLOBSTORE_FILE_ERROR;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.model.BlobMetadata;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class BlobStoreClientTest {

  private static final String INPUT_BUCKET = "input_bucket";
  private static final String INPUT_PREFIX = "input_prefix";
  static final String FOLDER_DELIMITER = "/";

  private static final DataLocation BLOB_STORAGE_DATA_LOCATION =
      DataLocation.ofBlobStoreDataLocation(
          DataLocation.BlobStoreDataLocation.create(INPUT_BUCKET, INPUT_PREFIX + FOLDER_DELIMITER));

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private BlobStorageClient mockBlobStorageClient;
  @Mock private InputStream mockInputStream;

  private BlobStoreClient blobStoreClient;

  @Before
  public void setup() {
    blobStoreClient = new BlobStoreClient(mockBlobStorageClient);
  }

  @Test
  public void isAtLeastSize_prefix_success() throws Exception {
    long size = 1000L;
    var blobs = createBlobs();
    when(mockBlobStorageClient.listBlobs(BLOB_STORAGE_DATA_LOCATION, Optional.empty()))
        .thenReturn(blobs);
    when(mockBlobStorageClient.getBlobMetadata(
            argThat(
                locationArg -> locationArg.blobStoreDataLocation().key().startsWith(INPUT_PREFIX)),
            eq(Optional.empty())))
        .thenReturn(
            BlobMetadata.builder()
                .setSize(600L)
                .setDataLocation(BLOB_STORAGE_DATA_LOCATION)
                .build());

    var result =
        blobStoreClient.isAtLeastSize(size, INPUT_BUCKET, INPUT_PREFIX + "/", Optional.empty());

    assertThat(result).isTrue();
    verify(mockBlobStorageClient).listBlobs(eq(BLOB_STORAGE_DATA_LOCATION), any());
    verify(mockBlobStorageClient, times(2)).getBlobMetadata(any(), any());
  }

  @Test
  public void isAtLeastSize_paths_success() throws Exception {
    long size = 1000L;
    var blobs =
        ImmutableList.of(
            "test0" + FOLDER_DELIMITER + "test.csv", "test1" + FOLDER_DELIMITER + "test1.csv");
    when(mockBlobStorageClient.getBlobMetadata(
            argThat(locationArg -> locationArg.blobStoreDataLocation().key().startsWith("test")),
            eq(Optional.empty())))
        .thenReturn(
            BlobMetadata.builder()
                .setSize(300L)
                .setDataLocation(BLOB_STORAGE_DATA_LOCATION)
                .build());

    var result = blobStoreClient.isAtLeastSize(size, INPUT_BUCKET, blobs, Optional.empty());

    assertThat(result).isFalse();
    verify(mockBlobStorageClient, never()).listBlobs(any(), any());
    verify(mockBlobStorageClient, times(2)).getBlobMetadata(any(), any());
  }

  @Test
  public void isAtLeastSize_getBlobListError_returnsBlobsStoreException() throws Exception {
    var blobs = createBlobs();
    when(mockBlobStorageClient.listBlobs(any(), eq(Optional.empty())))
        .thenThrow(NullPointerException.class);

    var ex =
        assertThrows(
            BlobStoreClientException.class,
            () -> blobStoreClient.isAtLeastSize(1000, INPUT_BUCKET, blobs, Optional.empty()));

    assertThat(ex.getErrorCode()).isEqualTo(INPUT_FILE_READ_ERROR);
    assertThat(ex.getCause().getClass()).isEqualTo(NullPointerException.class);
  }

  @Test
  public void getSchema_whenStoragePermissionEx_returnsBlobException() throws Exception {
    var details = new GoogleJsonError();
    details.setCode(403);
    var exception =
        new StorageException(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(403, "error", new HttpHeaders()), details));
    when(mockBlobStorageClient.listBlobs(BLOB_STORAGE_DATA_LOCATION, Optional.empty()))
        .thenThrow(exception);

    var ex =
        assertThrows(
            BlobStoreClientException.class,
            () ->
                blobStoreClient.isAtLeastSize(1000, INPUT_BUCKET, INPUT_PREFIX, Optional.empty()));

    assertThat(ex.getErrorCode()).isEqualTo(BLOBSTORE_PERMISSIONS_ERROR);
  }

  @Test
  public void getSchema_whenBlobMissing_returnsBlobException() throws Exception {
    var blobs = createBlobs();
    when(mockBlobStorageClient.listBlobs(BLOB_STORAGE_DATA_LOCATION, Optional.empty()))
        .thenReturn(blobs);
    var details = new GoogleJsonError();
    details.setCode(404);
    var exception =
        new StorageException(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(404, "error", new HttpHeaders()), details));
    when(mockBlobStorageClient.getBlobMetadata(
            argThat(
                locationArg -> locationArg.blobStoreDataLocation().key().startsWith(INPUT_PREFIX)),
            eq(Optional.empty())))
        .thenThrow(exception);

    var ex =
        assertThrows(
            BlobStoreClientException.class,
            () ->
                blobStoreClient.isAtLeastSize(1000, INPUT_BUCKET, INPUT_PREFIX, Optional.empty()));

    assertThat(ex.getErrorCode()).isEqualTo(MISSING_BLOBSTORE_FILE_ERROR);
  }

  @Test
  public void getSchema_whenOtherStorageException_returnsBlobException() throws Exception {
    var blobs = createBlobs();
    when(mockBlobStorageClient.listBlobs(BLOB_STORAGE_DATA_LOCATION, Optional.empty()))
        .thenReturn(blobs);
    var exception = new StorageException(new IOException(new SocketTimeoutException()));
    when(mockBlobStorageClient.getBlobMetadata(
            argThat(
                locationArg -> locationArg.blobStoreDataLocation().key().startsWith(INPUT_PREFIX)),
            eq(Optional.empty())))
        .thenThrow(exception);

    var ex =
        assertThrows(
            BlobStoreClientException.class,
            () ->
                blobStoreClient.isAtLeastSize(1000, INPUT_BUCKET, INPUT_PREFIX, Optional.empty()));

    assertThat(ex.getErrorCode()).isEqualTo(INPUT_FILE_READ_ERROR);
  }

  private ImmutableList<String> createBlobs() {
    String filePrefix = INPUT_PREFIX + FOLDER_DELIMITER + "test.csv";
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    for (int i = 0; i < 2; ++i) {
      listBuilder.add(filePrefix + "-" + i + "-of-" + 2);
    }
    return listBuilder.build();
  }
}
