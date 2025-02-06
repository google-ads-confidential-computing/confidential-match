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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DestinationInfoProto.DestinationInfo;
import com.google.cm.mrp.backend.DestinationInfoProto.DestinationInfo.GcsDestination;
import com.google.common.collect.ImmutableList;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient.BlobStorageClientException;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation.BlobStoreDataLocation;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import java.io.File;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class BlobStoreDataDestinationTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private BlobStorageClient mockBlobStorageClient;
  @Mock private MetricClient mockMetricClient;
  @Captor private ArgumentCaptor<DataLocation> dataLocationCaptor;
  @Captor private ArgumentCaptor<Optional<String>> accountIdentityCaptor;
  private DataDestination dataDestination;
  private static final String OUTPUT_BUCKET = "output_bucket";
  private static final String OUTPUT_PREFIX = "output_prefix";

  @Before
  public void setUp() throws Exception {
    when(mockBlobStorageClient.listBlobs(
            DataLocation.ofBlobStoreDataLocation(
                BlobStoreDataLocation.create(OUTPUT_BUCKET, OUTPUT_PREFIX)),
            Optional.empty()))
        .thenReturn(ImmutableList.of("test.csv"));
    dataDestination =
        new BlobStoreDataDestination(
            mockBlobStorageClient,
            mockMetricClient,
            DestinationInfo.newBuilder()
                .setGcsDestination(
                    GcsDestination.newBuilder()
                        .setOutputBucket(OUTPUT_BUCKET)
                        .setOutputPrefix(OUTPUT_PREFIX))
                .build());
  }

  @Test
  public void upload_whenClientPutsThenSucceeds() throws Exception {
    File file = new File("test.csv");

    dataDestination.write(file, "test.csv");

    verify(mockBlobStorageClient)
        .listBlobs(dataLocationCaptor.capture(), accountIdentityCaptor.capture());
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().bucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().key())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(accountIdentityCaptor.getValue()).isEqualTo(Optional.empty());
    verify(mockBlobStorageClient)
        .deleteBlob(dataLocationCaptor.capture(), accountIdentityCaptor.capture());
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().bucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().key())
        .isEqualTo(OUTPUT_PREFIX + File.separator + "test.csv");
    assertThat(accountIdentityCaptor.getValue()).isEqualTo(Optional.empty());
    verify(mockBlobStorageClient)
        .putBlob(dataLocationCaptor.capture(), eq(file.toPath()), eq(Optional.empty()));
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().bucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().key())
        .isEqualTo(OUTPUT_PREFIX + File.separator + "test.csv");
    verifyNoMoreInteractions(mockBlobStorageClient);
  }

  @Test
  public void upload_whenClientThrowsThenThrows() throws Exception {
    File file = new File("test.csv");
    DataLocation dataLocation =
        DataLocation.ofBlobStoreDataLocation(
            DataLocation.BlobStoreDataLocation.create(
                OUTPUT_BUCKET, OUTPUT_PREFIX + File.separator + "test.csv"));

    doThrow(BlobStorageClientException.class)
        .when(mockBlobStorageClient)
        .putBlob(dataLocation, file.toPath(), Optional.empty());

    JobProcessorException ex =
        assertThrows(JobProcessorException.class, () -> dataDestination.write(file, "test.csv"));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("BlobStoreDataDestination threw an exception while uploading the file.");
    assertThat(ex).hasCauseThat().isInstanceOf(BlobStorageClientException.class);
    verify(mockBlobStorageClient)
        .listBlobs(dataLocationCaptor.capture(), accountIdentityCaptor.capture());
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().bucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().key())
        .isEqualTo(OUTPUT_PREFIX);
    assertThat(accountIdentityCaptor.getValue()).isEqualTo(Optional.empty());
    verify(mockBlobStorageClient)
        .deleteBlob(dataLocationCaptor.capture(), accountIdentityCaptor.capture());
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().bucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().key())
        .isEqualTo(OUTPUT_PREFIX + File.separator + "test.csv");
    assertThat(accountIdentityCaptor.getValue()).isEqualTo(Optional.empty());
    verify(mockBlobStorageClient)
        .putBlob(dataLocationCaptor.capture(), eq(file.toPath()), eq(Optional.empty()));
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().bucket())
        .isEqualTo(OUTPUT_BUCKET);
    assertThat(dataLocationCaptor.getValue().blobStoreDataLocation().key())
        .isEqualTo(OUTPUT_PREFIX + File.separator + "test.csv");
    verifyNoMoreInteractions(mockBlobStorageClient);
  }
}
