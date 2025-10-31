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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_BLOBSTORE_FILE_ERROR;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import com.google.cm.mrp.clients.blobstoreclient.BlobStoreClient;
import com.google.cm.mrp.clients.blobstoreclient.BlobStoreClientException;
import com.google.common.collect.ImmutableList;
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
public class BlobStoreDataSourceSizeProviderTest {

  private static final String INPUT_BUCKET = "input_bucket";
  private static final String INPUT_PREFIX = "input_prefix";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private BlobStoreClient blobStoreClient;
  private DataSourceSizeProvider dataSourceSizeProvider;

  @Before
  public void setup() {
    dataSourceSizeProvider = new BlobStoreDataSourceSizeProvider(blobStoreClient);
  }

  @Test
  public void isAtLeastSize_success() throws Exception {
    long size = 1000L;
    var dataOwnersList =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataOwner.DataLocation.newBuilder()
                            .setInputDataBucketName(INPUT_BUCKET)
                            .setInputDataBlobPrefix(INPUT_PREFIX)
                            .setIsStreamed(true)))
            .build();
    when(blobStoreClient.isAtLeastSize(eq(size), eq(INPUT_BUCKET), eq(INPUT_PREFIX), any()))
        .thenReturn(true);

    var result = dataSourceSizeProvider.isAtLeastSize(size, dataOwnersList, Optional.empty());

    assertThat(result).isTrue();
    verify(blobStoreClient).isAtLeastSize(eq(size), eq(INPUT_BUCKET), eq(INPUT_PREFIX), any());
  }

  @Test
  public void isAtLeastSize_paths_success() throws Exception {
    long size = 1000L;
    var blobs = ImmutableList.of("test0/test.csv", "test1/test1.csv");
    var dataOwnersList =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataOwner.DataLocation.newBuilder()
                            .setInputDataBucketName(INPUT_BUCKET)
                            .addAllInputDataBlobPaths(blobs)
                            .setIsStreamed(true)))
            .build();
    when(blobStoreClient.isAtLeastSize(eq(size), eq(INPUT_BUCKET), eq(blobs), any()))
        .thenReturn(true);

    var result = dataSourceSizeProvider.isAtLeastSize(size, dataOwnersList, Optional.empty());

    assertThat(result).isTrue();
    verify(blobStoreClient).isAtLeastSize(eq(size), eq(INPUT_BUCKET), eq(blobs), any());
  }

  @Test
  public void isAtLeastSize_blobStoreException_throwsException() throws Exception {
    long size = 1000L;
    var dataOwnersList =
        DataOwnerList.newBuilder()
            .addDataOwners(
                DataOwner.newBuilder()
                    .setDataLocation(
                        DataOwner.DataLocation.newBuilder()
                            .setInputDataBucketName(INPUT_BUCKET)
                            .setInputDataBlobPrefix(INPUT_PREFIX)
                            .setIsStreamed(true)))
            .build();
    when(blobStoreClient.isAtLeastSize(eq(size), eq(INPUT_BUCKET), eq(INPUT_PREFIX), any()))
        .thenThrow(new BlobStoreClientException("error", MISSING_BLOBSTORE_FILE_ERROR));

    JobProcessorException ex =
        assertThrows(
            JobProcessorException.class,
            () -> dataSourceSizeProvider.isAtLeastSize(size, dataOwnersList, Optional.empty()));

    assertThat(ex.getErrorCode()).isEqualTo(MISSING_BLOBSTORE_FILE_ERROR);
    verify(blobStoreClient).isAtLeastSize(eq(size), eq(INPUT_BUCKET), eq(INPUT_PREFIX), any());
  }

  @Test
  public void isAtLeastSize_runtimeError_throwsException() throws Exception {
    long size = 1000L;
    var dataOwnersList = DataOwnerList.newBuilder().addDataOwners(DataOwner.newBuilder()).build();
    when(blobStoreClient.isAtLeastSize(eq(size), any(), anyString(), any()))
        .thenThrow(new RuntimeException());

    JobProcessorException ex =
        assertThrows(
            JobProcessorException.class,
            () -> dataSourceSizeProvider.isAtLeastSize(size, dataOwnersList, Optional.empty()));

    assertThat(ex.getErrorCode()).isEqualTo(DATA_SOURCE_SIZE_PROVIDER_ERROR);
    verify(blobStoreClient).isAtLeastSize(eq(size), any(), anyString(), any());
  }
}
