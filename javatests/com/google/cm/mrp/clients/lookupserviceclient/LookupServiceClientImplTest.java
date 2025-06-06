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

package com.google.cm.mrp.clients.lookupserviceclient;

import static com.google.api.client.testing.http.HttpTesting.SIMPLE_URL;
import static com.google.cm.util.JumpConsistentHasher.hash;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo;
import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.CoordinatorInfo;
import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.CoordinatorKeyInfo;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.KeyFormat;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.cm.lookupserver.api.LookupProto.MatchedDataRecord;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord.LookupKey;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient.LookupServiceClientException;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClient.LookupServiceShardClientException;
import com.google.cm.mrp.clients.lookupserviceclient.converters.DataRecordConverter;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientRequest;
import com.google.cm.mrp.clients.lookupserviceclient.model.LookupServiceClientResponse;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient.OrchestratorClientException;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse.Shard;
import com.google.cm.shared.api.model.Code;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
public final class LookupServiceClientImplTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  private static final String TEST_CLUSTER_GROUP_ID = "testClusterGroupId";

  @Mock private OrchestratorClient orchestratorClient;
  @Mock private LookupServiceShardClient lookupServiceShardClient;
  private LookupServiceClient lookupServiceClient;
  @Mock private CryptoClient mockCryptoClient;

  @Captor private ArgumentCaptor<String> shardEndpointCaptor;
  @Captor private ArgumentCaptor<LookupRequest> lookupRequestCaptor;

  // Makes requests for tests, and at least one record is required to trigger shard client calls
  private static LookupServiceClientRequest getLookupRequest(int numRecords) {
    return getLookupRequest(numRecords, /* associatedData= */ ImmutableList.of());
  }

  private static LookupServiceClientRequest getLookupRequest(
      int numRecords, List<String> associatedData) {
    var builder = LookupServiceClientRequest.builder().setKeyFormat(KeyFormat.KEY_FORMAT_HASHED);
    for (int i = numRecords; i > 0; --i) {
      builder.addRecord(
          LookupDataRecord.newBuilder().setLookupKey(LookupKey.newBuilder().setKey("" + i)));
    }
    if (!associatedData.isEmpty()) {
      builder.setAssociatedDataKeys(associatedData);
    }
    return builder.build();
  }

  private LookupServiceClientRequest getEncryptedLookupRequest(int numRecords) {
    var builder =
        LookupServiceClientRequest.builder().setKeyFormat(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED);
    for (int i = numRecords; i > 0; --i) {
      builder.addRecord(
          LookupDataRecord.newBuilder().setLookupKey(LookupKey.newBuilder().setKey("" + i)));
    }
    builder.setCryptoClient(mockCryptoClient);

    String TEST_WIP = "testWip";
    String TEST_IDENTITY = "testIdentity";
    String TEST_ENDPOINT = "testEndpoint";
    String TEST_WIP_B = "testWipB";
    String TEST_IDENTITY_B = "testIdentityB";
    String TEST_ENDPOINT_B = "testEndpointB";
    EncryptionKeyInfo keyInfo =
        EncryptionKeyInfo.newBuilder()
            .setCoordinatorKeyInfo(
                CoordinatorKeyInfo.newBuilder()
                    .setKeyId("123")
                    .addCoordinatorInfo(
                        CoordinatorInfo.newBuilder()
                            .setKeyServiceEndpoint(TEST_ENDPOINT)
                            .setKmsIdentity(TEST_IDENTITY)
                            .setKmsWipProvider(TEST_WIP))
                    .addCoordinatorInfo(
                        CoordinatorInfo.newBuilder()
                            .setKeyServiceEndpoint(TEST_ENDPOINT_B)
                            .setKmsIdentity(TEST_IDENTITY_B)
                            .setKmsWipProvider(TEST_WIP_B))
                    .build())
            .build();
    builder.setEncryptionKeyInfo(keyInfo);

    return builder.build();
  }

  private LookupServiceClientRequest getEncryptedLookupRequestWithoutEncryptionInfo(
      int numRecords) {
    var builder =
        LookupServiceClientRequest.builder().setKeyFormat(KeyFormat.KEY_FORMAT_HASHED_ENCRYPTED);
    for (int i = numRecords; i > 0; --i) {
      builder.addRecord(
          LookupDataRecord.newBuilder().setLookupKey(LookupKey.newBuilder().setKey("" + i)));
    }
    builder.setCryptoClient(mockCryptoClient);

    return builder.build();
  }

  // Makes responses for the shard client to return, with given client data record
  private static LookupResponse getLookupResponse(List<LookupDataRecord> records) {
    var builder = LookupResponse.newBuilder();
    for (var record : records) {
      builder.addLookupResults(
          LookupResult.newBuilder()
              .setClientDataRecord(DataRecordConverter.convertToDataRecord(record))
              .addMatchedDataRecords(MatchedDataRecord.getDefaultInstance()));
    }
    return builder.build();
  }

  // Contains shards, all of which use the fake endpoint
  private static GetCurrentShardingSchemeResponse getScheme(int numShards) {
    var builder = GetCurrentShardingSchemeResponse.newBuilder();
    for (int i = 0; i < numShards; i++) {
      builder.addShards(Shard.newBuilder().setShardNumber(i).setServerAddressUri(SIMPLE_URL));
    }
    return builder.build();
  }

  private static GetCurrentShardingSchemeResponse getShuffledScheme(int numShards) {
    var builder = GetCurrentShardingSchemeResponse.newBuilder();
    List<Long> shardNumbers = LongStream.range(0, numShards).boxed().collect(Collectors.toList());
    Collections.shuffle(shardNumbers);
    for (int i = 0; i < numShards; i++) {
      builder.addShards(
          Shard.newBuilder()
              .setShardNumber(shardNumbers.get(i))
              .setServerAddressUri(String.valueOf(shardNumbers.get(i))));
    }
    return builder.build();
  }

  @Before
  public void setUp() {
    // Default maximum of 10 data records per shard request
    lookupServiceClient =
        new LookupServiceClientImpl(
            orchestratorClient,
            lookupServiceShardClient,
            directExecutor(),
            10,
            TEST_CLUSTER_GROUP_ID);
  }

  @Test
  public void lookupRecords_returnsLookupResponse() throws Exception {
    var lookupRequest = getLookupRequest(100, ImmutableList.of("encrypted_gaia_id"));
    var firstLookupResponse = getLookupResponse(lookupRequest.records());
    var otherLookupResponse = getLookupResponse(List.of());
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getScheme(3));
    // The first call returns all records, since this test doesn't check that records hash to
    // different shards.
    when(lookupServiceShardClient.lookupRecords(eq(SIMPLE_URL), lookupRequestCaptor.capture()))
        .thenReturn(firstLookupResponse)
        .thenReturn(otherLookupResponse);

    LookupServiceClientResponse result = lookupServiceClient.lookupRecords(lookupRequest);

    // 100 data records, 3 shards, and 10 records max per shard request
    // Each shard gets 3 full requests, and 1 partially filled request; 4x3 = 12 total
    verify(lookupServiceShardClient, times(12)).lookupRecords(eq(SIMPLE_URL), any());
    assertThat(result.results().size()).isEqualTo(lookupRequest.records().size());
    // verify all requests have the same "associatedDataKeys"
    var associatedDataSet =
        lookupRequestCaptor.getAllValues().stream()
            .map(LookupRequest::getAssociatedDataKeysList)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    assertThat(associatedDataSet).hasSize(1);
    assertThat(associatedDataSet.contains("encrypted_gaia_id")).isTrue();
  }

  @Test
  public void lookupRecords_decryptedKeySendsToCorrectShard() throws Exception {
    int numShards = 3;
    String encryptedKey = "encrypted";
    String decryptedKey = "decrypted";
    var lookupRequest =
        LookupServiceClientRequest.builder()
            .setKeyFormat(KeyFormat.KEY_FORMAT_HASHED)
            .addRecord(
                LookupDataRecord.newBuilder()
                    .setLookupKey(
                        LookupKey.newBuilder().setKey(encryptedKey).setDecryptedKey(decryptedKey)))
            .build();
    int encryptedJch = hash(encryptedKey, numShards);
    int decryptedJch = hash(decryptedKey, numShards);
    var lookupResponse = getLookupResponse(lookupRequest.records());
    var schemeBuilder = GetCurrentShardingSchemeResponse.newBuilder();
    for (int i = 0; i < numShards; i++) {
      schemeBuilder.addShards(Shard.newBuilder().setShardNumber(i).setServerAddressUri(i + ""));
    }
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(schemeBuilder.build());
    // Only respond when hitting the shard received from the decrypted JCH
    when(lookupServiceShardClient.lookupRecords(eq(String.valueOf(decryptedJch)), any()))
        .thenReturn(lookupResponse);

    LookupServiceClientResponse result = lookupServiceClient.lookupRecords(lookupRequest);

    // Verify only decrypted JCH shard was called
    verify(lookupServiceShardClient)
        .lookupRecords(eq(String.valueOf(decryptedJch)), lookupRequestCaptor.capture());
    verify(lookupServiceShardClient, never())
        .lookupRecords(eq(String.valueOf(encryptedJch)), any());
    assertThat(result.results()).hasSize(1);
    // Verify encrypted key was passed to lookup server
    assertThat(lookupRequestCaptor.getValue().getDataRecords(0).getLookupKey().getKey())
        .isEqualTo(encryptedKey);
  }

  @Test
  public void lookupRecords_whenUnorderedResponseFromOrchestratorThenCallsCorrectShard()
      throws Exception {
    final int numRecords = 1000;
    final int numShards = 50;
    int[] shardCounters = new int[numShards];
    for (int i = 1; i <= numRecords; i++) {
      // this counts number of records sent to each shard
      shardCounters[hash(String.valueOf(i), numShards)]++;
    }
    int expectedTotalShardCalls = 0;
    for (int i = 0; i < numShards; i++) {
      // counts all full requests and possibly one partially filled request for each shard
      expectedTotalShardCalls += shardCounters[i] / 10 + (shardCounters[i] % 10 == 0 ? 0 : 1);
    }
    var lookupRequest = getLookupRequest(numRecords);
    var lookupResponse = getLookupResponse(List.of());
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getShuffledScheme(numShards));
    when(lookupServiceShardClient.lookupRecords(anyString(), any())).thenReturn(lookupResponse);

    LookupServiceClientResponse result = lookupServiceClient.lookupRecords(lookupRequest);

    verify(lookupServiceShardClient, times(expectedTotalShardCalls))
        .lookupRecords(shardEndpointCaptor.capture(), lookupRequestCaptor.capture());
    List<String> shardEndpoints = shardEndpointCaptor.getAllValues();
    assertThat(shardEndpoints.size()).isEqualTo(expectedTotalShardCalls);
    List<LookupRequest> lookupRequests = lookupRequestCaptor.getAllValues();
    assertThat(lookupRequests.size()).isEqualTo(expectedTotalShardCalls);
    for (int i = 0; i < expectedTotalShardCalls; i++) {
      int actualShardNumber = Integer.parseInt(shardEndpoints.get(i));
      LookupRequest actualLookupRequest = lookupRequests.get(i);
      // this confirms that all records in the lookup request were sent to the correct shard
      assertThat(
              actualLookupRequest.getDataRecordsList().stream()
                  .allMatch(
                      dataRecord ->
                          hash(dataRecord.getLookupKey().getKey(), numShards) == actualShardNumber))
          .isTrue();
      // this subtracts the actual data record counts from the expected ones (counts back)
      shardCounters[actualShardNumber] -= actualLookupRequest.getDataRecordsCount();
    }
    // confirms that all requests for each shard are accounted for
    assertThat(Arrays.stream(shardCounters).allMatch(i -> i == 0)).isTrue();
    assertThat(result.results().size()).isEqualTo(0);
  }

  @Test
  public void lookupRecords_shardClientThrows_throwsException() throws Exception {
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getScheme(1));
    when(lookupServiceShardClient.lookupRecords(eq(SIMPLE_URL), any()))
        .thenThrow(LookupServiceShardClientException.class);

    var ex =
        assertThrows(
            LookupServiceClientException.class,
            () -> lookupServiceClient.lookupRecords(getLookupRequest(1)));

    verify(lookupServiceShardClient).lookupRecords(eq(SIMPLE_URL), any());
    verifyNoMoreInteractions(lookupServiceShardClient);
    assertThat(ex).hasCauseThat().isInstanceOf(LookupServiceClientException.class);
    assertThat(ex).hasCauseThat().hasCauseThat().isInstanceOf(CompletionException.class);
    assertThat(ex)
        .hasCauseThat()
        .hasCauseThat()
        .hasCauseThat()
        .isInstanceOf(LookupServiceShardClientException.class);
    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "LookupServiceClient threw an exception while retrieving lookup results from one of the"
                + " shards.");
    assertThat(ex.getErrorCode()).isEqualTo(Code.INTERNAL.name());
  }

  @Test
  public void lookupRecords_orchestratorClientThrows_throwsException() throws Exception {
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenThrow(OrchestratorClientException.class);

    var ex =
        assertThrows(
            LookupServiceClientException.class,
            () -> lookupServiceClient.lookupRecords(getLookupRequest(1)));

    assertThat(ex).isInstanceOf(LookupServiceClientException.class);
    assertThat(ex).hasCauseThat().isInstanceOf(OrchestratorClientException.class);
    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "LookupServiceClient threw an exception while attempting to get the scheme from the"
                + " orchestrator.");
    assertThat(ex.getErrorCode()).isEqualTo(Code.INTERNAL.name());
    verifyNoInteractions(lookupServiceShardClient);
  }

  @Test
  public void
      lookupRecords_whenShardClientRejectsShardingSchemeThenGetsNewSchemeAndReturnsLookupResponse()
          throws Exception {
    var lookupRequest = getLookupRequest(10);
    var lookupResponse = getLookupResponse(lookupRequest.records());
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getScheme(1));
    when(lookupServiceShardClient.lookupRecords(eq(SIMPLE_URL), any()))
        .thenThrow(
            new LookupServiceShardClientException(
                Code.INVALID_ARGUMENT.name(), ErrorReason.INVALID_SCHEME.name(), "message"))
        .thenReturn(lookupResponse);

    LookupServiceClientResponse result = lookupServiceClient.lookupRecords(lookupRequest);

    verify(lookupServiceShardClient, times(2)).lookupRecords(eq(SIMPLE_URL), any());
    verifyNoMoreInteractions(lookupServiceShardClient);
    assertThat(result.results().size()).isEqualTo(lookupRequest.records().size());
  }

  @Test
  public void lookupRecords_whenShardClientRejectsShardingSchemeTwiceThenThrows() throws Exception {
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getScheme(1));
    when(lookupServiceShardClient.lookupRecords(eq(SIMPLE_URL), any()))
        .thenThrow(
            new LookupServiceShardClientException(
                Code.INVALID_ARGUMENT.name(), ErrorReason.INVALID_SCHEME.name(), "message"));

    var ex =
        assertThrows(
            LookupServiceClientException.class,
            () -> lookupServiceClient.lookupRecords(getLookupRequest(1)));

    verify(lookupServiceShardClient, times(2)).lookupRecords(eq(SIMPLE_URL), any());
    verifyNoMoreInteractions(lookupServiceShardClient);
    assertThat(ex).hasCauseThat().isInstanceOf(LookupServiceClientException.class);
    assertThat(ex).hasCauseThat().hasCauseThat().isInstanceOf(CompletionException.class);
    assertThat(ex)
        .hasCauseThat()
        .hasCauseThat()
        .hasCauseThat()
        .isInstanceOf(LookupServiceShardClientException.class);
    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "LookupServiceClient threw an exception while retrying lookup results from one of the"
                + " shards.");
    assertThat(ex.getErrorCode()).isEqualTo(Code.INTERNAL.name());
  }

  @Test
  public void lookupRecordsWithBatchEncryptionWithoutEncryptionKeyInfo_throwsException()
      throws Exception {
    var lookupRequest = getEncryptedLookupRequestWithoutEncryptionInfo(100);
    var firstLookupResponse = getLookupResponse(lookupRequest.records());
    var otherLookupResponse = getLookupResponse(List.of());
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getScheme(3));
    // The first call returns all records, since this test doesn't check that records hash to
    // different shards.
    when(lookupServiceShardClient.lookupRecords(eq(SIMPLE_URL), any()))
        .thenReturn(firstLookupResponse)
        .thenReturn(otherLookupResponse);

    when(mockCryptoClient.encrypt(any(), any(byte[].class))).thenReturn("encryptedstring");

    var ex =
        assertThrows(
            LookupServiceClientException.class,
            () -> lookupServiceClient.lookupRecords(lookupRequest));
    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "LookupServiceClient threw an exception while retrieving lookup results from one of the"
                + " shards.");
    assertThat(ex.getErrorCode()).isEqualTo(Code.INTERNAL.name());
  }

  @Test
  public void lookupRecordsWithBatchEncryption_returnsResponse() throws Exception {
    var lookupRequest = getEncryptedLookupRequest(100);
    var firstLookupResponse = getLookupResponse(lookupRequest.records());
    var otherLookupResponse = getLookupResponse(List.of());
    when(orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID))
        .thenReturn(getScheme(3));
    // The first call returns all records, since this test doesn't check that records hash to
    // different shards.
    when(lookupServiceShardClient.lookupRecords(eq(SIMPLE_URL), any()))
        .thenReturn(firstLookupResponse)
        .thenReturn(otherLookupResponse);
    when(mockCryptoClient.encrypt(any(), any(byte[].class))).thenReturn("encryptedstring");

    LookupServiceClientResponse result = lookupServiceClient.lookupRecords(lookupRequest);

    // 100 data records, 3 shards, and 10 records max per shard request
    // Each shard gets 3 full requests, and 1 partially filled request; 4x3 = 12 total
    verify(lookupServiceShardClient, times(12)).lookupRecords(eq(SIMPLE_URL), any());
    assertThat(result.results().size()).isEqualTo(lookupRequest.records().size());
  }
}
