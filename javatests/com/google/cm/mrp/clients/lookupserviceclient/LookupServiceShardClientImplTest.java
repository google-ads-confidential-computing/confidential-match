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

import static com.google.cm.mrp.selectors.LookupProtoFormatSelector.BINARY;
import static com.google.cm.mrp.selectors.LookupProtoFormatSelector.JSON;
import static com.google.cm.shared.api.model.Code.NOT_FOUND;
import static com.google.cm.shared.api.model.Code.OK;
import static com.google.cm.shared.api.model.Code.UNKNOWN;
import static com.google.cm.util.ProtoUtils.getJsonFromProto;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.lookupserver.api.LookupProto.DataRecord;
import com.google.cm.lookupserver.api.LookupProto.LookupKey;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.KeyFormat;
import com.google.cm.lookupserver.api.LookupProto.LookupResponse;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceShardClient.LookupServiceShardClientException;
import com.google.common.util.concurrent.Futures;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class LookupServiceShardClientImplTest {

  private static final String SIMPLE_URL = "http://unused";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private CloseableHttpAsyncClient httpClient;
  private LookupServiceShardClient jsonShardClient;
  private LookupServiceShardClient binaryShardClient;

  @Before
  public void setUp() {
    jsonShardClient =
        jsonShardClient == null
            ? new LookupServiceShardClientImpl(httpClient, JSON.getFormatHandler())
            : jsonShardClient;
    binaryShardClient =
        binaryShardClient == null
            ? new LookupServiceShardClientImpl(httpClient, BINARY.getFormatHandler())
            : binaryShardClient;
  }

  @Test
  public void lookupRecords_json_returnsLookupResponse() throws Exception {
    DataRecord clientDataRecord =
        DataRecord.newBuilder().setLookupKey(LookupKey.newBuilder().setKey("unittestkey")).build();
    LookupRequest lookupRequest =
        LookupRequest.newBuilder()
            .addDataRecords(clientDataRecord)
            .setKeyFormat(KeyFormat.KEY_FORMAT_HASHED)
            .build();
    LookupResponse lookupResponseExpected =
        LookupResponse.newBuilder()
            .addLookupResults(LookupResult.newBuilder().setClientDataRecord(clientDataRecord))
            .build();
    var response =
        SimpleHttpResponse.create(OK.getHttpStatusCode(), getJsonFromProto(lookupResponseExpected));
    when(httpClient.execute(any(), any())).thenReturn(Futures.immediateFuture(response));

    LookupResponse lookupResponseActual = jsonShardClient.lookupRecords(SIMPLE_URL, lookupRequest);

    assertThat(lookupResponseActual.getLookupResultsList()).hasSize(1);
    assertThat(lookupResponseActual.getLookupResults(0).getClientDataRecord())
        .isEqualTo(clientDataRecord);
    assertThat(lookupResponseActual).isEqualTo(lookupResponseExpected);
    verify(httpClient, times(1)).execute(any(), any());
    verifyNoMoreInteractions(httpClient);
  }

  @Test
  public void lookupRecords_json_httpResponseNotOk_throwsException() {
    LookupRequest lookupRequest = LookupRequest.getDefaultInstance();
    var response = SimpleHttpResponse.create(NOT_FOUND.getHttpStatusCode());
    when(httpClient.execute(any(), any())).thenReturn(Futures.immediateFuture(response));

    var ex =
        assertThrows(
            LookupServiceShardClientException.class,
            () -> jsonShardClient.lookupRecords(SIMPLE_URL, lookupRequest));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "LookupServiceShardClient HTTP response status: "
                + NOT_FOUND.getHttpStatusCode()
                + " "
                + NOT_FOUND.name());
    assertThat(ex.getErrorCode()).isEqualTo(NOT_FOUND.name());
    verify(httpClient, times(1)).execute(any(), any());
    verifyNoMoreInteractions(httpClient);
  }

  @Test
  public void lookupRecords_json_invalidResponseJson_throwsException() {
    LookupRequest lookupRequest = LookupRequest.getDefaultInstance();
    var response =
        Futures.immediateFuture(SimpleHttpResponse.create(OK.getHttpStatusCode(), "bad json"));
    when(httpClient.execute(any(), any())).thenReturn(response);

    var ex =
        assertThrows(
            LookupServiceShardClientException.class,
            () -> jsonShardClient.lookupRecords(SIMPLE_URL, lookupRequest));

    assertThat(ex).hasMessageThat().isEqualTo("LookupServiceShardClient threw an exception.");
    assertThat(ex).hasCauseThat().isInstanceOf(java.io.IOException.class);
    assertThat(ex.getErrorCode()).isEqualTo(UNKNOWN.name());
    verify(httpClient, times(1)).execute(any(), any());
    verifyNoMoreInteractions(httpClient);
  }

  @Test
  public void lookupRecords_binary_returnsLookupResponse() throws Exception {
    DataRecord clientDataRecord =
        DataRecord.newBuilder().setLookupKey(LookupKey.newBuilder().setKey("unittestkey")).build();
    LookupRequest lookupRequest =
        LookupRequest.newBuilder()
            .addDataRecords(clientDataRecord)
            .setKeyFormat(KeyFormat.KEY_FORMAT_HASHED)
            .build();
    LookupResponse lookupResponseExpected =
        LookupResponse.newBuilder()
            .addLookupResults(LookupResult.newBuilder().setClientDataRecord(clientDataRecord))
            .build();
    var response =
        SimpleHttpResponse.create(OK.getHttpStatusCode(), lookupResponseExpected.toByteArray());
    when(httpClient.execute(any(), any())).thenReturn(Futures.immediateFuture(response));

    LookupResponse lookupResponseActual =
        binaryShardClient.lookupRecords(SIMPLE_URL, lookupRequest);

    assertThat(lookupResponseActual.getLookupResultsList()).hasSize(1);
    assertThat(lookupResponseActual.getLookupResults(0).getClientDataRecord())
        .isEqualTo(clientDataRecord);
    assertThat(lookupResponseActual).isEqualTo(lookupResponseExpected);
    verify(httpClient, times(1)).execute(any(), any());
    verifyNoMoreInteractions(httpClient);
  }

  @Test
  public void lookupRecords_binary_httpResponseNotOk_throwsException() {
    LookupRequest lookupRequest = LookupRequest.getDefaultInstance();
    var response = SimpleHttpResponse.create(NOT_FOUND.getHttpStatusCode());
    when(httpClient.execute(any(), any())).thenReturn(Futures.immediateFuture(response));

    var ex =
        assertThrows(
            LookupServiceShardClientException.class,
            () -> binaryShardClient.lookupRecords(SIMPLE_URL, lookupRequest));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "LookupServiceShardClient HTTP response status: "
                + NOT_FOUND.getHttpStatusCode()
                + " "
                + NOT_FOUND.name());
    assertThat(ex.getErrorCode()).isEqualTo(NOT_FOUND.name());
    verify(httpClient, times(1)).execute(any(), any());
    verifyNoMoreInteractions(httpClient);
  }

  @Test
  public void lookupRecords_binary_invalidResponseJson_throwsException() {
    LookupRequest lookupRequest = LookupRequest.getDefaultInstance();
    var response =
        Futures.immediateFuture(SimpleHttpResponse.create(OK.getHttpStatusCode(), "bad bytes"));
    when(httpClient.execute(any(), any())).thenReturn(response);

    var ex =
        assertThrows(
            LookupServiceShardClientException.class,
            () -> binaryShardClient.lookupRecords(SIMPLE_URL, lookupRequest));

    assertThat(ex).hasMessageThat().isEqualTo("LookupServiceShardClient threw an exception.");
    assertThat(ex).hasCauseThat().isInstanceOf(java.io.IOException.class);
    assertThat(ex.getErrorCode()).isEqualTo(UNKNOWN.name());
    verify(httpClient, times(1)).execute(any(), any());
    verifyNoMoreInteractions(httpClient);
  }
}
