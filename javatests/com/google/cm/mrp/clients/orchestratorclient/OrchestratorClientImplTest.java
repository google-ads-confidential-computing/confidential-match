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

package com.google.cm.mrp.clients.orchestratorclient;

import static com.google.api.client.testing.http.HttpTesting.SIMPLE_URL;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cm.mrp.clients.orchestratorclient.OrchestratorClient.OrchestratorClientException;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse;
import com.google.cm.orchestrator.api.OrchestratorProto.GetCurrentShardingSchemeResponse.Shard;
import com.google.cm.shared.api.model.Code;
import com.google.cm.util.ProtoUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class OrchestratorClientImplTest {

  private static final String TEST_CLUSTER_GROUP_ID = "testClusterGroupId";

  private OrchestratorClient orchestratorClient;
  private LowLevelHttpRequest httpRequest;
  private LowLevelHttpResponse httpResponse;

  @Before
  public void setUp() {
    httpRequest =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() {
            return httpResponse;
          }
        };
    HttpRequestFactory factory =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return httpRequest;
          }
        }.createRequestFactory();
    orchestratorClient = new OrchestratorClientImpl(factory, SIMPLE_URL);
  }

  @Test
  public void getCurrentShardingScheme_returnsSchemeResponse() throws Exception {
    var responseProto =
        GetCurrentShardingSchemeResponse.newBuilder()
            .setType("test")
            .addShards(Shard.getDefaultInstance())
            .build();
    setResponse(Code.OK.getHttpStatusCode(), ProtoUtils.getJsonFromProto(responseProto));

    var schemeResponse = orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID);

    assertThat(schemeResponse).isEqualTo(responseProto);
  }

  @Test
  public void getCurrentShardingScheme_httpResponseNotOk_throwsException() {
    setResponse(Code.NOT_FOUND.getHttpStatusCode(), "");

    var ex =
        assertThrows(
            OrchestratorClientException.class,
            () -> orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "OrchestratorClient HTTP response status: "
                + Code.NOT_FOUND.getHttpStatusCode()
                + " "
                + Code.NOT_FOUND.name());
    assertThat(ex.getErrorCode()).isEqualTo(Code.NOT_FOUND.name());
    assertThat(ex).hasCauseThat().isInstanceOf(HttpResponseException.class);
    assertThat(((HttpResponseException) ex.getCause()).getStatusCode())
        .isEqualTo(Code.NOT_FOUND.getHttpStatusCode());
  }

  @Test
  public void getCurrentShardingScheme_httpClientSendException_throwsException() {
    httpRequest =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            throw new IOException();
          }
        };

    var ex =
        assertThrows(
            OrchestratorClientException.class,
            () -> orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID));

    assertThat(ex).hasMessageThat().isEqualTo("OrchestratorClient threw an exception.");
    assertThat(ex).hasCauseThat().isInstanceOf(IOException.class);
    assertThat(ex.getErrorCode()).isEqualTo(Code.UNKNOWN.name());
  }

  @Test
  public void getCurrentShardingScheme_internalRuntimeException_throwsClientException() {
    httpRequest =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() {
            throw new IllegalArgumentException();
          }
        };

    var ex =
        assertThrows(
            OrchestratorClientException.class,
            () -> orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID));

    assertThat(ex).hasMessageThat().isEqualTo("OrchestratorClient threw an exception.");
    assertThat(ex).hasCauseThat().isInstanceOf(IllegalArgumentException.class);
    assertThat(ex.getErrorCode()).isEqualTo(Code.UNKNOWN.name());
  }

  @Test
  public void getCurrentShardingScheme_invalidResponseJson_throwsException() {
    setResponse(Code.OK.getHttpStatusCode(), "Invalid proto message JSON");

    var ex =
        assertThrows(
            OrchestratorClientException.class,
            () -> orchestratorClient.getCurrentShardingScheme(TEST_CLUSTER_GROUP_ID));

    assertThat(ex).hasMessageThat().isEqualTo("OrchestratorClient threw an exception.");
    assertThat(ex.getErrorCode()).isEqualTo(Code.UNKNOWN.name());
    assertThat(ex).hasCauseThat().isInstanceOf(IOException.class);
  }

  private void setResponse(int status, String body) {
    httpResponse =
        new MockLowLevelHttpResponse() {

          @Override
          public int getStatusCode() {
            return status;
          }

          @Override
          public InputStream getContent() {
            return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
          }
        };
  }
}
