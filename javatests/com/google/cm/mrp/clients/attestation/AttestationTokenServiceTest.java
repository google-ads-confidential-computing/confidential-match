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

package com.google.cm.mrp.clients.attestation;

import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.createFakeJwt;
import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.getAttestationTokenAsJson;
import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.getHttpResponseForGivenTestToken;
import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.getHttpResponseForGivenTestTokenAndResponseCode;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cm.mrp.clients.attestation.AttestationTokenServiceExceptions.AttestationTokenServiceException;
import com.google.scp.shared.mapper.GuavaObjectMapper;
import java.io.IOException;
import java.util.List;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
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
public class AttestationTokenServiceTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  private static final ObjectMapper mapper = new GuavaObjectMapper();
  private static final String DEFAULT_AUDIENCE = "testAudience";
  private static final List<String> SIGNATURES = List.of("keyId1");
  @Mock private CloseableHttpClient httpClient;
  private @Captor ArgumentCaptor<ClassicHttpRequest> requestCaptor;
  private final String testToken = createFakeJwt(getAttestationTokenAsJson());
  private AttestationTokenService tokenService;

  @Before
  public void setup() {
    tokenService = new AttestationTokenService(httpClient, DEFAULT_AUDIENCE, SIGNATURES);
  }

  @Test
  public void getAttestationToken_success() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));

    assertThat(tokenService.getToken()).isEqualTo(testToken);

    var request = requestCaptor.getValue();
    String requestBody = EntityUtils.toString(request.getEntity());
    // Test both Object and JSON to ensure JSON parsing is not false positive
    assertThat(mapper.readValue(requestBody, ConfidentialSpaceTokenRequest.class))
        .isEqualTo(getExpectedConfidentialSpaceTokenRequest(DEFAULT_AUDIENCE));
    JsonNode requestJson = mapper.readTree(requestBody);
    assertThat(requestJson.get("audience").asText()).isEqualTo(DEFAULT_AUDIENCE);
    assertThat(request.getMethod()).isEqualTo("POST");
    assertThat(request.getUri().getHost()).isEqualTo("localhost");
    assertThat(request.getUri().getPath()).isEqualTo("/v1/token");
  }

  @Test
  public void getAttestationTokenForAudience_Success() throws Exception {
    String audience = "testAudience";
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));

    assertThat(tokenService.getToken()).isEqualTo(testToken);

    var request = requestCaptor.getValue();
    String requestBody = EntityUtils.toString(request.getEntity());
    assertThat(mapper.readValue(requestBody, ConfidentialSpaceTokenRequest.class))
        .isEqualTo(getExpectedConfidentialSpaceTokenRequest(audience));
    JsonNode requestJson = mapper.readTree(requestBody);
    assertThat(requestJson.get("audience").asText()).isEqualTo(audience);
    assertThat(request.getMethod()).isEqualTo("POST");
    assertThat(request.getUri().getHost()).isEqualTo("localhost");
    assertThat(request.getUri().getPath()).isEqualTo("/v1/token");
  }

  @Test
  public void getAttestationTokenForAudience_BadResponseCode_Throws() throws Exception {
    when(httpClient.execute(any(), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            getHttpResponseForGivenTestTokenAndResponseCode("INTERNAL_ERROR", /* code= */ 500));

    var ex = assertThrows(AttestationTokenServiceException.class, () -> tokenService.getToken());
    assertThat(ex.getMessage()).isEqualTo("Could not get token from cache.");
  }

  @Test
  public void getAttestationTokenForAudience_ClientException_Throws() throws Exception {
    when(httpClient.execute(any(), any(HttpClientResponseHandler.class)))
        .thenThrow(IOException.class);

    var ex = assertThrows(AttestationTokenServiceException.class, () -> tokenService.getToken());
    assertThat(ex.getMessage()).isEqualTo("Could not get token from cache.");
  }

  private static ConfidentialSpaceTokenRequest getExpectedConfidentialSpaceTokenRequest(
      String audience) throws Exception {
    String signatureString = String.join(",", SIGNATURES);
    String tokenJson =
        "{\"audience\":\""
            + audience
            + "\","
            + "\"token_type\":\"AWS_PRINCIPALTAGS\","
            + "\"aws_principal_tag_options\":{"
            + "\"allowed_principal_tags\":{"
            + "\"container_image_signatures\":{"
            + "\"key_ids\":[\""
            + signatureString
            + "\"]}}}}";
    return mapper.readValue(tokenJson, ConfidentialSpaceTokenRequest.class);
  }
}
