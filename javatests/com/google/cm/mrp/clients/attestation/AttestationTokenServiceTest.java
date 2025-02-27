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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Base64.getUrlEncoder;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cm.mrp.clients.attestation.AttestationTokenServiceExceptions.AttestationTokenServiceException;
import com.google.scp.shared.mapper.GuavaObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
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
  private final String testToken = createFakeJwt(getJsonToken());
  private AttestationTokenService tokenService;

  @Before
  public void setup() {
    tokenService = new AttestationTokenService(httpClient, DEFAULT_AUDIENCE, SIGNATURES);
  }

  @Test
  public void getAttestationToken_success() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            (InvocationOnMock invocation) -> {
              var response = new BasicClassicHttpResponse(200);
              response.setEntity(new StringEntity(testToken));

              @SuppressWarnings("unchecked")
              HttpClientResponseHandler<String> handler =
                  (HttpClientResponseHandler<String>) invocation.getArguments()[1];
              return handler.handleResponse(response);
            });

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
        .thenAnswer(
            (InvocationOnMock invocation) -> {
              var response = new BasicClassicHttpResponse(200);
              response.setEntity(new StringEntity(testToken));

              @SuppressWarnings("unchecked")
              HttpClientResponseHandler<String> handler =
                  (HttpClientResponseHandler<String>) invocation.getArguments()[1];
              return handler.handleResponse(response);
            });

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
            (InvocationOnMock invocation) -> {
              var response = new BasicClassicHttpResponse(500);
              response.setEntity(new StringEntity("INTERNAL ERROR"));

              @SuppressWarnings("unchecked")
              HttpClientResponseHandler<String> handler =
                  (HttpClientResponseHandler<String>) invocation.getArguments()[1];
              return handler.handleResponse(response);
            });

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

  private static String getJsonToken() {
    long time = Instant.now().getEpochSecond();
    return "{\"hwmodel\":   \"GCP_INTEL_TDX\",\"swname\":    \"CONFIDENTIAL_SPACE\",\"swversion\":"
        + " \"240900\",\"confidential_space.support_attributes\":"
        + " \"LATEST=STABLE=USABLE\",\"gce.project_id\":"
        + " \"projectidpaddedto30chars0000000000000000000\",\"gce.zone\":      "
        + " \"northamerica-northeast1-a\",\"container.signatures.key_ids\":"
        + " \"abcd357b59e9407fb017ca0e3e783b2bd5acbfea6c83dd82971a4150df5b25f9\",\"exp\": "
        + time
        + "}";
  }

  public static String createFakeJwt(String jsonPayload) {
    String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    String encodedHeader = getUrlEncoder().encodeToString(header.getBytes());
    String encodedPayload = getUrlEncoder().encodeToString(jsonPayload.getBytes());
    String signature = "fakesignature";

    return encodedHeader + "." + encodedPayload + "." + signature;
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
