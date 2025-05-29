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

package com.google.cm.mrp.clients.testutils;

import static java.util.Base64.getUrlEncoder;

import java.time.Instant;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Utility class to organize attestationToken-related test operations */
public final class AttestationTokenTestUtil {

  // private constructor
  private AttestationTokenTestUtil() {}

  /** Returns 200 OK HTTP response containing the given test token */
  public static Answer<?> getHttpResponseForGivenTestToken(String token) {
    return getHttpResponseForGivenTestTokenAndResponseCode(token, /* code= */ 200);
  }

  /** Returns HTTP response containing the given test token with the given status code */
  public static Answer<?> getHttpResponseForGivenTestTokenAndResponseCode(String token, int code) {
    return (InvocationOnMock invocation) -> {
      var response = new BasicClassicHttpResponse(code);
      response.setEntity(new StringEntity(token));

      @SuppressWarnings("unchecked")
      HttpClientResponseHandler<String> handler =
          (HttpClientResponseHandler<String>) invocation.getArguments()[1];
      return handler.handleResponse(response);
    };
  }

  /** Gets attestation token in JSON format */
  public static String getAttestationTokenAsJson() {
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

  /** Creates fake JWT to return in test HTTP calls */
  public static String createFakeJwt(String jsonPayload) {
    String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    String encodedHeader = getUrlEncoder().encodeToString(header.getBytes());
    String encodedPayload = getUrlEncoder().encodeToString(jsonPayload.getBytes());
    String signature = "fakesignature";

    return encodedHeader + "." + encodedPayload + "." + signature;
  }
}
