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

package com.google.cm.mrp.clients.cryptoclient.aws;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.AWS_AUTH_FAILED;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK_FORMAT;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ROLE_FORMAT;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNAUTHORIZED_AUDIENCE;
import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.createFakeJwt;
import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.getAttestationTokenAsJson;
import static com.google.cm.mrp.clients.testutils.AttestationTokenTestUtil.getHttpResponseForGivenTestToken;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.clients.attestation.AttestationTokenService;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider.UncheckedAeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.AwsParameters;
import com.google.cm.mrp.clients.cryptoclient.models.DekKeysetReader;
import com.google.crypto.tink.Aead;
import com.google.protobuf.ByteString;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.util.List;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
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
import software.amazon.awssdk.services.kms.model.IncorrectKeyException;
import software.amazon.awssdk.services.sts.model.InvalidIdentityTokenException;
import software.amazon.awssdk.services.sts.model.StsException;

@RunWith(JUnit4.class)
public class AwsAeadProviderTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String TEST_ROLE = "arn:aws:kms:us-east-2:0000:test";
  private static final AeadProviderParameters TEST_PARAMETERS =
      AeadProviderParameters.builder()
          .setAwsParameters(
              AwsParameters.builder().setAudience("testAudience").setRoleArn(TEST_ROLE).build())
          .build();

  private static final String DEFAULT_AUDIENCE = "testAudience";
  private static final List<String> SIGNATURES = List.of("keyId1");
  @Mock private Aead mockAead;
  @Mock private CloseableHttpClient httpClient;
  private @Captor ArgumentCaptor<ClassicHttpRequest> requestCaptor;
  private final String testToken = createFakeJwt(getAttestationTokenAsJson());

  private AwsAeadProvider aeadProvider;

  @Before
  public void setup() {
    AttestationTokenService tokenService =
        new AttestationTokenService(httpClient, DEFAULT_AUDIENCE, SIGNATURES);
    aeadProvider = new AwsAeadProvider(tokenService);
  }

  @Test
  public void getAeadSelector_success() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));

    CloudAeadSelector result = aeadProvider.getAeadSelector(TEST_PARAMETERS);
    // "Aead" which is used to decrypt using AWS KMS
    Aead aead = result.getAead(TEST_ROLE);
  }

  @Test
  public void getAeadSelector_noPrefix_success() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));

    CloudAeadSelector result = aeadProvider.getAeadSelector(TEST_PARAMETERS);
    // "Aead" which is used to decrypt using AWS KMS
    Aead aead = result.getAead(TEST_ROLE);
  }

  @Test
  public void getAeadSelector_invalidKek_fail() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));

    CloudAeadSelector result = aeadProvider.getAeadSelector(TEST_PARAMETERS);
    UncheckedAeadProviderException ex =
        assertThrows(UncheckedAeadProviderException.class, () -> result.getAead("invalid"));

    assertThat(ex.getJobResultCode()).isEqualTo(INVALID_KEK_FORMAT);
    assertThat(ex).hasMessageThat().isEqualTo("Invalid format for KEK");
  }

  @Test
  public void readKeysetHandle_invalidAudience_fail() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));
    when(mockAead.decrypt(any(), any()))
        .thenThrow(
            InvalidIdentityTokenException.builder().message("Incorrect token audience").build());

    DekKeysetReader dekReader = new DekKeysetReader(ByteString.copyFromUtf8("test"));
    AeadProviderException ex =
        assertThrows(
            AeadProviderException.class, () -> aeadProvider.readKeysetHandle(dekReader, mockAead));

    assertThat(ex.getJobResultCode()).isEqualTo(UNAUTHORIZED_AUDIENCE);
    assertThat(ex).hasMessageThat().isEqualTo("Invalid audience passed to AWS KMS.");
  }

  @Test
  public void readKeysetHandle_unauthorized_fail() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));
    when(mockAead.decrypt(any(), any()))
        .thenThrow(
            StsException.builder()
                .message("Not authorized to perform sts:AssumeRoleWithWebIdentity")
                .build());

    DekKeysetReader dekReader = new DekKeysetReader(ByteString.copyFromUtf8("test"));
    AeadProviderException ex =
        assertThrows(
            AeadProviderException.class, () -> aeadProvider.readKeysetHandle(dekReader, mockAead));

    assertThat(ex.getJobResultCode()).isEqualTo(AWS_AUTH_FAILED);
    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("Attestation failed, either due to KMS trust conditions or role is invalid.");
  }

  @Test
  public void readKeysetHandle_wrongKek_fail() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));
    when(mockAead.decrypt(any(), any()))
        .thenThrow(
            IncorrectKeyException.builder()
                .message(
                    "The key ID in the request does not identify a CMK that can perform this"
                        + " operation")
                .build());

    DekKeysetReader dekReader = new DekKeysetReader(ByteString.copyFromUtf8("test"));
    AeadProviderException ex =
        assertThrows(
            AeadProviderException.class, () -> aeadProvider.readKeysetHandle(dekReader, mockAead));

    assertThat(ex.getJobResultCode()).isEqualTo(INVALID_KEK);
    assertThat(ex)
        .hasMessageThat()
        .isEqualTo(
            "KEK cannot decrypt DEK, either because it doesn't exist or does not have permission.");
  }

  @Test
  public void readKeysetHandle_invalidRoleFormat_fail() throws Exception {
    when(httpClient.execute(requestCaptor.capture(), any(HttpClientResponseHandler.class)))
        .thenAnswer(getHttpResponseForGivenTestToken(testToken));
    when(mockAead.decrypt(any(), any()))
        .thenThrow(StsException.builder().message("Request ARN is invalid").build());

    DekKeysetReader dekReader = new DekKeysetReader(ByteString.copyFromUtf8("test"));
    AeadProviderException ex =
        assertThrows(
            AeadProviderException.class, () -> aeadProvider.readKeysetHandle(dekReader, mockAead));

    assertThat(ex.getJobResultCode()).isEqualTo(INVALID_ROLE_FORMAT);
    assertThat(ex).hasMessageThat().isEqualTo("Invalid role ARN (bad format) passed.");
  }
}
