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

package com.google.cm.mrp.clients.cryptoclient.gcp;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cm.mrp.clients.cryptoclient.gcp.CredentialConfig;
import com.google.scp.shared.clients.configclient.gcp.CredentialSource;
import com.google.scp.shared.mapper.GuavaObjectMapper;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CredentialConfigTest {
  private static final ObjectMapper mapper = new GuavaObjectMapper();

  @Test
  public void getCredentialConfigWithWipSaBoth_success() throws IOException {
    String wip = "wip";
    String sa = "serviceAccount";

    CredentialConfig config =
        CredentialConfig.builder()
            .type("external_account")
            .audience(String.format("//iam.googleapis.com/%s", wip))
            .credentialSource(
                CredentialSource.builder()
                    .file("/run/container_launcher/attestation_verifier_claims_token")
                    .build())
            .serviceAccountImpersonationUrl(
                String.format(
                    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
                    sa))
            .subjectTokenType("urn:ietf:params:oauth:token-type:jwt")
            .tokenUrl("https://sts.googleapis.com/v1/token")
            .build();

    String outputConfig = mapper.writeValueAsString(config);

    // outputConfig contains "audience" and "serviceAccountImpersonationUrl" both.
    assertThat(outputConfig).contains("audience");
    assertThat(outputConfig).contains("service_account_impersonation_url");
  }

  @Test
  public void getCredentialConfigWithWipOnly_success() throws IOException {
    String wip = "wip";

    CredentialConfig config =
        CredentialConfig.builder()
            .type("external_account")
            .audience(String.format("//iam.googleapis.com/%s", wip))
            .credentialSource(
                CredentialSource.builder()
                    .file("/run/container_launcher/attestation_verifier_claims_token")
                    .build())
            .subjectTokenType("urn:ietf:params:oauth:token-type:jwt")
            .tokenUrl("https://sts.googleapis.com/v1/token")
            .build();

    String outputConfig = mapper.writeValueAsString(config);

    assertThat(outputConfig).contains("audience");
    // The outputConfig shouldn't contain serviceAccountImpersonationUrl.
    assertFalse(outputConfig.contains("service_account_impersonation_url"));
  }
}
