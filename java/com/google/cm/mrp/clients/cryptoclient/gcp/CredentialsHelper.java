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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.scp.shared.clients.configclient.gcp.CredentialSource;
import com.google.scp.shared.mapper.GuavaObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

/** Helper class to generate Google credentials. */
public final class CredentialsHelper {
  private static final ObjectMapper mapper = new GuavaObjectMapper();

  /**
   * Provides credentials which can be used by TEE to impersonate Service Account to access
   * protected resources.
   */
  public static GoogleCredentials getAttestedCredentials(
      String wipProvider, Optional<String> serviceAccountToImpersonate) throws IOException {

    String credentialConfig = getCredentialConfig(wipProvider, serviceAccountToImpersonate);

    return GoogleCredentials.fromStream(new ByteArrayInputStream(credentialConfig.getBytes()));
  }

  // Configures GCP credential with Workload Identity Pool Provider and impersonate service account
  // both.
  private static String getCredentialConfig(
      String wipProvider, Optional<String> serviceAccountToImpersonate)
      throws JsonProcessingException {
    var credentialConfigBuilder =
        CredentialConfig.builder()
            .type("external_account")
            .audience(String.format("//iam.googleapis.com/%s", wipProvider))
            .credentialSource(
                CredentialSource.builder()
                    .file("/run/container_launcher/attestation_verifier_claims_token")
                    .build())
            .subjectTokenType("urn:ietf:params:oauth:token-type:jwt")
            .tokenUrl("https://sts.googleapis.com/v1/token");

    if (serviceAccountToImpersonate.isPresent()) {
      credentialConfigBuilder =
          credentialConfigBuilder.serviceAccountImpersonationUrl(
              String.format(
                  "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
                  serviceAccountToImpersonate.get()));
    }

    return mapper.writeValueAsString(credentialConfigBuilder.build());
  }
}
