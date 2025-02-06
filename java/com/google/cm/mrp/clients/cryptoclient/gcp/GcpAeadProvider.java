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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.GcpParameters;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

/** Gets AEADs from GCP KMS using attested credentials if necessary */
public final class GcpAeadProvider implements AeadProvider {

  public GcpAeadProvider() {}

  /** Gets selector to retrieve Aeads from GCP Cloud KMS. */
  @Override
  public CloudAeadSelector getAeadSelector(AeadProviderParameters aeadProviderParameters)
      throws AeadProviderException {
    GcpParameters gcpParameters = aeadProviderParameters.gcpParameters().orElseThrow();
    GoogleCredentials credentials =
        getCredentials(gcpParameters.wipProvider(), gcpParameters.serviceAccountToImpersonate());
    return getKmsClient(credentials);
  }

  private GoogleCredentials getCredentials(
      String kmsWipProvider, Optional<String> serviceAccountToImpersonate)
      throws AeadProviderException {
    try {
      return CredentialsHelper.getAttestedCredentials(kmsWipProvider, serviceAccountToImpersonate);
    } catch (IOException e) {
      throw new AeadProviderException(e.getCause());
    }
  }

  private CloudAeadSelector getKmsClient(GoogleCredentials credentials) {
    return (kekUri) -> {
      if (!kekUri.startsWith("gcp-kms://") && kekUri.startsWith("projects/")) {
        kekUri = "gcp-kms://" + kekUri;
      }
      GcpKmsClient client = new GcpKmsClient();
      try {
        client.withCredentials(credentials);
        return client.getAead(kekUri);
      } catch (GeneralSecurityException e) {
        String message = String.format("Error getting gcloud Aead with uri %s.", kekUri);
        throw new UncheckedAeadProviderException(message, e);
      }
    };
  }
}
