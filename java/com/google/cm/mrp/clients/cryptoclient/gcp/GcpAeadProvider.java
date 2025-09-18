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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK_FORMAT;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.exceptions.UncheckedAeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.GcpParameters;
import com.google.cm.mrp.clients.cryptoclient.utils.GcpProviderUtils;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KeysetReader;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets AEADs from GCP KMS using attested credentials if necessary */
public final class GcpAeadProvider implements AeadProvider {
  private static final Logger logger = LoggerFactory.getLogger(GcpAeadProvider.class);
  private static final String KEK_URI_PREFIX = "gcp-kms://";

  public GcpAeadProvider() {}

  /** Gets selector to retrieve Aeads from GCP Cloud KMS. */
  @Override
  public CloudAeadSelector getAeadSelector(AeadProviderParameters aeadProviderParameters)
      throws AeadProviderException {
    GcpParameters gcpParameters =
        aeadProviderParameters
            .gcpParameters()
            .orElseThrow(
                () -> {
                  String msg = "GCP parameters not found in GcpAeadProvider";
                  logger.error(msg);
                  return new AeadProviderException(msg, CRYPTO_CLIENT_CONFIGURATION_ERROR);
                });
    GoogleCredentials credentials =
        getCredentials(gcpParameters.wipProvider(), gcpParameters.serviceAccountToImpersonate());
    return getKmsClient(credentials);
  }

  @Override
  public KeysetHandle readKeysetHandle(KeysetReader dekReader, Aead kekAead)
      throws AeadProviderException {
    try {
      return KeysetHandle.read(dekReader, kekAead);
    } catch (GeneralSecurityException | IOException e) {
      Optional<AeadProviderException> gcpException = GcpProviderUtils.tryParseGcpKmsException(e);
      if (gcpException.isPresent()) {
        throw gcpException.get();
      } else {
        String msg = "KeysetHandle read failed for unknown reason.";
        logger.warn(msg, e);
        throw new AeadProviderException(msg, e, DEK_DECRYPTION_ERROR);
      }
    } catch (IllegalArgumentException e) {
      String msg = "Invalid format for KEK.";
      logger.info(msg, e);
      throw new AeadProviderException(msg, e, INVALID_KEK_FORMAT);
    }
  }

  private GoogleCredentials getCredentials(
      String kmsWipProvider, Optional<String> serviceAccountToImpersonate)
      throws AeadProviderException {
    try {
      return CredentialsHelper.getAttestedCredentials(kmsWipProvider, serviceAccountToImpersonate);
    } catch (IOException e) {
      String msg = "Could not get GCP credentials.";
      logger.error(msg);
      throw new AeadProviderException(msg, e);
    }
  }

  private CloudAeadSelector getKmsClient(GoogleCredentials credentials) {
    return (kekUri) -> {
      if (!kekUri.startsWith(KEK_URI_PREFIX) && kekUri.startsWith("projects/")) {
        kekUri = KEK_URI_PREFIX + kekUri;
      }
      GcpKmsClient client = new GcpKmsClient();
      try {
        client.withCredentials(credentials);
        return client.getAead(kekUri);
      } catch (GeneralSecurityException e) {
        String message = String.format("Error getting gcloud Aead with uri %s.", kekUri);
        throw new UncheckedAeadProviderException(message, e);
      } catch (IllegalArgumentException e) {
        String msg = "Invalid format for KEK.";
        logger.info(msg, e);
        throw new UncheckedAeadProviderException(msg, e, INVALID_KEK_FORMAT);
      }
    };
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
