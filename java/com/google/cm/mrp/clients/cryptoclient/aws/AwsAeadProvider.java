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

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import com.google.cm.mrp.clients.attestation.AttestationTokenService;
import com.google.cm.mrp.clients.attestation.AttestationTokenServiceExceptions.AttestationTokenServiceException;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.AwsParameters;
import com.google.crypto.tink.integration.awskmsv2.AwsKmsV2Client;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

/** Gets AEADs from AWS KMS using attested credentials */
public final class AwsAeadProvider implements AeadProvider {
  private static final Logger logger = LoggerFactory.getLogger(AwsAeadProvider.class);

  private static final SdkHttpClient HTTP_CLIENT = ApacheHttpClient.builder().build();
  private static final String FILE_PREFIX = "cfm-aws";
  private static final String ROLE_SESSION_NAME = "cfm-worker";
  private final AttestationTokenService attestationTokenService;
  private final List<Path> tokenFilePaths;

  @Inject
  public AwsAeadProvider(AttestationTokenService attestationTokenService) {
    this.attestationTokenService = attestationTokenService;
    tokenFilePaths = Collections.synchronizedList(new ArrayList<>());
  }

  /** Gets selector to retrieve Aeads from Cloud KMS */
  @Override
  public CloudAeadSelector getAeadSelector(AeadProviderParameters aeadProviderParameters)
      throws AeadProviderException {
    AwsParameters awsParameters =
        aeadProviderParameters
            .awsParameters()
            .orElseThrow(() -> new UncheckedAeadProviderException("Missing AWS parameters"));
    // AWS Credentials provider requires a token file
    Path tokenFilePath = createNewTokenFile();
    getAndWriteTokenToFile(tokenFilePath, awsParameters.audience());
    return getAwsClientForRole(awsParameters.roleArn(), tokenFilePath);
  }

  /** Closes AeadProvider, deleting existing files. */
  @Override
  public void close() throws IOException {
    try {
      synchronized (tokenFilePaths) {
        tokenFilePaths.forEach(tokenFilePath -> deleteFile(tokenFilePath.toFile()));
      }
    } catch (UncheckedAeadProviderException e) {
      logger.error("Failed to delete AwsAeadProvider file", e);
    }
  }

  private void getAndWriteTokenToFile(Path tokenFilePath, Optional<String> audience)
      throws AeadProviderException {
    try (BufferedWriter writer = Files.newBufferedWriter(tokenFilePath, TRUNCATE_EXISTING)) {
      String token =
          audience.isPresent()
              ? attestationTokenService.getTokenWithAudience(audience.get())
              : attestationTokenService.getToken();
      writer.write(token);
    } catch (IOException | AttestationTokenServiceException | UncheckedAeadProviderException e) {
      String msg = "Failed to write AttestationToken to file.";
      logger.error(msg, e);
      throw new AeadProviderException(msg, e);
    }
  }

  private CloudAeadSelector getAwsClientForRole(String roleArn, Path tokenFilePath) {
    return (kekUri) -> {
      AwsKmsV2Client kmsV2Client = buildAwsV2Client(roleArn, tokenFilePath);
      return kmsV2Client.getAead(kekUri);
    };
  }

  private AwsKmsV2Client buildAwsV2Client(String roleArn, Path tokenFilePath) {
    try {
      var credentialsProvider =
          WebIdentityTokenFileCredentialsProvider.builder()
              .webIdentityTokenFile(tokenFilePath)
              .roleArn(roleArn)
              .roleSessionName(ROLE_SESSION_NAME)
              .build();
      return new AwsKmsV2Client()
          .withHttpClient(HTTP_CLIENT)
          .withCredentialsProvider(credentialsProvider);
    } catch (GeneralSecurityException e) {
      String msg = "Failed to build AwsKmsV2Client";
      logger.error(msg, e);
      throw new UncheckedAeadProviderException(msg, e);
    }
  }

  private Path createNewTokenFile() {
    try {
      // TODO(b/398062906): Look into in-memory file solution.
      Path tokenFilePath = Files.createTempFile(FILE_PREFIX, /* suffix= */ "");
      tokenFilePath.toFile().deleteOnExit();
      tokenFilePaths.add(tokenFilePath);
      return tokenFilePath;
    } catch (IOException e) {
      String msg = "Could not create AttestationToken file.";
      logger.error(msg, e);
      throw new UncheckedAeadProviderException(msg, e);
    }
  }

  private void deleteFile(File file) {
    if (!file.delete()) {
      String msg =
          String.format(
              "Unable to delete the local file: %s after uploading", file.getAbsolutePath());
      logger.error(msg);
      throw new UncheckedAeadProviderException(msg);
    }
  }
}
