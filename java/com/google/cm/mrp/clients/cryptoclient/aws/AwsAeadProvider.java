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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK_FORMAT;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_ROLE_FORMAT;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNAUTHORIZED_AUDIENCE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import com.google.cm.mrp.clients.attestation.AttestationTokenService;
import com.google.cm.mrp.clients.attestation.AttestationTokenServiceExceptions.AttestationTokenServiceException;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.exceptions.UncheckedAeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.AwsParameters;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KeysetReader;
import com.google.crypto.tink.integration.awskmsv2.AwsKmsV2Client;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kms.model.IncorrectKeyException;
import software.amazon.awssdk.services.sts.model.InvalidIdentityTokenException;
import software.amazon.awssdk.services.sts.model.StsException;

/** Gets AEADs from AWS KMS using attested credentials */
public final class AwsAeadProvider implements AeadProvider {
  private static final Logger logger = LoggerFactory.getLogger(AwsAeadProvider.class);

  private static final SdkHttpClient HTTP_CLIENT = ApacheHttpClient.builder().build();
  private static final String KEK_URI_PREFIX = "aws-kms://";
  private static final String FILE_PREFIX = "cfm-aws";
  private static final String ROLE_SESSION_NAME = "cfm-worker";
  private final AttestationTokenService attestationTokenService;
  private final List<Path> tokenFilePaths;
  private static final String[] INVALID_ROLE_MSGS = {
    "'roleArn' failed to satisfy constraint", "Request ARN is invalid"
  };

  private static final String WRONG_KEK_MSG =
      "The key ID in the request does not identify a CMK that can perform this operation";

  private static final String INVALID_AUDIENCE_MSG = "Incorrect token audience";

  private static final String ATTESTATION_MSG =
      "Not authorized to perform sts:AssumeRoleWithWebIdentity";

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

  @Override
  public KeysetHandle readKeysetHandle(KeysetReader dekReader, Aead kekAead)
      throws AeadProviderException {
    try {
      return KeysetHandle.read(dekReader, kekAead);
    } catch (Exception exception) {
      Throwable rootEx = ExceptionUtils.getRootCause(exception);
      if (rootEx instanceof InvalidIdentityTokenException
          && rootEx.getMessage().contains(INVALID_AUDIENCE_MSG)) {
        String msg = "Invalid audience passed to AWS KMS.";
        logger.warn(msg, rootEx);
        throw new AeadProviderException(msg, rootEx, UNAUTHORIZED_AUDIENCE);
      } else if (rootEx instanceof IncorrectKeyException
          && rootEx.getMessage().contains(WRONG_KEK_MSG)) {
        String msg =
            "KEK cannot decrypt DEK, either because it doesn't exist or does not have permission.";
        logger.info(msg, rootEx);
        throw new AeadProviderException(msg, rootEx, INVALID_KEK);

      } else if (rootEx instanceof StsException) {
        String errorMessage = rootEx.getMessage();
        if (Arrays.stream(INVALID_ROLE_MSGS).anyMatch(errorMessage::contains)) {
          String msg = "Invalid role ARN (bad format) passed.";
          logger.warn(msg, rootEx);
          throw new AeadProviderException(msg, rootEx, INVALID_ROLE_FORMAT);
        } else if (errorMessage.contains(ATTESTATION_MSG)) {
          String msg = "Attestation failed, either due to KMS trust conditions or role is invalid.";
          logger.info(msg, rootEx);
          throw new AeadProviderException(msg, rootEx, AWS_AUTH_FAILED);
        }
      }
      String msg = "KeysetHandle read failed for unknown reason.";
      logger.warn(msg, rootEx);
      throw new AeadProviderException(msg, rootEx, DEK_DECRYPTION_ERROR);
    }
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
      if (!kekUri.startsWith(KEK_URI_PREFIX)) {
        kekUri = KEK_URI_PREFIX + kekUri;
      }
      AwsKmsV2Client kmsV2Client = buildAwsV2Client(roleArn, tokenFilePath);
      try {
        return kmsV2Client.getAead(kekUri);
      } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
        String msg = "Invalid format for KEK";
        logger.info(msg, e);
        throw new UncheckedAeadProviderException(msg, e, INVALID_KEK_FORMAT);
      }
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
