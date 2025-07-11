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

package com.google.cm.mrp.clients.cryptoclient;

import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_KEY_TYPE_MISMATCH;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ROLE_ARN_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_DEK_KEY_TYPE;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_MISSING_IN_RECORD;
import static com.google.crypto.tink.LegacyKeysetSerialization.getKeysetInfo;
import static com.google.crypto.tink.aead.AeadConfig.XCHACHA20_POLY1305_TYPE_URL;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.AwsWrappedKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.AwsWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.clients.cryptoclient.converters.AeadProviderParametersConverter;
import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.exceptions.UncheckedAeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.DekKeysetReader;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KeysetReader;
import com.google.crypto.tink.proto.KeysetInfo.KeyInfo;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encrypts and decrypts data by using Tink AEAD */
public final class AeadCryptoClient extends BaseCryptoClient {

  private static final Logger logger = LoggerFactory.getLogger(AeadCryptoClient.class);

  private static final int MAX_CACHE_SIZE = 150;
  private static final long CACHE_ENTRY_IDLE_EXPIRY = 300;
  private static final byte[] NO_BYTES = new byte[0];
  private static final ImmutableMap<KeyType, String> KEY_TYPE_VALIDATION_MAP =
      ImmutableMap.of(XCHACHA20_POLY1305, XCHACHA20_POLY1305_TYPE_URL);

  private final LoadingCache<DataRecordEncryptionKeys, Aead> dekCache =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_CACHE_SIZE)
          .expireAfterAccess(CACHE_ENTRY_IDLE_EXPIRY, TimeUnit.SECONDS)
          .concurrencyLevel(CONCURRENCY_LEVEL)
          .build(
              new CacheLoader<>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public Aead load(final DataRecordEncryptionKeys encryptionKeys)
                    throws CryptoClientException {
                  try {
                    WrappedEncryptionKeys keys = encryptionKeys.getWrappedEncryptionKeys();
                    String kekUri = keys.getKekUri().trim();

                    // DEK is always in Base64
                    byte[] dekBytes = decode(keys.getEncryptedDek().trim(), EncodingType.BASE64);
                    KeysetReader reader = new DekKeysetReader(ByteString.copyFrom(dekBytes));
                    Aead kmsAead =
                        aeadProvider
                            .getAeadSelector(
                                AeadProviderParametersConverter.convertToAeadProviderParameters(
                                    encryptionKeys))
                            .getAead(kekUri);
                    KeysetHandle dekKeyset = aeadProvider.readKeysetHandle(reader, kmsAead);
                    // Validate that all key types match the job's key type
                    for (KeyInfo key : getKeysetInfo(dekKeyset).getKeyInfoList()) {
                      if (!keyTypeUrl.equals(key.getTypeUrl())) {
                        String message = "Unexpected DEK type: " + key.getTypeUrl();
                        logger.warn(message);
                        throw new CryptoClientException(DEK_KEY_TYPE_MISMATCH);
                      }
                    }
                    return dekKeyset.getPrimitive(Aead.class);
                  } catch (AeadProviderException | UncheckedAeadProviderException e) {
                    logger.warn(
                        "KEK could not be used in AeadProvider. Will not be retried in job.", e);
                    invalidDecrypters.put(encryptionKeys.toString(), e.getJobResultCode());
                    throw new CryptoClientException(e, e.getJobResultCode());
                  } catch (GeneralSecurityException | RuntimeException e) {
                    logger.warn(
                        "DEK could not be decrypted with given KEK. Will not be retried in job.",
                        e);
                    invalidDecrypters.put(encryptionKeys.toString(), DEK_DECRYPTION_ERROR);
                    throw new CryptoClientException(e, DEK_DECRYPTION_ERROR);
                  }
                }
              });

  private final AeadProvider aeadProvider;
  private final EncryptionKeyInfo encryptionKeyInfo;
  private final String keyTypeUrl;

  @AssistedInject
  public AeadCryptoClient(
      @Assisted AeadProvider aeadProvider, @Assisted EncryptionKeyInfo encryptionKeyInfo)
      throws CryptoClientException {
    this.aeadProvider = aeadProvider;
    this.encryptionKeyInfo = encryptionKeyInfo;
    try {
      KeyType keyType = encryptionKeyInfo.getWrappedKeyInfo().getKeyType();
      String keyTypeUrl = KEY_TYPE_VALIDATION_MAP.get(keyType);
      this.keyTypeUrl = requireNonNull(keyTypeUrl, "Unsupported key type: " + keyType.name());
    } catch (NullPointerException ex) {
      throw new CryptoClientException(ex, UNSUPPORTED_DEK_KEY_TYPE);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String encrypt(DataRecordEncryptionKeys encryptionKeys, byte[] plaintextBytes)
      throws CryptoClientException {
    Aead aead = decryptDek(encryptionKeys);
    try {
      return base64Encode(aead.encrypt(plaintextBytes, NO_BYTES));
    } catch (RuntimeException | GeneralSecurityException e) {
      throw new CryptoClientException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String decrypt(DataRecordEncryptionKeys encryptionKeys, byte[] ciphertextBytes)
      throws CryptoClientException {
    Aead aead = decryptDek(encryptionKeys);
    try {
      return new String(aead.decrypt(ciphertextBytes, NO_BYTES), UTF_8);
    } catch (RuntimeException | GeneralSecurityException e) {
      throw new CryptoClientException(e, DECRYPTION_ERROR);
    }
  }

  /** Closes any crypto-client resources that need it */
  @Override
  public void close() throws IOException {
    aeadProvider.close();
  }

  /** {@inheritDoc} */
  @Override
  protected Logger getLogger() {
    return logger;
  }

  /** Throw if KEK or DEK are not present. */
  private static void validateEncryptionKeys(DataRecordEncryptionKeys encryptionKeys)
      throws CryptoClientException {
    if (encryptionKeys.getWrappedEncryptionKeys().getEncryptedDek().isBlank()) {
      throw new CryptoClientException(DEK_MISSING_IN_RECORD);
    }
    if (encryptionKeys.getWrappedEncryptionKeys().getKekUri().isBlank()) {
      throw new CryptoClientException(KEK_MISSING_IN_RECORD);
    }
  }

  /**
   * Decrypts an encrypted KeysetHandle using the given KMS KEK URI contained inside a {@link
   * DataRecordEncryptionKeys} proto. Returns KeysetHandle primitive. The result is cached for the
   * value listed in CACHE_ENTRY_TTL_SEC.
   */
  private Aead decryptDek(DataRecordEncryptionKeys dataRecordEncryptionKeys)
      throws CryptoClientException {
    validateEncryptionKeys(dataRecordEncryptionKeys);
    DataRecordEncryptionKeys encryptionKeysWithKmsParams =
        addOrValidateKmsEncryptionKeyParams(dataRecordEncryptionKeys);
    try {
      String key = encryptionKeysWithKmsParams.toString();
      if (invalidDecrypters.containsKey(key)) {
        throw new CryptoClientException(invalidDecrypters.get(key));
      }
      return dekCache.get(encryptionKeysWithKmsParams);
    } catch (ExecutionException | UncheckedExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof CryptoClientException) {
        throw (CryptoClientException) e.getCause();
      }
      throw new CryptoClientException(e, DEK_DECRYPTION_ERROR);
    }
  }

  private DataRecordEncryptionKeys addOrValidateKmsEncryptionKeyParams(
      DataRecordEncryptionKeys encryptionKeys) throws CryptoClientException {
    if (encryptionKeyInfo.getWrappedKeyInfo().hasGcpWrappedKeyInfo()) {
      return addGcpKeyMetadata(encryptionKeys);
    } else if (encryptionKeyInfo.getWrappedKeyInfo().hasAwsWrappedKeyInfo()) {
      return addAwsKeyMetadata(encryptionKeys);
    } else {
      String msg = "CryptoClient did not find GCP or AWS wrappedKeyInfo from job.";
      logger.error(msg);
      throw new CryptoClientException(CRYPTO_CLIENT_CONFIGURATION_ERROR);
    }
  }

  /** Adds role ARN and audience either from job-level or from record-level. */
  private DataRecordEncryptionKeys addAwsKeyMetadata(DataRecordEncryptionKeys encryptionKeys)
      throws CryptoClientException {
    AwsWrappedKeyInfo awsJobLevelInfo =
        encryptionKeyInfo.getWrappedKeyInfo().getAwsWrappedKeyInfo();
    WrappedEncryptionKeys wrappedEncryptionKeys = encryptionKeys.getWrappedEncryptionKeys();

    // default to upstream config
    var awsWrappedKeys = wrappedEncryptionKeys.getAwsWrappedKeys().toBuilder();
    if (!awsJobLevelInfo.getRoleArn().isBlank()) {
      awsWrappedKeys = AwsWrappedKeys.newBuilder().setRoleArn(awsJobLevelInfo.getRoleArn());
    } else if (awsWrappedKeys.getRoleArn().isBlank()) {
      throw new CryptoClientException(ROLE_ARN_MISSING_IN_RECORD);
    }

    if (!awsJobLevelInfo.getAudience().isBlank()) {
      awsWrappedKeys.setAudience(awsJobLevelInfo.getAudience());
    }
    return encryptionKeys.toBuilder()
        .setWrappedEncryptionKeys(
            wrappedEncryptionKeys.toBuilder().setAwsWrappedKeys(awsWrappedKeys))
        .build();
  }

  /** Use WIP for job if present, else require WIP to be present in DataRecordEncryptionKeys. */
  private DataRecordEncryptionKeys addGcpKeyMetadata(DataRecordEncryptionKeys encryptionKeys)
      throws CryptoClientException {
    GcpWrappedKeyInfo gcpJobLevelInfo =
        encryptionKeyInfo.getWrappedKeyInfo().getGcpWrappedKeyInfo();
    WrappedEncryptionKeys wrappedEncryptionKeys = encryptionKeys.getWrappedEncryptionKeys();
    if (!gcpJobLevelInfo.getWipProvider().isBlank()) {
      return encryptionKeys.toBuilder()
          .setWrappedEncryptionKeys(
              wrappedEncryptionKeys.toBuilder()
                  .setGcpWrappedKeys(
                      GcpWrappedKeys.newBuilder().setWipProvider(gcpJobLevelInfo.getWipProvider())))
          .build();
    } else if (wrappedEncryptionKeys.getGcpWrappedKeys().getWipProvider().isBlank()) {
      throw new CryptoClientException(WIP_MISSING_IN_RECORD);
    } else {
      return encryptionKeys;
    }
  }
}
