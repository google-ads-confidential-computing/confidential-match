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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.COORDINATOR_KEY_ENCRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.COORDINATOR_KEY_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.COORDINATOR_KEY_SERVICE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.TINK_REGISTRATION_FAILED;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.utils.GcpProviderUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.HybridEncrypt;
import com.google.crypto.tink.hybrid.HybridConfig;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService.KeyFetchException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encrypts and decrypts data by using Tink HybridEncrypt and HybridDecrypt */
public final class HybridCryptoClient extends BaseCryptoClient {

  private static final Logger logger = LoggerFactory.getLogger(HybridCryptoClient.class);

  private static final int MAX_CACHE_SIZE = 10;
  private static final long CACHE_ENTRY_IDLE_EXPIRY = 60 * 15;
  private static final byte[] NO_BYTES = new byte[0];

  private final LoadingCache<String, HybridPair> cache =
      CacheBuilder.newBuilder()
          .maximumSize(MAX_CACHE_SIZE)
          .expireAfterAccess(CACHE_ENTRY_IDLE_EXPIRY, TimeUnit.SECONDS)
          .concurrencyLevel(CONCURRENCY_LEVEL)
          .build(
              new CacheLoader<>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public HybridPair load(final String keyId) throws CryptoClientException {
                  try {
                    HybridDecrypt decrypter = decryptionKeyService.getDecrypter(keyId);
                    HybridEncrypt encrypter = decryptionKeyService.getEncrypter(keyId);
                    return new HybridPair(encrypter, decrypter);
                  } catch (KeyFetchException e) {
                    throwIfWipFailure(keyId, e);
                    logger.warn(
                        String.format(
                            "%s error when fetching the primitives for the key %s",
                            e.getReason().name(), keyId),
                        e);
                    switch (e.getReason()) {
                      case KEY_NOT_FOUND:
                      case KEY_DECRYPTION_ERROR:
                      case INVALID_ARGUMENT:
                        invalidDecrypters.put(keyId, DECRYPTION_ERROR);
                        logger.warn(
                            "Coordinator key could not be used. Will not be retried in job.");
                        throw new CryptoClientException(e, DECRYPTION_ERROR);
                      case PERMISSION_DENIED:
                      case UNAUTHENTICATED:
                        // fall back to throwing a retriable error code.
                        logger.warn(
                            "Authentication/Authorization failed on request to private key service"
                                + " or KMS client.",
                            e.getCause());
                    }
                    // For all other error reasons, send a retryable error code.
                    throw new CryptoClientException(e, COORDINATOR_KEY_SERVICE_ERROR);
                  } catch (IllegalArgumentException e) {
                    logger.warn("Error when calling getDecrypter/getEncrypter", e.getCause());
                    throw new CryptoClientException(e, DECRYPTION_ERROR);
                  }
                }
              });

  private void throwIfWipFailure(String keyId, KeyFetchException exception)
      throws CryptoClientException {
    Optional<AeadProviderException> wipExceptionOpt =
        GcpProviderUtils.tryParseWipException(exception);
    if (wipExceptionOpt.isPresent()) {
      var wipException = wipExceptionOpt.get();
      invalidDecrypters.put(keyId, wipException.getJobResultCode());
      logger.warn(
          "Coordinator split key could not be decrypted due to a configuration WIP failure. Will"
              + " not be retried.");
      throw new CryptoClientException(wipException, wipException.getJobResultCode());
    }
  }

  private final HybridEncryptionKeyService decryptionKeyService;

  @AssistedInject
  public HybridCryptoClient(@Assisted HybridEncryptionKeyService decryptionKeyService)
      throws CryptoClientException {
    this.decryptionKeyService = decryptionKeyService;
    try {
      HybridConfig.register();
    } catch (GeneralSecurityException e) {
      logger.error("Error registering tink HybridConfig.", e);
      throw new CryptoClientException(e, TINK_REGISTRATION_FAILED);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String encrypt(DataRecordEncryptionKeys encryptionKeys, byte[] plaintextBytes)
      throws CryptoClientException {
    validateEncryptionKeys(encryptionKeys);
    HybridEncrypt encrypter = getEncrypter(encryptionKeys.getCoordinatorKey().getKeyId());
    try {
      return base64Encode(encrypter.encrypt(plaintextBytes, NO_BYTES));
    } catch (RuntimeException | GeneralSecurityException e) {
      throw new CryptoClientException(e, COORDINATOR_KEY_ENCRYPTION_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String decrypt(DataRecordEncryptionKeys encryptionKeys, byte[] ciphertextBytes)
      throws CryptoClientException {
    validateEncryptionKeys(encryptionKeys);
    HybridDecrypt decrypter = getDecrypter(encryptionKeys.getCoordinatorKey().getKeyId());
    try {
      return new String(decrypter.decrypt(ciphertextBytes, NO_BYTES), UTF_8);
    } catch (RuntimeException | GeneralSecurityException e) {
      logger.warn("Error when calling decrypter.decrypt.", e.getCause());
      throw new CryptoClientException(e, DECRYPTION_ERROR);
    }
  }

  /** Throw if kek or dek are not present. */
  private static void validateEncryptionKeys(DataRecordEncryptionKeys encryptionKeys)
      throws CryptoClientException {
    if (encryptionKeys.getCoordinatorKey().getKeyId().isBlank()) {
      throw new CryptoClientException(COORDINATOR_KEY_MISSING_IN_RECORD);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Logger getLogger() {
    return logger;
  }

  /**
   * Gets a decryption primitive by key ID. Returns tink.HybridDecrypt. The result is cached
   * according to the specified cache size and expiration time.
   */
  private HybridDecrypt getDecrypter(String keyID) throws CryptoClientException {
    if (invalidDecrypters.containsKey(keyID)) {
      throw new CryptoClientException(
          new RuntimeException("Key " + keyID + " is invalid."), invalidDecrypters.get(keyID));
    }
    try {
      return cache.get(keyID).getDecrypter();
    } catch (ExecutionException | UncheckedExecutionException e) {
      if (e.getCause() instanceof CryptoClientException) {
        throw (CryptoClientException) e.getCause();
      }
      throw new CryptoClientException(e, CRYPTO_CLIENT_ERROR);
    }
  }

  /**
   * Gets a encryption primitive by key ID. Returns tink.HybridEncrypt. The result is cached
   * according to the specified cache size and expiration time.
   */
  private HybridEncrypt getEncrypter(String keyID) throws CryptoClientException {
    if (invalidDecrypters.containsKey(keyID)) {
      throw new CryptoClientException(
          new RuntimeException("Key " + keyID + " is invalid."), invalidDecrypters.get(keyID));
    }
    try {
      return cache.get(keyID).getEncrypter();
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new CryptoClientException(e, CRYPTO_CLIENT_ERROR);
    }
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  private static class HybridPair {
    private final HybridEncrypt encrypter;
    private final HybridDecrypt decrypter;

    public HybridPair(HybridEncrypt encrypter, HybridDecrypt decrypter) {
      this.encrypter = encrypter;
      this.decrypter = decrypter;
    }

    public HybridEncrypt getEncrypter() {
      return encrypter;
    }

    public HybridDecrypt getDecrypter() {
      return decrypter;
    }
  }
}
