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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECODING_ERROR;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.io.BaseEncoding.base64Url;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;

/** Abstract class for shared methods between CryptoClients */
public abstract class BaseCryptoClient implements CryptoClient {

  protected static final int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();
  private static final String CRYPTO_CLIENT_RETRY_NAME = "CryptoClientRetry";
  private static final int CRYPTO_CLIENT_RETRY_ATTEMPTS = 5;

  // If KEK or CoordKey has issues decrypting, do not try again for the rest of the job.
  protected ConcurrentHashMap<String, JobResultCode> invalidDecrypters = new ConcurrentHashMap<>();

  protected final Retry retry;

  protected BaseCryptoClient() {
    this.retry =
        Retry.of(
            CRYPTO_CLIENT_RETRY_NAME,
            RetryConfig.custom()
                .maxAttempts(CRYPTO_CLIENT_RETRY_ATTEMPTS)
                .retryOnException(this::isCryptoClientExceptionRetryable)
                .failAfterMaxAttempts(true)
                .intervalFunction(
                    IntervalFunction.ofExponentialBackoff(
                        /* initialIntervalMillis */ 500, /* multiplier */ 1.5))
                .build());
  }

  /** {@inheritDoc} */
  @Override
  public String encrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, String plaintext)
      throws CryptoClientException {
    return encrypt(dataRecordEncryptionKeys, plaintext.getBytes(UTF_8));
  }

  /** {@inheritDoc} */
  @Override
  public String decrypt(
      DataRecordEncryptionKeys dataRecordEncryptionKeys,
      String ciphertext,
      EncodingType encodingType)
      throws CryptoClientException {
    return decrypt(dataRecordEncryptionKeys, decode(ciphertext, encodingType));
  }

  /** Encode bytes into a Base64 String. */
  protected String base64Encode(byte[] value) {
    return base64().encode(value);
  }

  /** Decode a String using the given encodingType. */
  protected byte[] decode(String value, EncodingType encodingType) throws CryptoClientException {
    switch (encodingType) {
      case BASE64:
        if (base64().canDecode(value)) {
          return base64().decode(value);
        }
        break;
      case BASE64URL:
        if (base64Url().canDecode(value)) {
          return base64Url().decode(value);
        }
        break;
      case HEX:
        if (base16().canDecode(value)) {
          return base16().decode(value);
        }
        break;
      case UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        break;
    }
    throw new CryptoClientException(DECODING_ERROR);
  }

  /** Get child class logger */
  protected abstract Logger getLogger();

  /** Determines if decrypt request is retryable */
  protected abstract boolean isCryptoClientExceptionRetryable(Throwable e);
}
