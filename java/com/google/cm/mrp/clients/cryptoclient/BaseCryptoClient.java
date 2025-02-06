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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_WIP_PARAMETER;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_AUTH_FAILED;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.io.BaseEncoding.base64Url;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

/** Abstract class for shared methods between CryptoClients */
public abstract class BaseCryptoClient implements CryptoClient {

  protected static final int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();

  // If KEK or CoordKey has issues decrypting, do not try again for the rest of the job.
  protected ConcurrentHashMap.KeySetView<String, Boolean> invalidDecrypters =
      ConcurrentHashMap.newKeySet();

  private static final String EXPECTED_EXCEPTION = "OAuthException";
  private static final String[] INVALID_WIP_CODES = {"invalid_target", "invalid_request"};
  private static final String WIP_CONDITION_FAILED_CODE = "unauthorized_client";

  /** {@inheritDoc} */
  @Override
  public String encrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, String plaintext)
      throws CryptoClientException {
    return encrypt(dataRecordEncryptionKeys, plaintext.getBytes(UTF_8));
  }

  /** {@inheritDoc} */
  @Override
  public String decrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, String ciphertext)
      throws CryptoClientException {
    return decrypt(dataRecordEncryptionKeys, ciphertext, false);
  }

  /** {@inheritDoc} */
  @Override
  public String decrypt(
      DataRecordEncryptionKeys dataRecordEncryptionKeys, String ciphertext, boolean isBase64Url)
      throws CryptoClientException {
    return decrypt(dataRecordEncryptionKeys, base64Decode(ciphertext, isBase64Url));
  }

  /** Encode bytes into a Base64 String. */
  protected String base64Encode(byte[] value) {
    return base64().encode(value);
  }

  /** Decode a String from Base64 or Base64Url. */
  protected byte[] base64Decode(String value, boolean isBase64Url) throws CryptoClientException {
    if (isBase64Url && base64Url().canDecode(value)) {
      return base64Url().decode(value);
    }
    if (!isBase64Url && base64().canDecode(value)) {
      return base64().decode(value);
    }
    throw new CryptoClientException(DECODING_ERROR);
  }

  /** Get child class logger */
  protected abstract Logger getLogger();

  /** Check exception for wrapped WIP (Workload Identity Pool) failures */
  protected void throwIfWipFailure(Exception e) throws CryptoClientException {
    var rootEx = ExceptionUtils.getRootCause(e);
    // OAuthException is package private so we cannot explicitly check against it.
    // Checking name as next-best solution
    if (rootEx.getClass().getSimpleName().equals(EXPECTED_EXCEPTION)) {
      if (Arrays.stream(INVALID_WIP_CODES).anyMatch(code -> rootEx.getMessage().contains(code))) {
        getLogger().warn("WIP parameter invalid", rootEx);
        throw new CryptoClientException(rootEx, INVALID_WIP_PARAMETER);
      } else if (rootEx.getMessage().contains(WIP_CONDITION_FAILED_CODE)) {
        getLogger().warn("WIP conditions failed", rootEx);
        throw new CryptoClientException(rootEx, WIP_AUTH_FAILED);
      }
    }
  }
}
