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

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;

/**
 * Interface for encrypting and decrypting records by using a given {@link DataRecordEncryptionKeys}
 */
public interface CryptoClient {
  /** Encrypts a String, returning a Base64-encoded String. */
  String encrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, String plaintext)
      throws CryptoClientException;

  /** Encrypts a byte array, returning a Base64-encoded String. */
  String encrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, byte[] plaintextBytes)
      throws CryptoClientException;

  /** Decodes and decrypts a Base64-encoded String, returning an unencoded String. */
  String decrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, String ciphertext)
      throws CryptoClientException;

  /** Decodes and decrypts a Base64 or Base64Url String, returning an unencoded String. */
  String decrypt(
      DataRecordEncryptionKeys dataRecordEncryptionKeys, String ciphertext, boolean isBase64Url)
      throws CryptoClientException;

  /** Decrypts a byte array without decoding, returning an unencoded String. */
  String decrypt(DataRecordEncryptionKeys dataRecordEncryptionKeys, byte[] ciphertextBytes)
      throws CryptoClientException;

  /** Represents an exception thrown by the {@code CryptoClient} class. */
  class CryptoClientException extends Exception {

    private final JobResultCode errorCode;

    /** Creates a new instance of the {@code CryptoClientException} class. */
    public CryptoClientException(Throwable cause) {
      super(cause);
      this.errorCode = JobResultCode.JOB_RESULT_CODE_UNKNOWN;
    }

    /** Creates a new instance of the {@code CryptoClientException} class with error code. */
    public CryptoClientException(JobResultCode errorCode) {
      super(errorCode.name());
      this.errorCode = errorCode;
    }

    /** Creates a new instance of the {@code CryptoClientException} class with error code. */
    public CryptoClientException(Throwable cause, JobResultCode errorCode) {
      super(cause);
      this.errorCode = errorCode;
    }

    /** Returns the error code of the {@code CryptoClientException} instance. */
    public JobResultCode getErrorCode() {
      return errorCode;
    }
  }
}
