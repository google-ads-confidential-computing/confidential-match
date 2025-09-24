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

package com.google.cm.mrp.clients.cryptoclient.utils;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_WIP_FORMAT;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_WIP_PARAMETER;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_PERMISSION_DENIED;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_AUTH_FAILED;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to host reusable methods among GCP KMS providers */
public final class GcpProviderUtils {

  private static final Logger logger = LoggerFactory.getLogger(GcpProviderUtils.class);

  private static final String WIP_AUTH_EXCEPTION = "OAuthException";
  private static final String INVALID_WIP_FORMAT_CODE = "invalid_request";
  private static final String INVALID_WIP_CODE = "invalid_target";
  private static final String WIP_CONDITION_FAILED_CODE = "unauthorized_client";
  private static final String INVALID_CYPHERTEXT = "ciphertext is invalid";

  private GcpProviderUtils() {}

  /** Handles potential GCP Cloud KMS errors and converts to AeadProviderException */
  public static Optional<AeadProviderException> tryParseGcpKmsException(Exception e) {
    var rootEx = ExceptionUtils.getRootCause(e);
    // First check WIP failures
    Optional<AeadProviderException> exceptionFromWip = tryParseWipException(rootEx);
    if (exceptionFromWip.isPresent()) {
      return exceptionFromWip;
    }
    // OAuthException is package private so we cannot explicitly check against it.
    // Checking name as next-best solution
    if (rootEx instanceof GoogleJsonResponseException) {
      var apiResponse = (GoogleJsonResponseException) rootEx;
      int statusCode = apiResponse.getDetails().getCode();
      if (statusCode == 400) {
        if (apiResponse.getDetails().getMessage() != null
            && apiResponse.getDetails().getMessage().contains(INVALID_CYPHERTEXT)) {
          String msg = "Cloud KMS marked DEK as invalid and cannot be decrypted.";
          logger.warn(msg, rootEx);
          return Optional.of(new AeadProviderException(msg, rootEx, DEK_DECRYPTION_ERROR));
        } else {
          String msg = "KEK could not decrypt data, most likely incorrect KEK.";
          logger.warn(msg, rootEx);
          return Optional.of(new AeadProviderException(msg, rootEx, INVALID_KEK));
        }
      } else if (statusCode == 403) {
        String msg = "Permission denied when trying to use KEK.";
        logger.warn(msg, rootEx);
        return Optional.of(new AeadProviderException(msg, rootEx, KEK_PERMISSION_DENIED));
      }
    }
    return Optional.empty();
  }

  /** Handle errors for wrapped WIP (Workload Identity Pool) failures */
  public static Optional<AeadProviderException> tryParseWipException(Throwable e) {
    Throwable rootEx = ExceptionUtils.getRootCause(e);
    // OAuthException is package private so we cannot explicitly check against it.
    // Checking name as next-best solution
    if (rootEx.getClass().getSimpleName().equals(WIP_AUTH_EXCEPTION)) {
      if (rootEx.getMessage().contains(INVALID_WIP_CODE)) {
        String msg = "WIP parameter invalid.";
        logger.warn(msg, rootEx);
        return Optional.of(new AeadProviderException(msg, rootEx, INVALID_WIP_PARAMETER));
      } else if (rootEx.getMessage().contains(INVALID_WIP_FORMAT_CODE)) {
        String msg = "WIP parameter in an invalid format.";
        logger.warn(msg, rootEx);
        return Optional.of(new AeadProviderException(msg, rootEx, INVALID_WIP_FORMAT));
      } else if (rootEx.getMessage().contains(WIP_CONDITION_FAILED_CODE)) {
        String msg = "WIP conditions failed.";
        logger.warn(msg, rootEx);
        return Optional.of(new AeadProviderException(msg, rootEx, WIP_AUTH_FAILED));
      }
    }
    return Optional.empty();
  }
}
