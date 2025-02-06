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

package com.google.cm.mrp.dataprocessor.converters;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_INVALID_ERROR;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts ErrorCode-related items */
public final class ErrorCodeConverter {

  private static final String LOOKUP_SERVICE_CRYPTO_ERROR_CODE = "2415853572";
  private static final String LOOKUP_SERVICE_INVALID_SCHEME_ERROR_CODE = "INVALID_SCHEME";
  private static final Logger logger = LoggerFactory.getLogger(ErrorCodeConverter.class);

  private static final List<String> rowLevelLookupServiceErrorReasons =
      List.of(LOOKUP_SERVICE_CRYPTO_ERROR_CODE);

  /** Converts a LookupServer ErrorCode to an MRP JobResultCode */
  public static JobResultCode convertToJobResultCode(String lookupServerErrorReason) {
    switch (lookupServerErrorReason) {
      case LOOKUP_SERVICE_CRYPTO_ERROR_CODE:
        return DECRYPTION_ERROR;
      default:
        String message = "Unexpected lookupServerErrorReason: " + lookupServerErrorReason;
        logger.error(message);
        throw new JobProcessorException(message, LOOKUP_SERVICE_INVALID_ERROR);
    }
  }

  /** Checks whether an error reason indicates a row level error. */
  public static boolean isValidRowLevelErrorReason(String lookupServerErrorReason) {
    return rowLevelLookupServiceErrorReasons.contains(lookupServerErrorReason);
  }

  /** Checks whether an error reason indicates an invalid sharding scheme. */
  public static boolean isInvalidShardingSchemeErrorReason(String lookupServerErrorReason) {
    return LOOKUP_SERVICE_INVALID_SCHEME_ERROR_CODE.equals(lookupServerErrorReason);
  }
}
