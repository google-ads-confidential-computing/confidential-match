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

package com.google.cm.mrp.clients.cryptoclient.converters;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.GcpParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converter for AeadProviderParameters */
public class AeadProviderParametersConverter {
  private static final Logger logger =
      LoggerFactory.getLogger(AeadProviderParametersConverter.class);

  /** Convert {@link DataRecordEncryptionKeys} to {@link AeadProviderParameters} */
  public static AeadProviderParameters convertToAeadProviderParameters(
      DataRecordEncryptionKeys encryptionKeys) {
    switch (encryptionKeys.getKeysCase()) {
      case WRAPPED_ENCRYPTION_KEYS:
        var builder = AeadProviderParameters.builder();
        WrappedEncryptionKeys wrappedKeys = encryptionKeys.getWrappedEncryptionKeys();
        if (wrappedKeys.hasGcpWrappedKeys()) {
          builder.setGcpParameters(
              GcpParameters.builder()
                  .setWipProvider(wrappedKeys.getGcpWrappedKeys().getWipProvider())
                  .build());
        }
        return builder.build();
      case COORDINATOR_KEY:
      case KEYS_NOT_SET:
        logger.error(
            "{} not supported for conversion to AeadProviderParameters.",
            encryptionKeys.getKeysCase());
        break;
    }
    throw new JobProcessorException(
        "AeadProviderParameters attempted to convert invalid DataRecordEncryptionKeys.",
        CRYPTO_CLIENT_CONFIGURATION_ERROR);
  }
}
