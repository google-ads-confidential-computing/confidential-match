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

import static com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo.KeyType.KEY_TYPE_XCHACHA20_POLY1305;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_METADATA_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_PARAMETERS;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_INVALID_KEY_TYPE;

import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo;
import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo.WrappedKeyInfo.KeyType;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.EncryptionMetadataProto;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.CoordinatorKey;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.AwsWrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts EncryptionMetadata-related items */
public final class EncryptionMetadataConverter {

  private static final Logger logger = LoggerFactory.getLogger(EncryptionMetadataConverter.class);

  /**
   * Converts an MRP {@link WrappedKeyInfo.KeyType} proto to a lookup data {@link KeyType} proto to
   * pass to the Lookup Service
   */
  public static KeyType convertToLookupKeyType(WrappedKeyInfo.KeyType keyType) {
    KeyType keyFormat;
    switch (keyType) {
      case XCHACHA20_POLY1305:
        keyFormat = KEY_TYPE_XCHACHA20_POLY1305;
        break;
      case UNSPECIFIED:
      default:
        String message =
            String.format(
                "KeyType %s accepted by MRP but not supported in lookup server proto",
                keyType.name());
        logger.error(message);
        throw new JobProcessorException(message, LOOKUP_SERVICE_INVALID_KEY_TYPE);
    }
    return keyFormat;
  }

  /**
   * Converts a {@link EncryptionMetadataProto.EncryptionMetadata} api proto to a {@link
   * EncryptionMetadata} backend proto
   */
  public static EncryptionMetadata convertToBackendEncryptionMetadata(
      EncryptionMetadataProto.EncryptionMetadata metadata) {
    var backendMetadata = EncryptionMetadata.newBuilder();
    var apiKey = metadata.getEncryptionKeyInfo();
    switch (apiKey.getKeyInfoCase()) {
      case WRAPPED_KEY_INFO:
        backendMetadata
            .getEncryptionKeyInfoBuilder()
            .getWrappedKeyInfoBuilder()
            .setKeyType(
                WrappedKeyInfo.KeyType.valueOf(apiKey.getWrappedKeyInfo().getKeyType().name()))
            .setGcpWrappedKeyInfo(
                GcpWrappedKeyInfo.newBuilder()
                    .setWipProvider(apiKey.getWrappedKeyInfo().getKmsWipProvider().trim()));
        break;
      case AWS_WRAPPED_KEY_INFO:
        var awsInfo = apiKey.getAwsWrappedKeyInfo();
        backendMetadata
            .getEncryptionKeyInfoBuilder()
            .getWrappedKeyInfoBuilder()
            .setKeyType(WrappedKeyInfo.KeyType.valueOf(awsInfo.getKeyType().name()))
            .setAwsWrappedKeyInfo(
                AwsWrappedKeyInfo.newBuilder()
                    .setRoleArn(awsInfo.getRoleArn().trim())
                    .setAudience(awsInfo.getAudience().trim()));
      case COORDINATOR_KEY_INFO:
        apiKey
            .getCoordinatorKeyInfo()
            .getCoordinatorInfoList()
            .forEach(
                coordinatorInfo ->
                    backendMetadata
                        .getEncryptionKeyInfoBuilder()
                        .getCoordinatorKeyInfoBuilder()
                        .addCoordinatorInfoBuilder()
                        .setKeyServiceEndpoint(coordinatorInfo.getKeyServiceEndpoint())
                        .setKmsIdentity(coordinatorInfo.getKmsIdentity())
                        .setKmsWipProvider(coordinatorInfo.getKmsWipProvider())
                        .setKeyServiceAudienceUrl(coordinatorInfo.getKeyServiceAudienceUrl()));
        break;
      default:
        throw new JobProcessorException("Invalid EncryptionKeyInfo", INVALID_PARAMETERS);
    }
    return backendMetadata.build();
  }

  /**
   * Converts an MRP {@link DataRecordEncryptionKeys} proto with an MRP {@link EncryptionMetadata}
   * proto to a lookup data {@link EncryptionKeyInfo} proto to pass to the Lookup Service
   */
  public static EncryptionKeyInfo convertToLookupEncryptionKeyInfo(
      DataRecordEncryptionKeys encryptionKeys, EncryptionMetadata encryptionMetadata) {
    var lookupKey = EncryptionKeyInfo.newBuilder();
    var jobRequestKey = encryptionMetadata.getEncryptionKeyInfo();
    switch (jobRequestKey.getKeyInfoCase()) {
      case WRAPPED_KEY_INFO:
        var jobWrappedKeyInfo = jobRequestKey.getWrappedKeyInfo();
        WrappedEncryptionKeys wrappedEncryptionKeys = encryptionKeys.getWrappedEncryptionKeys();
        var lookupWrappedKeyInfo =
            lookupKey
                .getWrappedKeyInfoBuilder()
                .setEncryptedDek(wrappedEncryptionKeys.getEncryptedDek().trim())
                .setKekKmsResourceId(wrappedEncryptionKeys.getKekUri().trim())
                .setKeyType(convertToLookupKeyType(jobWrappedKeyInfo.getKeyType()));
        if (jobWrappedKeyInfo.hasAwsWrappedKeyInfo()) {
          String roleArn =
              !jobWrappedKeyInfo.getAwsWrappedKeyInfo().getRoleArn().isBlank()
                  ? jobWrappedKeyInfo.getAwsWrappedKeyInfo().getRoleArn()
                  : wrappedEncryptionKeys.getAwsWrappedKeys().getRoleArn();
          lookupWrappedKeyInfo.setAwsWrappedKeyInfo(
              EncryptionKeyInfo.WrappedKeyInfo.AwsWrappedKeyInfo.newBuilder()
                  .setAudience(jobWrappedKeyInfo.getAwsWrappedKeyInfo().getAudience())
                  .setRoleArn(roleArn.trim())
                  .build());
        } else if (jobWrappedKeyInfo.hasGcpWrappedKeyInfo()) {
          String wipProvider =
              !jobWrappedKeyInfo.getGcpWrappedKeyInfo().getWipProvider().isBlank()
                  ? jobWrappedKeyInfo.getGcpWrappedKeyInfo().getWipProvider()
                  : wrappedEncryptionKeys.getGcpWrappedKeys().getWipProvider();
          // TODO(b/384782105): Migrate to use Lookup GCP WrappedKeyInfo
          lookupWrappedKeyInfo.setKmsWipProvider(wipProvider.trim());
        } else {
          String msg = "WrappedKeyInfo must have either AWS or GCP details.";
          logger.error(msg);
          throw new JobProcessorException(msg, ENCRYPTION_METADATA_CONFIG_ERROR);
        }

        break;
      case COORDINATOR_KEY_INFO:
        jobRequestKey
            .getCoordinatorKeyInfo()
            .getCoordinatorInfoList()
            .forEach(
                coordinatorInfo ->
                    lookupKey
                        .getCoordinatorKeyInfoBuilder()
                        .addCoordinatorInfoBuilder()
                        .setKeyServiceEndpoint(coordinatorInfo.getKeyServiceEndpoint())
                        .setKmsIdentity(coordinatorInfo.getKmsIdentity())
                        .setKmsWipProvider(coordinatorInfo.getKmsWipProvider())
                        .setKeyServiceAudienceUrl(coordinatorInfo.getKeyServiceAudienceUrl()));
        lookupKey
            .getCoordinatorKeyInfoBuilder()
            .setKeyId(encryptionKeys.getCoordinatorKey().getKeyId());
        break;
      default:
        String msg = "EncryptionMetadata must have valid encryptionKeyInfo case";
        logger.error(msg);
        throw new JobProcessorException(msg, ENCRYPTION_METADATA_CONFIG_ERROR);
    }
    return lookupKey.build();
  }

  public static DataRecordEncryptionKeys convertToDataEncryptionKeys(
      EncryptionKeyInfo encryptionKeyInfo) {
    var dataRecordEncryptionKeys = DataRecordEncryptionKeys.newBuilder();
    switch (encryptionKeyInfo.getKeyInfoCase()) {
      case WRAPPED_KEY_INFO:
        dataRecordEncryptionKeys.setWrappedEncryptionKeys(
            WrappedEncryptionKeys.newBuilder()
                .setEncryptedDek(encryptionKeyInfo.getWrappedKeyInfo().getEncryptedDek())
                .setKekUri(encryptionKeyInfo.getWrappedKeyInfo().getKekKmsResourceId())
                .build());
        break;
      case COORDINATOR_KEY_INFO:
        dataRecordEncryptionKeys.setCoordinatorKey(
            CoordinatorKey.newBuilder()
                .setKeyId(encryptionKeyInfo.getCoordinatorKeyInfo().getKeyId())
                .build());
        break;
      default:
        String msg = "EncryptionMetadata must have valid encryptionKeyInfo case";
        logger.error(msg);
        throw new JobProcessorException(msg, ENCRYPTION_METADATA_CONFIG_ERROR);
    }
    return dataRecordEncryptionKeys.build();
  }
}
