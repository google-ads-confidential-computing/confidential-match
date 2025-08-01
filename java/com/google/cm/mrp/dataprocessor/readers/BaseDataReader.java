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

package com.google.cm.mrp.dataprocessor.readers;

import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase.COORDINATOR_KEY_INFO;
import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase.WRAPPED_KEY_INFO;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DATA_READER_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECODING_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.ENCRYPTION_COLUMNS_CONFIG_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_ENCRYPTION_TYPE;
import static com.google.cm.mrp.backend.SchemaProto.Schema.ColumnEncoding.BASE64_URL;

import com.google.cloud.storage.StorageException;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.AwsWrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices.GcpWrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.common.io.BaseEncoding;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract {@link DataReader} for implementations with logging and decryption. */
public abstract class BaseDataReader implements DataReader {

  private static final Logger logger = LoggerFactory.getLogger(BaseDataReader.class);

  /** Get the logger. */
  protected abstract Logger getLogger();

  /** Create a new {@link DataRecordEncryptionColumns}. */
  protected static DataRecordEncryptionColumns buildDataRecordEncryptionColumns(
      EncryptionKeyColumns matchConfigKeys,
      List<Column> columns,
      EncryptionMetadata encryptionMetadata) {
    var columnsBuilder = DataRecordEncryptionColumns.newBuilder();
    for (int i = 0; i < columns.size(); ++i) {
      Column column = columns.get(i);
      if (column.getEncrypted()) {
        columnsBuilder.addEncryptedColumnIndices(i);
      }
      // TODO(b/410914524): Proto defaults for aliases could cause collision of metadata and
      // encryption columns
      KeyInfoCase keyType = encryptionMetadata.getEncryptionKeyInfo().getKeyInfoCase();
      if (keyType == WRAPPED_KEY_INFO && column.hasColumnAlias()) {
        String dekAlias = matchConfigKeys.getWrappedKeyColumns().getEncryptedDekColumnAlias();
        String kekAlias = matchConfigKeys.getWrappedKeyColumns().getKekUriColumnAlias();
        Optional<String> wipAlias =
            maybeGetWipAlias(matchConfigKeys, encryptionMetadata.getEncryptionKeyInfo());
        Optional<String> roleArnAlias =
            maybeGetRoleArnAlias(matchConfigKeys, encryptionMetadata.getEncryptionKeyInfo());

        if (column.getColumnAlias().equalsIgnoreCase(dekAlias)) {
          columnsBuilder
              .getEncryptionKeyColumnIndicesBuilder()
              .getWrappedKeyColumnIndicesBuilder()
              .setEncryptedDekColumnIndex(i);
        }
        if (column.getColumnAlias().equalsIgnoreCase(kekAlias)) {
          columnsBuilder
              .getEncryptionKeyColumnIndicesBuilder()
              .getWrappedKeyColumnIndicesBuilder()
              .setKekUriColumnIndex(i);
        }
        if (wipAlias.isPresent() && column.getColumnAlias().equalsIgnoreCase(wipAlias.get())) {
          columnsBuilder
              .getEncryptionKeyColumnIndicesBuilder()
              .getWrappedKeyColumnIndicesBuilder()
              .setGcpColumnIndices(
                  GcpWrappedKeyColumnIndices.newBuilder().setWipProviderIndex(i).build());
        }
        if (roleArnAlias.isPresent()
            && column.getColumnAlias().equalsIgnoreCase(roleArnAlias.get())) {
          columnsBuilder
              .getEncryptionKeyColumnIndicesBuilder()
              .getWrappedKeyColumnIndicesBuilder()
              .setAwsColumnIndices(
                  AwsWrappedKeyColumnIndices.newBuilder().setRoleArnIndex(i).build());
        }
      } else if (keyType == COORDINATOR_KEY_INFO && column.hasColumnAlias()) {
        String coordAlias =
            matchConfigKeys.getCoordinatorKeyColumn().getCoordinatorKeyColumnAlias();
        if (column.getColumnAlias().equalsIgnoreCase(coordAlias)) {
          columnsBuilder
              .getEncryptionKeyColumnIndicesBuilder()
              .getCoordinatorKeyColumnIndicesBuilder()
              .setCoordinatorKeyColumnIndex(i);
        }
      }
    }
    DataRecordEncryptionColumns encryptionColumns = columnsBuilder.build();
    validateEncryptionColumns(encryptionMetadata, encryptionColumns);
    return encryptionColumns;
  }

  /** Get the error code from an exception. */
  protected static JobResultCode getErrorCode(Exception ex) {
    if (ex instanceof CryptoClientException) {
      return ((CryptoClientException) ex).getErrorCode();
    } else if (ex instanceof JobProcessorException) {
      return ((JobProcessorException) ex).getErrorCode();
    } else {
      logger.warn("Unknown exception thrown when decrypting: ", ex);
      return DECRYPTION_ERROR;
    }
  }

  /** Determine if the error code represents a row level error. */
  protected static boolean isRowLevelError(JobResultCode errorCode) {
    return errorCode.getNumber() > 200;
  }

  /** Determine if the exception is caused by the input stream. */
  protected static boolean isInputStreamException(Exception e) {
    // Check if nested exceptions contain a Storage exception
    return ExceptionUtils.indexOfType(e, StorageException.class) != -1;
  }

  /** Only fetch WIP alias if needed */
  private static Optional<String> maybeGetWipAlias(
      EncryptionKeyColumns matchConfigKeys, EncryptionKeyInfo requestKeyInfo) {
    // Return empty if not a Wrapped Key or a GCP wrappedKey
    if (!requestKeyInfo.hasWrappedKeyInfo()
        || !requestKeyInfo.getWrappedKeyInfo().hasGcpWrappedKeyInfo()) {
      return Optional.empty();
    }
    // Return empty if WIP is at the request level
    if (!requestKeyInfo.getWrappedKeyInfo().getGcpWrappedKeyInfo().getWipProvider().isBlank()) {
      return Optional.empty();
    }
    // WIP can now only be at the row level, so try to get alias from match config
    if (!matchConfigKeys.getWrappedKeyColumns().hasGcpWrappedKeyColumns()) {
      String msg = "WIP missing in request and no WIP column in match config.";
      logger.error(msg);
      throw new JobProcessorException(msg, ENCRYPTION_COLUMNS_CONFIG_ERROR);
    } else {
      return Optional.of(
          matchConfigKeys.getWrappedKeyColumns().getGcpWrappedKeyColumns().getWipProviderAlias());
    }
  }

  /** Only fetch Role ARN alias if needed */
  private static Optional<String> maybeGetRoleArnAlias(
      EncryptionKeyColumns matchConfigKeys, EncryptionKeyInfo requestKeyInfo) {
    // Return empty if not a Wrapped Key or an AWS wrappedKey
    if (!requestKeyInfo.hasWrappedKeyInfo()
        || !requestKeyInfo.getWrappedKeyInfo().hasAwsWrappedKeyInfo()) {
      return Optional.empty();
    }
    // Return empty if Role ARN is at the request level
    if (!requestKeyInfo.getWrappedKeyInfo().getAwsWrappedKeyInfo().getRoleArn().isBlank()) {
      return Optional.empty();
    }
    // RoleARN can now only be at the row level, so try to get alias from match config
    if (!matchConfigKeys.getWrappedKeyColumns().hasAwsWrappedKeyColumns()) {
      String msg = "Role ARN missing in request and no Role ARN  column in match config.";
      logger.error(msg);
      throw new JobProcessorException(msg, ENCRYPTION_COLUMNS_CONFIG_ERROR);
    } else {
      return Optional.of(
          matchConfigKeys.getWrappedKeyColumns().getAwsWrappedKeyColumns().getRoleArnAlias());
    }
  }

  /** Get encoding type */
  protected EncodingType getEncodingType() {
    return EncodingType.BASE64;
  }

  /** Returns the decrypted string and updates dataRecordBuilder. */
  protected DecryptionResult decryptColumn(
      CryptoClient cryptoClient,
      boolean rowLevelErrorsAllowed,
      Column column,
      String value,
      DataRecordEncryptionKeys encryptionKeys) {
    try {
      // Return without attempting decryption if the value shouldn't be decrypted
      if (!column.getEncrypted() || value.isBlank()) {
        return new DecryptionResult(value, Optional.empty(), /* decryptionSuccessful */ false);
      }
      EncodingType encodingType =
          column.getColumnEncoding() == BASE64_URL ? EncodingType.BASE64URL : getEncodingType();
      String result = cryptoClient.decrypt(encryptionKeys, value, encodingType);
      return new DecryptionResult(result, Optional.empty(), /* decryptionSuccessful */ true);
    } catch (Exception ex) {
      JobResultCode errorCode = getErrorCode(ex);
      String message =
          errorCode == DECODING_ERROR
              ? "Failed to decode column \"" + column.getColumnName() + "\""
              : "Unable to decrypt column \"" + column.getColumnName() + "\"";
      getLogger().info(message, ex);
      if (rowLevelErrorsAllowed && isRowLevelError(errorCode)) {
        return new DecryptionResult(
            value, Optional.of(errorCode), /* decryptionSuccessful */ false);
      } else {
        throw new JobProcessorException(message, ex, errorCode);
      }
    }
  }

  protected String convertHexToBase64(String encryptedInput) {
    String value = encryptedInput.toUpperCase();
    if (!BaseEncoding.base16().canDecode(value)) {
      String msg = "Hex conversion called on nonHex input.";
      logger.error(msg);
      throw new JobProcessorException(msg, DATA_READER_CONFIGURATION_ERROR);
    }
    byte[] bytes = BaseEncoding.base16().decode(value);
    return BaseEncoding.base64().encode(bytes);
  }

  /** Returns decryption results, which may be a valid result or an error code. */
  protected static final class DecryptionResult {
    private final String result;
    private final Optional<JobResultCode> errorCode;
    private final boolean decryptionSuccessful;

    DecryptionResult(
        String result, Optional<JobResultCode> errorCode, boolean decryptionSuccessful) {
      this.result = result;
      this.errorCode = errorCode;
      this.decryptionSuccessful = decryptionSuccessful;
    }

    String getResult() {
      return result;
    }

    Optional<JobResultCode> getErrorCode() {
      return errorCode;
    }

    boolean getDecryptionSuccessful() {
      return decryptionSuccessful;
    }
  }

  private static void validateEncryptionColumns(
      EncryptionMetadata encryptionMetadata, DataRecordEncryptionColumns encryptionColumns) {
    switch (encryptionMetadata.getEncryptionKeyInfo().getKeyInfoCase()) {
      case WRAPPED_KEY_INFO:
        if (!encryptionColumns.getEncryptionKeyColumnIndices().hasWrappedKeyColumnIndices()) {
          String message = "Match config does not support wrapped key parameter from schema.";
          logger.info(message);
          throw new JobProcessorException(message, UNSUPPORTED_ENCRYPTION_TYPE);
        }
        break;
      case COORDINATOR_KEY_INFO:
        if (!encryptionColumns.getEncryptionKeyColumnIndices().hasCoordinatorKeyColumnIndices()) {
          String message = "Match config does not support coordinator key parameter from schema.";
          logger.info(message);
          throw new JobProcessorException(message, UNSUPPORTED_ENCRYPTION_TYPE);
        }
        break;
      default:
        break;
    }
  }
}
