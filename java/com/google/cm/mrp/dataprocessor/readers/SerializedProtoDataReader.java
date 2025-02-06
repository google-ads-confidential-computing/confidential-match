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
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.COORDINATOR_KEY_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_MISSING_IN_RECORD;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.ConfidentialMatchDataRecordProto.ConfidentialMatchDataRecord;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.CoordinatorKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.EncryptionKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnType;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.dataprocessor.common.Annotations.InputDataChunkSize;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataReader for Serialized Proto data format files. */
public final class SerializedProtoDataReader extends BaseDataReader {

  private static final Logger logger = LoggerFactory.getLogger(SerializedProtoDataReader.class);
  private final int dataChunkSize;
  private final Schema schema;
  private final Schema internalSchema;
  private final InputStream inputStream;
  private final Scanner reader;
  private final String name;
  private final SuccessMode successMode;
  private final Optional<SerializedProtoDataDecrypter> decrypter;
  private final ConfidentialMatchDataRecordParser confidentialMatchDataRecordParser;

  @AssistedInject
  public SerializedProtoDataReader(
      ConfidentialMatchDataRecordParserFactory confidentialMatchDataRecordParserFactory,
      @InputDataChunkSize Integer dataChunkSize,
      @Assisted InputStream inputStream,
      @Assisted Schema schema,
      @Assisted String name,
      @Assisted MatchConfig matchConfig,
      @Assisted SuccessMode successMode) {
    this.dataChunkSize = dataChunkSize;
    this.inputStream = inputStream;
    this.schema = schema;
    this.internalSchema = generateInternalSchema(schema);
    this.name = name;
    this.reader = new Scanner(inputStream);
    this.successMode = successMode;
    this.confidentialMatchDataRecordParser =
        confidentialMatchDataRecordParserFactory.create(
            matchConfig, this.internalSchema, this.successMode);
    this.decrypter = Optional.empty();
  }

  @AssistedInject
  public SerializedProtoDataReader(
      ConfidentialMatchDataRecordParserFactory confidentialMatchDataRecordParserFactory,
      @InputDataChunkSize Integer dataChunkSize,
      @Assisted InputStream inputStream,
      @Assisted Schema schema,
      @Assisted String name,
      @Assisted MatchConfig matchConfig,
      @Assisted SuccessMode successMode,
      @Assisted EncryptionMetadata encryptionMetadata,
      @Assisted CryptoClient cryptoClient) {
    this.dataChunkSize = dataChunkSize;
    this.inputStream = inputStream;
    this.schema = schema;
    this.internalSchema = generateInternalSchema(schema);
    this.name = name;
    this.reader = new Scanner(inputStream);
    this.successMode = successMode;
    this.decrypter =
        Optional.of(
            new SerializedProtoDataDecrypter(
                successMode,
                cryptoClient,
                matchConfig.getEncryptionKeyColumns(),
                encryptionMetadata,
                internalSchema));
    this.confidentialMatchDataRecordParser =
        confidentialMatchDataRecordParserFactory.create(
            matchConfig, this.internalSchema, this.successMode, encryptionMetadata);
  }

  /** Name of the input file. */
  @Override
  public String getName() {
    return name;
  }

  /** Schema to map the proto fields to. */
  @Override
  public Schema getSchema() {
    return internalSchema;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  /**
   * Reads file with base64 encoded serialized proto {@link ConfidentialMatchDataRecord} per row.
   * The ConfidentialMatchDataRecord is passed to the {@link ConfidentialMatchDataRecordParser} to
   * parse to a list of DataRecord. If encrypted, the data is decrypted.
   *
   * @return {@link DataChunk} - containing DataRecords and updated schema
   */
  @Override
  @SuppressWarnings("UnstableApiUsage") // ImmutableList::builderWithExpectedSize
  public DataChunk next() {
    try {
      if (!hasNext()) {
        throw new NoSuchElementException("SerializedProtoDataReader has no more records to read.");
      }
      var records = ImmutableList.<DataRecord>builderWithExpectedSize(dataChunkSize);
      for (int i = 0; hasNext() && i < dataChunkSize; ++i) {
        ConfidentialMatchDataRecord cfmDataRecord;
        try {
          String inputLine = reader.next();
          if (!BaseEncoding.base64().canDecode(inputLine)) {
            throw new JobProcessorException(
                "Invalid encoding for proto.", INVALID_INPUT_FILE_ERROR);
          }
          cfmDataRecord =
              ConfidentialMatchDataRecord.parseFrom(BaseEncoding.base64().decode(inputLine));
        } catch (InvalidProtocolBufferException e) {
          throw new JobProcessorException("Invalid proto format.", INVALID_INPUT_FILE_ERROR);
        }
        List<DataRecord> modifiedDataRecords =
            confidentialMatchDataRecordParser.parse(cfmDataRecord).stream()
                .map(dataRecord -> populateEncryptionKeyValues(dataRecord))
                .collect(Collectors.toList());
        records.addAll(modifiedDataRecords);
      }

      return DataChunk.builder()
          .setSchema(internalSchema)
          .setInputSchema(schema)
          .setEncryptionColumns(
              decrypter.map(SerializedProtoDataDecrypter::getDataRecordEncryptionColumns))
          .setRecords(records.build())
          .build();
    } catch (UncheckedIOException e) {
      if (isInputStreamException(e)) {
        String message = "Could not read proto text file from input stream.";
        logger.error(message);
        throw new JobProcessorException(message, e, INPUT_FILE_READ_ERROR);
      }
      String message = "Could not read proto text file, make sure input data is correct.";
      logger.error(message);
      throw new JobProcessorException(message, e, INVALID_INPUT_FILE_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    reader.close();
    inputStream.close();
  }

  /** {@inheritDoc} */
  @Override
  protected Logger getLogger() {
    return logger;
  }

  private DataRecord populateEncryptionKeyValues(DataRecord dataRecord) {
    if (dataRecord.hasErrorCode()) {
      return dataRecord;
    }
    var dataRecordBuilder = dataRecord.toBuilder();

    Optional<DataRecordEncryptionKeys> encryptionKeys;
    try {
      // If the encryption keys are present, so is the decrypter
      encryptionKeys = decrypter.map(d -> d.getEncryptionKeyValues(dataRecord));
    } catch (JobProcessorException ex) {
      JobResultCode errorCode = getErrorCode(ex);
      if (decrypter.map(SerializedProtoDataDecrypter::getRowLevelErrorsAllowed).orElse(false)
          && isRowLevelError(errorCode)) {
        encryptionKeys = Optional.empty();
        dataRecordBuilder.setErrorCode(errorCode);
      } else {
        throw new JobProcessorException("Error parsing encryption keys.", ex, errorCode);
      }
    }

    for (int i = 0; i < internalSchema.getColumnsCount(); ++i) {
      Column column = internalSchema.getColumns(i);
      switch (column.getColumnType()) {
        case INT:
        case DOUBLE:
        case BOOL:
          break;
        case STRING:
        default: // Missing type implies String
          if (!dataRecord.getKeyValues(i).hasStringValue()) {
            break;
          }
          String value = dataRecord.getKeyValues(i).getStringValue();
          if (encryptionKeys.isPresent()) {
            DecryptionResult result =
                decrypter.get().decryptColumn(column, value, encryptionKeys.get());
            dataRecordBuilder.setKeyValues(
                i,
                KeyValue.newBuilder()
                    .setKey(dataRecord.getKeyValues(i).getKey())
                    .setStringValue(result.getResult())
                    .build());
            if (result.getErrorCode().isPresent()) {
              dataRecordBuilder.setErrorCode(result.getErrorCode().get());
            } else if (result.getDecryptionSuccessful()) {
              dataRecordBuilder.putEncryptedKeyValues(i, value);
            }
          }
      }
    }
    return dataRecordBuilder.build();
  }

  /** Returns a schema for internal use with column names and row marker column. */
  private Schema generateInternalSchema(Schema inputSchema) {
    Schema.Builder inputSchemaBuilder = inputSchema.toBuilder();
    List<Column> columnList =
        inputSchemaBuilder.getColumnsList().stream()
            .map(
                column ->
                    column.toBuilder()
                        .setColumnType(column.getColumnType())
                        .setColumnAlias(column.getColumnAlias())
                        .setColumnName(column.getColumnAlias())
                        .build())
            .collect(Collectors.toList());
    columnList.add(
        Column.newBuilder()
            .setColumnAlias(ROW_MARKER_COLUMN_NAME)
            .setColumnName(ROW_MARKER_COLUMN_NAME)
            .setColumnType(ColumnType.STRING)
            .build());
    return inputSchemaBuilder.clearColumns().addAllColumns(columnList).build();
  }

  /** Inner class for handling decryption. */
  private final class SerializedProtoDataDecrypter {
    final boolean rowLevelErrorsAllowed;
    final CryptoClient cryptoClient;
    final EncryptionMetadata encryptionMetadata;
    final DataRecordEncryptionColumns dataRecordEncryptionColumns;

    SerializedProtoDataDecrypter(
        SuccessMode successMode,
        CryptoClient cryptoClient,
        EncryptionKeyColumns encryptionKeyColumns,
        EncryptionMetadata encryptionMetadata,
        Schema schema) {
      this.cryptoClient = cryptoClient;
      this.encryptionMetadata = encryptionMetadata;
      rowLevelErrorsAllowed = successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS;
      dataRecordEncryptionColumns =
          buildDataRecordEncryptionColumns(
              encryptionKeyColumns, schema.getColumnsList(), encryptionMetadata);
    }

    DataRecordEncryptionColumns getDataRecordEncryptionColumns() {
      return dataRecordEncryptionColumns;
    }

    EncryptionMetadata getEncryptionMetadata() {
      return encryptionMetadata;
    }

    boolean getRowLevelErrorsAllowed() {
      return rowLevelErrorsAllowed;
    }

    DecryptionResult decryptColumn(
        Column column, String value, DataRecordEncryptionKeys encryptionKeys) {
      return SerializedProtoDataReader.this.decryptColumn(
          cryptoClient, rowLevelErrorsAllowed, column, value, encryptionKeys);
    }

    /** Get encryption key values for a record. */
    DataRecordEncryptionKeys getEncryptionKeyValues(DataRecord dataRecord) {
      try {
        var keysBuilder = DataRecordEncryptionKeys.newBuilder();
        KeyInfoCase keyType = encryptionMetadata.getEncryptionKeyInfo().getKeyInfoCase();
        EncryptionKeyColumnIndices encryptionIndices =
            dataRecordEncryptionColumns.getEncryptionKeyColumnIndices();
        if (keyType == WRAPPED_KEY_INFO) {
          WrappedKeyColumnIndices wrappedKeyIndices =
              encryptionIndices.getWrappedKeyColumnIndices();
          String dek =
              dataRecord
                  .getKeyValues(wrappedKeyIndices.getEncryptedDekColumnIndex())
                  .getStringValue();
          String kekUri =
              dataRecord.getKeyValues(wrappedKeyIndices.getKekUriColumnIndex()).getStringValue();
          if (kekUri.isBlank()) {
            throw new JobProcessorException(KEK_MISSING_IN_RECORD.name(), KEK_MISSING_IN_RECORD);
          }
          if (dek.isBlank()) {
            throw new JobProcessorException(DEK_MISSING_IN_RECORD.name(), DEK_MISSING_IN_RECORD);
          }
          if (wrappedKeyIndices.hasGcpColumnIndices()) {
            String wipValue =
                dataRecord
                    .getKeyValues(wrappedKeyIndices.getGcpColumnIndices().getWipProviderIndex())
                    .getStringValue();
            if (wipValue.isBlank()) {
              throw new JobProcessorException(WIP_MISSING_IN_RECORD.name(), WIP_MISSING_IN_RECORD);
            }
            keysBuilder
                .getWrappedEncryptionKeysBuilder()
                .setGcpWrappedKeys(GcpWrappedKeys.newBuilder().setWipProvider(wipValue));
          }

          keysBuilder.getWrappedEncryptionKeysBuilder().setEncryptedDek(dek);
          keysBuilder.getWrappedEncryptionKeysBuilder().setKekUri(kekUri);
        }
        if (keyType == COORDINATOR_KEY_INFO) {
          CoordinatorKeyColumnIndices coordKeyIndices =
              encryptionIndices.getCoordinatorKeyColumnIndices();
          String coordKey =
              dataRecord
                  .getKeyValues(coordKeyIndices.getCoordinatorKeyColumnIndex())
                  .getStringValue();
          if (coordKey.isBlank()) {
            throw new JobProcessorException(
                COORDINATOR_KEY_MISSING_IN_RECORD.name(), COORDINATOR_KEY_MISSING_IN_RECORD);
          }
          keysBuilder.getCoordinatorKeyBuilder().setKeyId(coordKey);
        }
        DataRecordEncryptionKeys encryptionKeys = keysBuilder.build();
        return encryptionKeys;
      } catch (NoSuchElementException ex) {
        // Should never be thrown
        String message = "Encryption columns were not found in the schema.";
        logger.info(message);
        throw new JobProcessorException(message, MISSING_ENCRYPTION_COLUMN);
      } catch (ArrayIndexOutOfBoundsException ex) {
        // Should never be thrown
        String message = "Encryption column index out of bounds of record.";
        logger.info(message);
        throw new JobProcessorException(message, MISSING_ENCRYPTION_COLUMN);
      }
    }
  }
}
