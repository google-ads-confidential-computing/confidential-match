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

import static com.google.cm.mrp.backend.EncodingTypeProto.EncodingType.HEX;
import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase.COORDINATOR_KEY_INFO;
import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase.WRAPPED_KEY_INFO;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.COORDINATOR_KEY_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DATA_READER_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_MISSING_IN_RECORD;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_ENCRYPTION_COLUMN;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.WIP_MISSING_IN_RECORD;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.CoordinatorKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.EncryptionKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionColumns.WrappedKeyColumnIndices;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo.KeyInfoCase;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.dataprocessor.common.Annotations.InputDataChunkSize;
import com.google.cm.mrp.dataprocessor.converters.SchemaConverter;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.models.JobParameters;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataReader for CSV files. */
public final class CsvDataReader extends BaseDataReader {

  private static final Logger logger = LoggerFactory.getLogger(CsvDataReader.class);
  private final int dataChunkSize;
  private final Schema schema;
  private final InputStream inputStream;
  private final Supplier<CSVParser> csvParser;
  private final String name;

  // only used for encrypted data
  private final Optional<EncodingType> encodingType;
  private final Optional<CSVDataDecrypter> decrypter;

  @AssistedInject
  public CsvDataReader(
      @InputDataChunkSize Integer dataChunkSize,
      @Assisted InputStream inputStream,
      @Assisted Schema schema,
      @Assisted String name,
      @Assisted SuccessMode successMode /* unused */) {
    this.dataChunkSize = dataChunkSize;
    this.inputStream = inputStream;
    this.schema = schema;
    this.name = name;
    // Defer creating a CSVParser until needed
    csvParser = Suppliers.memoize(this::initCsvParser);
    decrypter = Optional.empty();
    encodingType = Optional.empty();
  }

  @AssistedInject
  public CsvDataReader(
      @InputDataChunkSize Integer dataChunkSize,
      @Assisted InputStream inputStream,
      @Assisted Schema schema,
      @Assisted String name,
      @Assisted JobParameters jobParameters,
      @Assisted EncryptionKeyColumns encryptionColumns,
      @Assisted SuccessMode successMode,
      @Assisted CryptoClient cryptoClient) {
    this.dataChunkSize = dataChunkSize;
    this.inputStream = inputStream;
    this.schema = schema;
    this.name = name;
    this.encodingType = jobParameters.encodingType();
    // Defer creating a CSVParser until needed
    csvParser = Suppliers.memoize(this::initCsvParser);
    decrypter =
        Optional.of(
            new CSVDataDecrypter(
                successMode,
                cryptoClient,
                encryptionColumns,
                jobParameters
                    .encryptionMetadata()
                    .orElseThrow(
                        () ->
                            new JobProcessorException(
                                "Encryption metadata required for encrypted inputs",
                                DATA_READER_CONFIGURATION_ERROR)),
                schema));
  }

  /** Name of the CSV file. */
  @Override
  public String getName() {
    return name;
  }

  /** Schema used for parsing the CSV file. */
  @Override
  public Schema getSchema() {
    return schema;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    try {
      return csvParser.get().iterator().hasNext();
    } catch (UncheckedIOException e) {
      throw convertToJobException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("UnstableApiUsage") // ImmutableList::builderWithExpectedSize
  public DataChunk next() {
    try {
      if (!hasNext()) {
        throw new NoSuchElementException("CSVDataReader has no more records to read.");
      }
      Iterator<CSVRecord> csvRecords = csvParser.get().iterator();
      var records = ImmutableList.<DataRecord>builderWithExpectedSize(dataChunkSize);
      for (int i = 0; hasNext() && i < dataChunkSize; ++i) {
        records.add(toDataRecord(csvRecords.next()));
      }
      return DataChunk.builder()
          .setSchema(schema)
          .setEncryptionColumns(decrypter.map(CSVDataDecrypter::getDataRecordEncryptionColumns))
          .setRecords(records.build())
          .build();
    } catch (UncheckedIOException e) {
      throw convertToJobException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  /** {@inheritDoc} */
  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  protected EncodingType getEncodingType() {
    return encodingType.orElseThrow(
        () ->
            new JobProcessorException(
                "Encoding type invoked for hashed data", DATA_READER_CONFIGURATION_ERROR));
  }

  private JobProcessorException convertToJobException(Exception e) {
    if (isInputStreamException(e)) {
      String message = "Could not read CSV file from input stream.";
      logger.error(message);
      throw new JobProcessorException(message, e, INPUT_FILE_READ_ERROR);
    }
    String message = "Could not read CSV file, make sure input data is correct.";
    logger.error(message);
    throw new JobProcessorException(message, e, INVALID_INPUT_FILE_ERROR);
  }

  private CSVParser initCsvParser() {
    // Create parser
    try {
      return CSVFormat.DEFAULT
          .builder()
          .setSkipHeaderRecord(schema.getSkipHeaderRecord())
          .setIgnoreHeaderCase(true)
          .setHeader(SchemaConverter.convertToColumnNames(schema))
          .build()
          .parse(new InputStreamReader(inputStream));
    } catch (JobProcessorException ex) {
      throw ex;
    } catch (IOException | RuntimeException ex) {
      String message = "Invalid input file.";
      logger.error(message, ex);
      throw new JobProcessorException(message, ex, INVALID_INPUT_FILE_ERROR);
    }
  }

  private DataRecord toDataRecord(CSVRecord csvRecord) {
    var dataRecordBuilder = DataRecord.newBuilder();

    // Validate encryption keys for this record
    Optional<DataRecordEncryptionKeys> encryptionKeys;
    try {
      // If the encryption keys are present, so is the decrypter
      encryptionKeys = decrypter.map(d -> d.getEncryptionKeyValues(csvRecord));
    } catch (JobProcessorException ex) {
      JobResultCode errorCode = getErrorCode(ex);
      if (decrypter.map(CSVDataDecrypter::getRowLevelErrorsAllowed).orElse(false)
          && isRowLevelError(errorCode)) {
        encryptionKeys = Optional.empty();
        dataRecordBuilder.setErrorCode(errorCode);
      } else {
        throw new JobProcessorException("Error parsing encryption keys.", ex, errorCode);
      }
    }

    for (int i = 0; i < schema.getColumnsCount(); ++i) {
      Column column = schema.getColumns(i);
      String value = csvRecord.get(column.getColumnName());
      var keyValueBuilder = dataRecordBuilder.addKeyValuesBuilder().setKey(column.getColumnName());
      // If the column type is not provided, it defaults to UNRECOGNIZED
      switch (column.getColumnType()) {
        case INT:
          keyValueBuilder.setIntValue(Integer.parseInt(value));
          break;
        case DOUBLE:
          keyValueBuilder.setDoubleValue(Double.parseDouble(value));
          break;
        case BOOL:
          keyValueBuilder.setBoolValue(Boolean.parseBoolean(value));
          break;
        case STRING:
        // fallthrough
        default:
          // Missing type implies String
          if (encryptionKeys.isPresent()) {
            DecryptionResult result =
                decrypter.get().decryptColumn(column, value, encryptionKeys.get());
            keyValueBuilder.setStringValue(result.getResult());
            if (result.getErrorCode().isPresent()) {
              dataRecordBuilder.setErrorCode(result.getErrorCode().get());
            } else if (result.getDecryptionSuccessful()) {
              // TODO(b/398114484): remove when LS supports hex
              if (encodingType.isPresent() && encodingType.get() == HEX) {
                dataRecordBuilder.putEncryptedKeyValues(i, convertHexToBase64(value));
              } else {
                dataRecordBuilder.putEncryptedKeyValues(i, value);
              }
            }
          } else {
            keyValueBuilder.setStringValue(value);
          }
      }
    }
    return dataRecordBuilder.build();
  }

  /** Inner class for handling decryption. */
  private final class CSVDataDecrypter {

    final boolean rowLevelErrorsAllowed;
    final CryptoClient cryptoClient;
    final EncryptionMetadata encryptionMetadata;
    final DataRecordEncryptionColumns dataRecordEncryptionColumns;

    CSVDataDecrypter(
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
      return CsvDataReader.this.decryptColumn(
          cryptoClient, rowLevelErrorsAllowed, column, value, encryptionKeys);
    }

    /** Get encryption key values for a record. */
    DataRecordEncryptionKeys getEncryptionKeyValues(CSVRecord csvRecord) {
      try {
        var keysBuilder = DataRecordEncryptionKeys.newBuilder();
        KeyInfoCase keyType = encryptionMetadata.getEncryptionKeyInfo().getKeyInfoCase();
        EncryptionKeyColumnIndices encryptionIndices =
            dataRecordEncryptionColumns.getEncryptionKeyColumnIndices();
        if (keyType == WRAPPED_KEY_INFO) {
          WrappedKeyColumnIndices wrappedKeyIndices =
              encryptionIndices.getWrappedKeyColumnIndices();
          String dek = csvRecord.get(wrappedKeyIndices.getEncryptedDekColumnIndex());
          String kekUri = csvRecord.get(wrappedKeyIndices.getKekUriColumnIndex());
          if (kekUri.isBlank()) {
            throw new JobProcessorException(KEK_MISSING_IN_RECORD.name(), KEK_MISSING_IN_RECORD);
          }
          if (dek.isBlank()) {
            throw new JobProcessorException(DEK_MISSING_IN_RECORD.name(), DEK_MISSING_IN_RECORD);
          }
          if (wrappedKeyIndices.hasGcpColumnIndices()) {
            String wipValue =
                csvRecord.get(wrappedKeyIndices.getGcpColumnIndices().getWipProviderIndex());
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
          String coordKey = csvRecord.get(coordKeyIndices.getCoordinatorKeyColumnIndex());
          if (coordKey.isBlank()) {
            throw new JobProcessorException(
                COORDINATOR_KEY_MISSING_IN_RECORD.name(), COORDINATOR_KEY_MISSING_IN_RECORD);
          }
          keysBuilder.getCoordinatorKeyBuilder().setKeyId(coordKey);
        }
        return keysBuilder.build();
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
