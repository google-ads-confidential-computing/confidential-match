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

package com.google.cm.mrp.dataprocessor;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_LIST_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INPUT_FILE_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_DATA_FORMAT;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.MISSING_SCHEMA_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SCHEMA_FILE_CLOSING_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SCHEMA_FILE_READ_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.SCHEMA_PERMISSIONS_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.UNSUPPORTED_MODE_ERROR;
import static com.google.cm.mrp.dataprocessor.common.Constants.ROW_MARKER_COLUMN_NAME;
import static com.google.cm.util.ProtoUtils.getProtoFromJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.storage.StorageException;
import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwner;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.ModeProto.Mode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.backend.SchemaProto.Schema.Column;
import com.google.cm.mrp.backend.SchemaProto.Schema.DataFormat;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.models.JobParameters;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient;
import com.google.scp.operator.cpio.blobstorageclient.BlobStorageClient.BlobStorageClientException;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation;
import com.google.scp.operator.cpio.blobstorageclient.model.DataLocation.BlobStoreDataLocation;
import com.google.scp.operator.cpio.metricclient.MetricClient;
import com.google.scp.operator.cpio.metricclient.MetricClient.MetricClientException;
import com.google.scp.operator.cpio.metricclient.model.CustomMetric;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete class for data sources that can be streamed and processed by the {@link DataProcessor}.
 * Streaming means loading and processing one {@link DataChunk} at a time.
 */
public final class BlobStoreStreamDataSource implements StreamDataSource {

  private static final Logger logger = LoggerFactory.getLogger(BlobStoreStreamDataSource.class);

  static final String FOLDER_DELIMITER = "/";
  static final String SCHEMA_DEFINITION_FILE = "schema.json";
  private final BlobStorageClient blobStorageClient;
  private final MetricClient metricClient;
  private final DataReaderFactory dataReaderFactory;
  private final DataOwner.DataLocation dataLocation;
  private final MatchConfig matchConfig;
  private final JobParameters jobParameters;
  private final Optional<String> dataOwnerIdentity;
  private final Optional<EncryptionMetadata> encryptionMetadata;
  private final Optional<CryptoClient> cryptoClient;
  private final Iterator<String> blobIterator;
  private final Schema schema;
  private final int size;

  /** Constructor for {@link BlobStoreStreamDataSource}. */
  @AssistedInject
  public BlobStoreStreamDataSource(
      BlobStorageClient blobStorageClient,
      MetricClient metricClient,
      DataReaderFactory dataReaderFactory,
      @Assisted MatchConfig matchConfig,
      @Assisted JobParameters jobParameters) {
    this.blobStorageClient = blobStorageClient;
    this.metricClient = metricClient;
    this.dataReaderFactory = dataReaderFactory;
    this.dataLocation = jobParameters.dataLocation();
    this.matchConfig = matchConfig;
    this.dataOwnerIdentity = jobParameters.dataOwnerIdentity();
    this.encryptionMetadata = Optional.empty();
    this.cryptoClient = Optional.empty();
    this.schema = getSchema();
    ImmutableList<String> blobList = getBlobList();
    this.size = blobList.size();
    this.blobIterator = blobList.iterator();
    this.jobParameters = jobParameters;
  }

  /** Constructor for {@link BlobStoreStreamDataSource}. */
  @AssistedInject
  public BlobStoreStreamDataSource(
      BlobStorageClient blobStorageClient,
      MetricClient metricClient,
      DataReaderFactory dataReaderFactory,
      @Assisted MatchConfig matchConfig,
      @Assisted JobParameters jobParameters,
      @Assisted CryptoClient cryptoClient) {
    this.blobStorageClient = blobStorageClient;
    this.metricClient = metricClient;
    this.dataReaderFactory = dataReaderFactory;
    this.dataLocation = jobParameters.dataLocation();
    this.matchConfig = matchConfig;
    this.dataOwnerIdentity = jobParameters.dataOwnerIdentity();
    this.encryptionMetadata = jobParameters.encryptionMetadata();
    this.cryptoClient = Optional.of(cryptoClient);
    this.schema = getSchema();
    ImmutableList<String> blobList = getBlobList();
    this.size = blobList.size();
    this.blobIterator = blobList.iterator();
    this.jobParameters = jobParameters;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getSchema() {
    try {
      Schema schema = getProtoFromJson(new String(getSchemaBytes(), UTF_8), Schema.class);
      // Default to CSV for backwards compatibility.
      if (schema.getDataFormat() == DataFormat.DATA_FORMAT_UNSPECIFIED) {
        schema = schema.toBuilder().setDataFormat(DataFormat.CSV).build();
      }
      if (!isValid(schema)) {
        String message = "Invalid schema file";
        logger.error(message);
        throw new JobProcessorException(message, INVALID_SCHEMA_FILE_ERROR);
      }
      return schema;
    } catch (IOException ex) {
      String message = "Unable to read the schema file";
      logger.error(message, ex);
      throw new JobProcessorException(message, ex, INVALID_SCHEMA_FILE_ERROR);
    }
  }

  private byte[] getSchemaBytes() throws IOException {
    String bucket = dataLocation.getInputDataBucketName();
    String path =
        dataLocation.getInputSchemaPath().isEmpty()
            ? dataLocation.getInputDataBlobPrefix() + FOLDER_DELIMITER + SCHEMA_DEFINITION_FILE
            : dataLocation.getInputSchemaPath();
    DataLocation location = getLocation(bucket, path);
    try (InputStream schemaStream = blobStorageClient.getBlob(location, dataOwnerIdentity)) {

      try {
        return schemaStream.readAllBytes();
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }

    } catch (IOException ex) {
      String message = "Unable to close the schema file";
      logger.error(message, ex);
      throw new JobProcessorException(message, ex, SCHEMA_FILE_CLOSING_ERROR);
    } catch (UncheckedIOException ex) {
      String message = "UncheckedIOException when closing the schema file";
      logger.error(message, ex);
      throw ex.getCause() != null ? ex.getCause() : new IOException(ex);
    } catch (StorageException ex) {
      if (ex.getCause() != null && ex.getCause() instanceof GoogleJsonResponseException) {
        int errorCode = ((GoogleJsonResponseException) ex.getCause()).getDetails().getCode();
        if (errorCode == 403) {
          String message = "Missing permission to open schema file.";
          logger.warn(message, ex);
          throw new JobProcessorException(message, ex, SCHEMA_PERMISSIONS_ERROR);
        } else if (errorCode == 404) {
          String message = "Unable to open the schema file";
          logger.error(message, ex);
          throw new JobProcessorException(message, ex, MISSING_SCHEMA_ERROR);
        }
      }
      String message = "Unable to read the schema file. Will retry.";
      logger.info(message, ex);
      throw new JobProcessorException(message, ex, SCHEMA_FILE_READ_ERROR);

    } catch (BlobStorageClientException | RuntimeException ex) {
      if (ex instanceof JobProcessorException) {
        throw (JobProcessorException) ex;
      }
      String message = "Unable to open the schema file";
      logger.error(message, ex);
      throw new JobProcessorException(message, ex, MISSING_SCHEMA_ERROR);
    }
  }

  // Returns a list of blob names from the data source.
  private ImmutableList<String> getBlobList() {
    if (dataLocation.getInputDataBlobPathsCount() > 0) {
      return dataLocation.getInputDataBlobPathsList().stream()
          .filter(path -> !path.equals(dataLocation.getInputSchemaPath()))
          .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }
    try {
      // TODO(b/452813040): refactor to use MRP blob store client
      String inputBucket = dataLocation.getInputDataBucketName();
      String inputPrefix = dataLocation.getInputDataBlobPrefix();
      DataLocation blobsLocation = getLocation(inputBucket, inputPrefix + FOLDER_DELIMITER);
      ImmutableList<String> list =
          blobStorageClient.listBlobs(blobsLocation, dataOwnerIdentity).stream()
              .filter(blob -> !blob.toLowerCase().endsWith(SCHEMA_DEFINITION_FILE))
              .filter(blob -> !blob.endsWith(FOLDER_DELIMITER))
              .filter(blob -> !blob.equals(dataLocation.getInputSchemaPath()))
              .collect(ImmutableList.toImmutableList());
      logger.info("BlobList: {}", list.toString());
      return list;
    } catch (BlobStorageClientException e) {
      String message = "Unable to list input files";
      logger.error(message, e);
      throw new JobProcessorException(message, e, INPUT_FILE_LIST_READ_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean hasNext() {
    return blobIterator.hasNext();
  }

  /** {@inheritDoc} */
  @Override
  public int size() {
    return size;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized DataReader next() {

    if (!blobIterator.hasNext()) {
      String message = "There are no more readers to list.";
      logger.error(message);
      throw new JobProcessorException(message, INPUT_FILE_LIST_READ_ERROR);
    }

    String blob = blobIterator.next();
    logger.info("Next blob to read: {}", blob);
    try {
      var location = getLocation(dataLocation.getInputDataBucketName(), blob);
      Stopwatch stopwatch = Stopwatch.createStarted();
      var gcsBlob = blobStorageClient.getBlob(location, dataOwnerIdentity);
      CustomMetric metric =
          CustomMetric.builder()
              .setNameSpace("cfm/mrp")
              .setName("gcsreadduration")
              .setValue(stopwatch.elapsed(MILLISECONDS))
              .setUnit("Milliseconds")
              .addLabel("blob", blob)
              .build();
      try {
        logger.trace("Writing metric: {}", metric);
        metricClient.recordMetric(metric);
      } catch (MetricClientException e) {
        logger.warn("Unable to record gcsreadduration metric", e);
      }

      return constructDataReader(gcsBlob, blob);
    } catch (BlobStorageClientException e) {
      String message = String.format("Unable to open the input file: %s", blob);
      logger.error(message, e);
      throw new JobProcessorException(message + e.getMessage(), INPUT_FILE_READ_ERROR);
    }
  }

  private DataLocation getLocation(String bucket, String key) {
    return DataLocation.ofBlobStoreDataLocation(BlobStoreDataLocation.create(bucket, key));
  }

  private boolean isValid(Schema schema) {
    switch (schema.getDataFormat()) {
      case SERIALIZED_PROTO:
        return isValidSerializedProtoSchema(schema);
      case CSV:
        if (!hasNestedColumns(schema)) {
          return isValidCsvSchemaWithoutNestedColumns(schema);
        } else {
          // Schema expansion is employed to validate schemas that contain nested schemas, ensuring
          // the
          // overall structure adheres to defined rules and constraints.
          return isValidCsvSchemaWithoutNestedColumns(expandSchema(schema))
              && isValidCsvSchemaWithNestedColumns(schema);
        }
      default:
        throw new JobProcessorException(
            "Unsupported data format: " + schema.getDataFormat(), INVALID_DATA_FORMAT);
    }
  }

  // Expands the existing schema by breaking down any column containing a nested schema into its
  // individual columns. If a column doesn't contain a nested schema, it remains unchanged in the
  // new schema.
  private Schema expandSchema(Schema schema) {
    Schema.Builder expandedSchemaBuilder = Schema.newBuilder();
    for (Column column : schema.getColumnsList()) {
      if (column.hasNestedSchema()) {
        expandedSchemaBuilder.addAllColumns(column.getNestedSchema().getColumnsList());
      } else {
        expandedSchemaBuilder.addColumns(column);
      }
    }
    return expandedSchemaBuilder.build();
  }

  private boolean isValidCsvSchemaWithoutNestedColumns(Schema schema) {
    return columnNamesAreUnique(schema)
        && hasAtLeastOneMatchColumn(schema)
        && hasCompleteCompositeColumns(schema)
        && hasColumnGroupsForMultipleCompositeColumns(schema)
        && unencryptedColumnsDoNotHaveColumnEncoding(schema)
        && encryptedColumnsAreMatchColumn(schema);
  }

  private boolean isValidCsvSchemaWithNestedColumns(Schema schema) {
    return columnNamesAreUnique(schema) && unencryptedColumnsDoNotHaveColumnEncoding(schema);
  }

  private boolean isValidSerializedProtoSchema(Schema schema) {
    // Currently not supporting nested columns and output schema for serialized proto data format.
    return !hasNestedColumns(schema)
        && !hasOutputSchema(schema)
        && columnNamesAreUnique(schema)
        && hasAtLeastOneMatchColumn(schema)
        && hasCompleteCompositeColumns(schema)
        && schemaDoesNotContainRestrictedColumnNames(schema)
        && hasColumnGroupsForMultipleCompositeColumns(schema)
        && unencryptedColumnsDoNotHaveColumnEncoding(schema)
        && encryptedColumnsAreMatchColumn(schema);
  }

  private boolean columnNamesAreUnique(Schema schema) {
    // HashSet::add returns false, if the element is already in the HashSet
    return schema.getColumnsList().stream()
        .map(Schema.Column::getColumnName)
        .map(String::toLowerCase)
        .allMatch(new HashSet<String>()::add);
  }

  private boolean hasAtLeastOneMatchColumn(Schema schema) {
    return
    // there is at least one column with an alias (this is needed for match config)
    schema.getColumnsList().stream().anyMatch(Schema.Column::hasColumnAlias)
        // and there is at least one column with an alias
        //     that has a corresponding match column in match config
        && schema.getColumnsList().stream()
            .filter(Schema.Column::hasColumnAlias)
            .map(Schema.Column::getColumnAlias)
            .anyMatch(
                alias ->
                    matchConfig.getMatchConditionsList().stream()
                        .flatMap(
                            matchCondition ->
                                matchCondition.getDataSource1Column().getColumnsList().stream()
                                    .map(MatchConfig.Column::getColumnAlias))
                        .anyMatch(matchColumn -> matchColumn.equalsIgnoreCase(alias)));
  }

  private boolean hasCompleteCompositeColumns(Schema schema) {
    return matchConfig.getMatchConditionsList().stream()
        .map(matchCondition -> matchCondition.getDataSource1Column().getColumnsList())

        // filter all composite multi columns
        .filter(columnsInCompositeColumn -> columnsInCompositeColumn.size() > 1)

        // find composite columns with at least one column in the schema
        .filter(
            columnsInCompositeColumn ->
                columnsInCompositeColumn.stream()
                    .anyMatch(
                        column ->
                            schema.getColumnsList().stream()
                                .anyMatch(
                                    schemaColumn ->
                                        schemaColumn
                                            .getColumnAlias()
                                            .equalsIgnoreCase(column.getColumnAlias()))))

        // check that composite columns have all columns in the schema
        .allMatch(
            compositeColumns ->
                schema.getColumnsList().stream()
                    .filter(
                        schemaColumn ->
                            compositeColumns.stream()
                                .anyMatch(
                                    column ->
                                        schemaColumn
                                            .getColumnAlias()
                                            .equalsIgnoreCase(column.getColumnAlias())))
                    .collect(Collectors.groupingBy(Schema.Column::getColumnGroup))
                    .values()
                    .stream()
                    .allMatch(schemaColumns -> schemaColumns.size() == compositeColumns.size()));
  }

  private boolean hasColumnGroupsForMultipleCompositeColumns(Schema schema) {
    long compositeMultiColumnCount =
        matchConfig.getMatchConditionsList().stream()
            .map(matchCondition -> matchCondition.getDataSource1Column().getColumnsList())

            // filter all composite multi columns
            .filter(columnsInCompositeColumn -> columnsInCompositeColumn.size() > 1)

            // find compositeColumns with at least one column in the schema
            .filter(
                columnsInCompositeColumn ->
                    columnsInCompositeColumn.stream()
                        .anyMatch(
                            column ->
                                schema.getColumnsList().stream()
                                    .anyMatch(
                                        schemaColumn ->
                                            schemaColumn
                                                .getColumnAlias()
                                                .equalsIgnoreCase(column.getColumnAlias()))))
            // get count
            .count();

    return (compositeMultiColumnCount <= 1)

        // get compositeColumns
        || matchConfig.getMatchConditionsList().stream()
            .map(matchCondition -> matchCondition.getDataSource1Column().getColumnsList())

            // filter all composite multi columns
            .filter(columnsInCompositeColumn -> columnsInCompositeColumn.size() > 1)

            // find compositeColumns with at least one column in the schema
            .filter(
                columnsInCompositeColumn ->
                    columnsInCompositeColumn.stream()
                        .anyMatch(
                            column ->
                                schema.getColumnsList().stream()
                                    .anyMatch(
                                        schemaColumn ->
                                            schemaColumn
                                                .getColumnAlias()
                                                .equalsIgnoreCase(column.getColumnAlias()))))

            // check that all schema columns have column_group field
            .allMatch(
                columnsInCompositeColumn ->
                    columnsInCompositeColumn.stream()
                        .allMatch(
                            column ->
                                schema.getColumnsList().stream()
                                    .filter(
                                        schemaColumn ->
                                            schemaColumn
                                                .getColumnAlias()
                                                .equalsIgnoreCase(column.getColumnAlias()))
                                    .allMatch(Schema.Column::hasColumnGroup)));
  }

  // Validate that schema does not have column_encoding specified for unencrypted columns
  private boolean unencryptedColumnsDoNotHaveColumnEncoding(Schema schema) {
    for (Column column : schema.getColumnsList()) {
      if (column.hasColumnEncoding() && (!column.hasEncrypted() || !column.getEncrypted())) {
        return false;
      }
    }
    return true;
  }

  private boolean encryptedColumnsAreMatchColumn(Schema schema) {
    return schema.getColumnsList().stream()
        .filter(Schema.Column::getEncrypted)
        .map(Schema.Column::getColumnAlias)
        .allMatch(
            alias ->
                matchConfig.getMatchConditionsList().stream()
                    .flatMap(
                        matchCondition ->
                            matchCondition.getDataSource1Column().getColumnsList().stream()
                                .map(MatchConfig.Column::getColumnAlias))
                    .anyMatch(matchColumn -> matchColumn.equalsIgnoreCase(alias)));
  }

  private boolean hasNestedColumns(Schema schema) {
    return schema.getColumnsList().stream().anyMatch(column -> column.hasNestedSchema());
  }

  private boolean hasOutputSchema(Schema schema) {
    return !schema.getOutputColumnsList().isEmpty();
  }

  private boolean schemaDoesNotContainRestrictedColumnNames(Schema schema) {
    List<String> restrictedColumnNames =
        new ArrayList<>(Arrays.asList(ROW_MARKER_COLUMN_NAME.toLowerCase()));
    if (matchConfig.getSuccessConfig().getSuccessMode() == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
      restrictedColumnNames.add(
          matchConfig
              .getSuccessConfig()
              .getPartialSuccessAttributes()
              .getRecordStatusFieldName()
              .toLowerCase());
    }
    return schema.getColumnsList().stream()
        .noneMatch(column -> restrictedColumnNames.contains(column.getColumnName().toLowerCase()));
  }

  private DataReader constructDataReader(InputStream gcsBlob, String blob) {
    if (cryptoClient.isPresent() && encryptionMetadata.isEmpty()) {
      String msg = "EncryptionMetadata is missing with encrypted data source";
      logger.error(msg);
      throw new JobProcessorException(msg, CRYPTO_CLIENT_CONFIGURATION_ERROR);
    }
    SuccessMode successMode = matchConfig.getSuccessConfig().getSuccessMode();

    switch (schema.getDataFormat()) {
      case CSV:
        if (Mode.JOIN == jobParameters.mode()) {
          String msg = "Join mode not supported for CSV data.";
          logger.warn(msg);
          throw new JobProcessorException(msg, UNSUPPORTED_MODE_ERROR);
        }
        if (cryptoClient.isPresent()) {
          return dataReaderFactory.createCsvDataReader(
              gcsBlob,
              schema,
              blob,
              jobParameters,
              matchConfig.getEncryptionKeyColumns(),
              successMode,
              cryptoClient.get());
        }
        return dataReaderFactory.createCsvDataReader(gcsBlob, schema, blob, successMode);
      case SERIALIZED_PROTO:
        if (cryptoClient.isPresent()) {
          return dataReaderFactory.createProtoDataReader(
              gcsBlob,
              schema,
              blob,
              matchConfig,
              successMode,
              encryptionMetadata.get(),
              cryptoClient.get());
        }
        return dataReaderFactory.createProtoDataReader(
            gcsBlob, schema, blob, matchConfig, successMode);
      default:
        throw new JobProcessorException(
            "Invalid data format: " + schema.getDataFormat(), INVALID_DATA_FORMAT);
    }
  }
}
