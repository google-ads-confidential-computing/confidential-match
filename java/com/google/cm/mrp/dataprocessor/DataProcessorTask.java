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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_INPUT_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_FAILURE;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.OUTPUT_FILE_WRITE_ERROR;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient;
import com.google.cm.mrp.dataprocessor.formatters.DataErrorMatcher;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.DataMatchResult;
import com.google.cm.mrp.dataprocessor.models.LookupDataSourceResult;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparer;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.writers.DataWriter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A data processor task, that processes a single {@link DataReader}. Runs on a separate thread from
 * other tasks.
 */
public final class DataProcessorTask {

  private static final Logger logger = LoggerFactory.getLogger(DataProcessorTask.class);

  private DataProcessorTask() {}

  /** Processes a single data source. */
  public static ImmutableList<MatchStatistics> run(
      DataReader dataReader,
      LookupDataSource lookupDataSource,
      DataMatcher dataMatcher,
      DataWriter dataWriter,
      Optional<EncryptionMetadata> encryptionMetadata,
      Optional<DataSourcePreparer> dataSourcePreparer,
      Optional<DataOutputPreparer> dataOutputPreparer)
      throws JobProcessorException {
    String filename = dataReader.getName();

    try {

      var stats = ImmutableList.<MatchStatistics>builder();
      logger.info("File {}: Running DataProcessorTask", filename);
      while (dataReader.hasNext()) {
        checkForInterruption();
        logger.info("File {}: Processing next DataChunk", filename);
        // If dataSourcePreparer is present, data chunk output from dataReader will be prepared by
        // dataSourcePreparer.
        dataSourcePreparer.ifPresent(
            (unused) -> logger.info("File {}: Preparing DataChunk", filename));
        DataChunk dataChunk =
            dataSourcePreparer.isPresent()
                ? dataSourcePreparer.get().prepare(dataReader.next())
                : dataReader.next();

        checkForInterruption();
        logger.info("File {}: Performing Lookup for DataChunk", filename);
        LookupDataSourceResult lookupResult =
            lookupDataSource.lookup(dataChunk, encryptionMetadata);
        // Check if Lookup results contained errors. If so match them to dataChunkWithErrors. This
        // allows DataMatcher to correctly map partial successes.
        var dataChunkWithErrors =
            lookupResult
                .erroredLookupResults()
                .map(errorChunk -> DataErrorMatcher.matchErrors(dataChunk, errorChunk));

        checkForInterruption();
        DataMatchResult dataMatchResult =
            dataMatcher.match(dataChunkWithErrors.orElse(dataChunk), lookupResult.lookupResults());

        checkForInterruption();
        dataOutputPreparer.ifPresent(
            (unused) -> logger.info("File {}: Preparing output result of DataChunk", filename));
        DataMatchResult preparedDataMatchResult =
            dataOutputPreparer.isPresent()
                ? dataOutputPreparer.get().prepare(dataMatchResult)
                : dataMatchResult;

        checkForInterruption();
        logger.info("File {}: Writing DataChunk", filename);
        dataWriter.write(preparedDataMatchResult.dataChunk());
        stats.add(preparedDataMatchResult.matchStatistics());
      }
      return stats.build();

    } catch (IllegalArgumentException e) {

      logger.error("Invalid input file.", e);
      throw new JobProcessorException("Invalid input file.", e, INVALID_INPUT_FILE_ERROR);

    } catch (LookupServiceClient.LookupServiceClientException e) {

      logger.error(String.format("Error while performing lookup: %s", e.getMessage()));
      throw new JobProcessorException("Error while performing lookup", e, LOOKUP_SERVICE_FAILURE);

    } catch (IOException e) {

      logger.error(String.format("Error while writing to blob/file: %s", e.getMessage()));
      throw new JobProcessorException(
          "Error while writing to blob/file", e, OUTPUT_FILE_WRITE_ERROR);
    } finally {
      try {
        dataReader.close();
      } catch (IOException e) {
        logger.error("Error closing blob input stream: " + e.getMessage());
      }

      try {
        dataWriter.close();
      } catch (IOException e) {
        logger.error("Error closing blob output stream: " + e.getMessage());
      }
      logger.info("Finished processing {}", filename);
    }
  }

  private static void checkForInterruption() {
    if (Thread.currentThread().isInterrupted()) {
      String message = "A DataProcessorTask thread was interrupted.";
      logger.error(message);
      throw new CompletionException(
          new InterruptedException(message));
    }
  }
}
