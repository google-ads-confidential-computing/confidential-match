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

package com.google.cm.mrp.dataprocessor.writers;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.common.Annotations.MaxRecordsPerOutputFile;
import com.google.cm.mrp.dataprocessor.converters.SchemaConverter;
import com.google.cm.mrp.dataprocessor.destinations.DataDestination;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of DataWriter for writing CSV files. Allows writing one {@link DataChunk}
 * at a time to a CSV output file.
 */
public final class CsvDataWriter extends BaseDataWriter {

  private static final Logger logger = LoggerFactory.getLogger(CsvDataWriter.class);
  private static final Retry PRINTER_RETRY =
      Retry.of(
          "csv_data_writer_retry",
          RetryConfig.custom()
              .retryExceptions(IOException.class)
              .maxAttempts(3)
              .failAfterMaxAttempts(true)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      /* initialIntervalMillis */ 500,
                      /* multiplier */ 1.5,
                      /* maxIntervalMillis */ 1500))
              .build());

  private final DataDestination dataDestination;
  private final int maxRecordsPerOutputFile;
  private final CSVFormat csvFormat;
  private final String name;

  private int numberOfRecords;
  private int fileNumber;
  private CSVPrinter csvPrinter;

  /** Constructor for {@link DataWriter}. */
  @AssistedInject
  public CsvDataWriter(
      @MaxRecordsPerOutputFile Integer maxRecordsPerOutputFile,
      @Assisted DataDestination dataDestination,
      @Assisted String name,
      @Assisted Schema schema,
      @Assisted List<String> defaultOutputColumns) {
    this.maxRecordsPerOutputFile = maxRecordsPerOutputFile;
    this.dataDestination = dataDestination;
    this.name = name;
    numberOfRecords = 0;
    fileNumber = 0;

    // Only print headers if headers were assumed present in the input
    csvFormat =
        schema.getSkipHeaderRecord()
            ? CSVFormat.DEFAULT
                .builder()
                .setHeader(SchemaConverter.convertToColumnNames(schema))
                .build()
            : CSVFormat.DEFAULT;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (numberOfRecords > 0) {
      uploadThenDeleteFile();
    }
    if (file != null && file.exists()) {
      deleteFile();
      logger.error("Temporary file exists in CSVDataWriter after all uploads done.");
    }
  }

  /** {@inheritDoc} This implementation is synchronized. */
  @Override
  public synchronized void write(DataChunk dataChunk) throws IOException {
    if (fileNumber == 0) {
      newFile();
    }

    for (DataRecord dataRecord : dataChunk.records()) {
      if (numberOfRecords >= maxRecordsPerOutputFile) {
        uploadThenDeleteFile();
        newFile();
      }

      try {
        Retry.decorateCheckedRunnable(
                PRINTER_RETRY,
                () ->
                    csvPrinter.printRecord(
                        dataRecord.getKeyValuesList().stream().map(KeyValue::getStringValue)))
            .run();
      } catch (Throwable e) {
        String message = "CSV data writer threw an exception while writing to the file.";
        logger.error(message, e);
        deleteFile();
        throw new IOException(message, e);
      }
      numberOfRecords++;
    }
  }

  private void uploadThenDeleteFile() {
    try {
      // Only calls flush and close on the PrintWriter, which never actually throws
      csvPrinter.close(true);
    } catch (IOException e) {
      String message = "CSV data writer threw an exception while closing/flushing the output file.";
      logger.error(message, e);
      deleteFile();
      throw new JobProcessorException(message, e);
    }

    try {
      dataDestination.write(file, getFilename(name, fileNumber));
    } catch (Exception e) {
      String message = "CSV data writer threw an exception while uploading the file.";
      logger.error(message, e);
      deleteFile();
      throw new JobProcessorException(message, e);
    }
    deleteFile();
  }

  private void newFile() throws IOException {
    fileNumber++;
    numberOfRecords = 0;
    file = File.createTempFile("mrp", "");
    file.deleteOnExit();
    PrintWriter writer = new PrintWriter(file);
    try {
      csvPrinter = csvFormat.print(writer);
    } catch (IOException ex) {
      writer.close(); // does not throw
      throw ex;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Logger getLogger() {
    return logger;
  }
}
