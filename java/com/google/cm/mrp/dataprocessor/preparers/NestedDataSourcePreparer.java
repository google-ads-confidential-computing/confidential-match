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

package com.google.cm.mrp.dataprocessor.preparers;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_NESTED_SCHEMA_FILE_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.NESTED_COLUMN_PARSING_ERROR;
import static com.google.cm.mrp.backend.SchemaProto.Schema.ColumnFormat.GTAG;
import static com.google.cm.mrp.dataprocessor.formatters.EnhancedMatchMapper.mapGtagToDataRecord;
import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema.ColumnFormat;
import com.google.cm.mrp.dataprocessor.formatters.DataSourceFormatter;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.common.collect.ImmutableList;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete class implementing {@link DataSourcePreparer} interface. */
public final class NestedDataSourcePreparer implements DataSourcePreparer {
  private static final Logger logger = LoggerFactory.getLogger(NestedDataSourcePreparer.class);

  private final DataSourceFormatter dataSourceFormatter;
  private final SuccessMode successMode;

  /** Constructor for {@link NestedDataSourcePreparer}. */
  @AssistedInject
  NestedDataSourcePreparer(
      @Assisted DataSourceFormatter dataSourceFormatter, @Assisted SuccessMode successMode) {
    this.dataSourceFormatter = dataSourceFormatter;
    this.successMode = successMode;
  }

  /*
   * This method does the following:
   * <li> calls a method to parse any nested columns that the input DataChunk might contain
   * <li> calls a method to split DataRecords, if needed, into multiple output DataRecords
   * <li> sets the schema of the DataChunk to match the schema of output DataRecords
   */
  public DataChunk prepare(DataChunk inputChunk) {
    ImmutableList<Integer> nestedColumnsIndices =
        IntStream.range(0, inputChunk.schema().getColumnsCount())
            .filter(i -> inputChunk.schema().getColumns(i).hasNestedSchema())
            .boxed()
            .collect(toImmutableList());

    if (nestedColumnsIndices.isEmpty()) {
      return inputChunk;
    }

    DataChunk.Builder outputChunkBuilder = DataChunk.builder();
    for (DataRecord inputRecord : inputChunk.records()) {
      DataRecord.Builder expandedDataRecord = inputRecord.toBuilder();
      for (int index : nestedColumnsIndices) {
        // Skip nested column parsing if DataRecord error code exists.
        if (expandedDataRecord.hasErrorCode()) {
          break;
        }
        // Note: This assumes that the order of KeyValues in the inputRecord matches with the order
        // of corresponding columns in the inputChunk.schema().
        KeyValue keyValue = inputRecord.getKeyValues(index);
        ColumnFormat columnFormat = inputChunk.schema().getColumns(index).getColumnFormat();
        switch (columnFormat) {
          case GTAG:
            try {
              DataRecord nestedDataRecord = mapGtagToDataRecord(keyValue.getStringValue());
              expandedDataRecord.addAllKeyValues(nestedDataRecord.getKeyValuesList());
            } catch (Exception e) {
              String message =
                  String.format(
                      "Failed to parse nested schema column with string length of %d",
                      keyValue.getStringValue().length());
              logger.error(message, e);
              if (successMode == SuccessMode.ALLOW_PARTIAL_SUCCESS) {
                expandedDataRecord.setErrorCode(NESTED_COLUMN_PARSING_ERROR);
              } else {
                throw new JobProcessorException(message, e, NESTED_COLUMN_PARSING_ERROR);
              }
            }
            break;
          default:
            String message = "Undefined column format for the nested schema column";
            logger.error(message);
            throw new JobProcessorException(message, INVALID_NESTED_SCHEMA_FILE_ERROR);
        }
      }
      ImmutableList<DataRecord> fannedOutDataRecords =
          dataSourceFormatter.format(expandedDataRecord.build());
      for (DataRecord fannedOut : fannedOutDataRecords) {
        outputChunkBuilder.addRecord(fannedOut);
      }
    }

    outputChunkBuilder.setSchema(dataSourceFormatter.getFormattedSchema());
    outputChunkBuilder.setInputSchema(inputChunk.schema());
    // Sets outputChunk encryptionColumns if it's present in dataSourceFormatter.
    dataSourceFormatter
        .getDataRecordEncryptionColumns()
        .ifPresent(outputChunkBuilder::setEncryptionColumns);
    return outputChunkBuilder.build();
  }
}
