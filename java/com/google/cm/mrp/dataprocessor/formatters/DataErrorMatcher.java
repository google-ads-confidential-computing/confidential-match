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

package com.google.cm.mrp.dataprocessor.formatters;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Matcher for errors in separate Data Chunks. */
public final class DataErrorMatcher {

  /**
   * Matches data records with errors in {@link DataChunk} to data records in the original {@link
   * DataChunk}. All records in the original DataChunk are queried and any that match a record the
   * error DataChunk will be modified to also include this error. Therefore, the original dataChunk
   * and resulting DataChunk have the same size.
   *
   * @return a new dataChunk to use in place of original
   */
  public static DataChunk matchErrors(DataChunk originalDataChunk, DataChunk errors) {
    if (errors.records().isEmpty()) {
      return originalDataChunk;
    }
    ImmutableList.Builder<DataRecord> newDataRecords =
        ImmutableList.builderWithExpectedSize(originalDataChunk.records().size());
    Map<String, JobResultCode> codeMap =
        errors.records().stream()
            .collect(
                /* key= */ Collectors.toMap(
                    dataRecord -> dataRecord.getKeyValues(0).getStringValue(),
                    /* value= */ DataRecord::getErrorCode,
                    (first, second) -> first));
    var dataChunkBuilder =
        DataChunk.builder()
            .setSchema(originalDataChunk.schema())
            .setEncryptionColumns(originalDataChunk.encryptionColumns())
            .setInputSchema(originalDataChunk.inputSchema());

    originalDataChunk
        .records()
        .forEach(
            dataRecord -> {
              Optional<JobResultCode> jobResultCode =
                  dataRecord.getKeyValuesList().stream()
                      .filter(kv -> codeMap.containsKey(kv.getStringValue()))
                      .map(kv -> codeMap.get(kv.getStringValue()))
                      .findAny();
              if (jobResultCode.isEmpty()) {
                newDataRecords.add(dataRecord);
              } else {
                newDataRecords.add(
                    dataRecord.toBuilder().setErrorCode(jobResultCode.get()).build());
              }
            });
    return dataChunkBuilder.setRecords(newDataRecords.build()).build();
  }
}
