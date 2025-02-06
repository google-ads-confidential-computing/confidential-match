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

package com.google.cm.mrp.clients.lookupserviceclient.converters;

import com.google.cm.lookupserver.api.LookupProto.DataRecord;
import com.google.cm.lookupserver.api.LookupProto.KeyValue;
import com.google.cm.lookupserver.api.LookupProto.LookupKey;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import java.util.List;

/** Converts DataRecord-related items */
public final class DataRecordConverter {

  /** Converts a {@link LookupDataRecord} proto to a {@link DataRecord} proto */
  public static DataRecord convertToDataRecord(LookupDataRecord lookupDataRecord) {
    var builder = DataRecord.newBuilder();
    builder
        .setLookupKey(LookupKey.newBuilder().setKey(lookupDataRecord.getLookupKey().getKey()));
    convertMetadata(lookupDataRecord.getMetadataList(), builder);
    return builder.build();
  }

  private static void convertMetadata(
      List<LookupDataRecord.KeyValue> keyValues, DataRecord.Builder builder) {
    for (var keyValue : keyValues) {
      var keyValBuilder = KeyValue.newBuilder().setKey(keyValue.getKey());
      switch (keyValue.getValueCase()) {
        case STRING_VALUE:
          keyValBuilder.setStringValue(keyValue.getStringValue());
          break;
        case INT_VALUE:
          keyValBuilder.setIntValue(keyValue.getIntValue());
          break;
        case DOUBLE_VALUE:
          keyValBuilder.setDoubleValue(keyValue.getDoubleValue());
          break;
        case BOOL_VALUE:
          keyValBuilder.setBoolValue(keyValue.getBoolValue());
          break;
        case VALUE_NOT_SET:
          throw new IllegalArgumentException("Invalid LookupDataRecord; invalid input data");
      }
      builder.addMetadata(keyValBuilder.build());
    }
  }
}
