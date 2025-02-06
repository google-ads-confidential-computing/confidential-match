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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.lookupserver.api.LookupProto.DataRecord;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataRecordConverterTest {

  @Test
  public void convertToDataRecord_success() {
    String lookupKey = "unittestkey";
    String dataKey = "dataKey";
    String metadataKey = "metadataKey";
    String metadataValue = "metadataValue";
    LookupDataRecord lookupDataRecord =
        LookupDataRecord.newBuilder()
            .setLookupKey(LookupDataRecord.LookupKey.newBuilder().setKey(lookupKey))
            .addMetadata(
                LookupDataRecord.KeyValue.newBuilder()
                    .setKey(metadataKey)
                    .setStringValue(metadataValue))
            .build();

    DataRecord result = DataRecordConverter.convertToDataRecord(lookupDataRecord);

    assertThat(result.getLookupKey().getKey()).isEqualTo(lookupKey);
    assertThat(result.getMetadataCount()).isEqualTo(1);
    assertThat(result.getMetadata(0).getKey()).isEqualTo(metadataKey);
    assertThat(result.getMetadata(0).getStringValue()).isEqualTo(metadataValue);
  }

  @Test
  public void convertToKeyFormat_unsupportedTypeFailure() {
    LookupDataRecord lookupDataRecord =
        LookupDataRecord.newBuilder()
            .setLookupKey(LookupDataRecord.LookupKey.newBuilder().setKey("test"))
            .addMetadata(LookupDataRecord.KeyValue.getDefaultInstance())
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> DataRecordConverter.convertToDataRecord(lookupDataRecord));
    assertThat(ex.getMessage()).isEqualTo("Invalid LookupDataRecord; invalid input data");
  }
}
