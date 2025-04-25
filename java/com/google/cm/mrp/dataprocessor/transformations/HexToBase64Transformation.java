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

package com.google.cm.mrp.dataprocessor.transformations;

import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import java.util.List;

/** Transformation for Hex16-encoded PII to transform to Base64 for matching */
public class HexToBase64Transformation implements Transformation {

  /*
   * Transforms a KeyValue Hexadecimal encoded string to a base64-encoded string.
   */
  public KeyValue transform(KeyValue sourceKeyValue) throws TransformationException {
    return transform(sourceKeyValue, /* dependentKeyValues= */ ImmutableList.of());
  }

  /*
   * Transforms a KeyValue Hexadecimal encoded string to a base64-encoded string.
   */
  @Override
  public KeyValue transform(KeyValue sourceKeyValue, List<KeyValue> dependentKeyValues)
      throws TransformationException {
    if (!dependentKeyValues.isEmpty()) {
      throw new TransformationException("Only sourceKeyValue is supported");
    }
    if (!sourceKeyValue.hasStringValue()) {
      throw new TransformationException("Input does not contain string value");
    }
    if (sourceKeyValue.getStringValue().isBlank()) {
      return sourceKeyValue;
    }
    String value = sourceKeyValue.getStringValue().toUpperCase();
    if (!BaseEncoding.base16().canDecode(value)) {
      throw new TransformationException("Input is not in Hex");
    }
    byte[] bytes = BaseEncoding.base16().decode(value);
    String base64Val = BaseEncoding.base64().encode(bytes);
    return sourceKeyValue.toBuilder().setStringValue(base64Val).build();
  }
}
