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
import java.util.List;
import java.util.Locale;

/** Transformation to convert the string value of any KeyValue to lowercase */
public class ToLowercaseTransformation implements Transformation {

  /*
   * Transforms a KeyValue string to lowercase. Fails if no stringValue exists
   */
  public KeyValue transform(KeyValue sourceKeyValue) throws TransformationException {
    return transform(sourceKeyValue, /* dependentKeyValues= */ ImmutableList.of());
  }

  /*
   * Transforms a KeyValue string to lowercase. Fails if dependentKeyValues are given or if no
   * stringValue exists
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
    return sourceKeyValue.toBuilder()
        // TODO(b/333460561): Pull out trim to seperate transformation
        .setStringValue(sourceKeyValue.getStringValue().toLowerCase(Locale.US).trim())
        .build();
  }
}
