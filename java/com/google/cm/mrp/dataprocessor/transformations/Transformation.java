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
import java.util.List;

/** Interface representing a transformation to a data source. */
public interface Transformation {

  /** All transformations that can be invoked conditionally. */
  enum ConditionalTransformations {
    HEX_TO_BASE64("HexToBase64Transformation");

    // Must match Java class name
    public final String transformationId;

    ConditionalTransformations(String transformationId) {
      this.transformationId = transformationId;
    }
  }

  /**
   * Returns a KeyValue with the value containing the transformed value.
   *
   * @param sourceKeyValue The source KeyValue that is being transformed.
   * @param dependentKeyValues A list of KeyValues that affect the transformation of the source
   *     KeyValue
   * @return The transformed KeyValue.
   */
  KeyValue transform(KeyValue sourceKeyValue, List<KeyValue> dependentKeyValues)
      throws TransformationException;

  /** Wrapper for exceptions thrown by a {@link Transformation} implementation. */
  class TransformationException extends Exception {

    /** Constructs a new instance. */
    public TransformationException(String message) {
      super(message);
    }
  }
}
