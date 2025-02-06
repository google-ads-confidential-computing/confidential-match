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

import com.google.cm.mrp.dataprocessor.transformations.Transformation.TransformationException;
import com.google.common.collect.ImmutableMap;

/** Provides transformation instances. */
public final class TransformationProvider {

  private static final ImmutableMap<String, Transformation> TRANSFORMATIONS =
      ImmutableMap.of(
          "ToLowercaseTransformation",
          new ToLowercaseTransformation(),
          "CountryBasedZipTransformation",
          new CountryBasedZipTransformation());

  private TransformationProvider() {}

  /** Get the transformation class from a given string */
  public static Transformation getTransformationFromId(String id) throws TransformationException {
    if (!TRANSFORMATIONS.containsKey(id)) {
      throw new TransformationException("Could not get Transformation from string");
    }
    return TRANSFORMATIONS.get(id);
  }
}
