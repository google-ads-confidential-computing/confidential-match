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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.dataprocessor.transformations.Transformation.TransformationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransformationProviderTest {

  @Test
  public void getTransformationFromName_success() throws Exception {
    String testName = "ToLowercaseTransformation";

    Transformation result = TransformationProvider.getTransformationFromId(testName);

    assertThat(result.getClass()).isEqualTo(ToLowercaseTransformation.class);
  }

  @Test
  public void getTransformationFromName_fail() {
    String testName = "invalid";

    var ex =
        assertThrows(
            TransformationException.class,
            () -> TransformationProvider.getTransformationFromId(testName));

    assertThat(ex.getMessage()).isEqualTo("Could not get Transformation from string");
  }
}
