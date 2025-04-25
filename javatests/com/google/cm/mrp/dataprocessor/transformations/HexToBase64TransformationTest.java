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

import com.google.cm.mrp.backend.DataRecordProto.DataRecord.KeyValue;
import com.google.cm.mrp.dataprocessor.transformations.Transformation.TransformationException;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HexToBase64TransformationTest {

  public final HexToBase64Transformation transformation = new HexToBase64Transformation();

  @Test
  public void transform_success() throws Exception {
    String testPlaintext = "TEST";
    String hex =
        BaseEncoding.base16().encode(testPlaintext.getBytes(StandardCharsets.UTF_8)).toLowerCase();
    String base64 = BaseEncoding.base64().encode(testPlaintext.getBytes(StandardCharsets.UTF_8));
    var input = KeyValue.newBuilder().setKey("testKey").setStringValue(hex).build();

    var result = transformation.transform(input);

    assertThat(result.getStringValue()).isEqualTo(base64);
  }

  @Test
  public void transform_blankValue_returns() throws Exception {
    String testPlaintext = "   ";
    var input = KeyValue.newBuilder().setKey("testKey").setStringValue(testPlaintext).build();

    var result = transformation.transform(input);

    assertThat(result.getStringValue()).isEqualTo(testPlaintext);
  }

  @Test
  public void transform_dependentKeyValues_fails() {
    var test = KeyValue.newBuilder().setKey("testKey").setStringValue("test").build();
    var dependents = ImmutableList.of(test, test);

    var ex =
        assertThrows(
            TransformationException.class, () -> transformation.transform(test, dependents));

    assertThat(ex.getMessage()).isEqualTo("Only sourceKeyValue is supported");
  }

  @Test
  public void transform_noStringValue_fails() {
    var input = KeyValue.newBuilder().setKey("testKey").setIntValue(1).build();

    var ex = assertThrows(TransformationException.class, () -> transformation.transform(input));

    assertThat(ex.getMessage()).isEqualTo("Input does not contain string value");
  }

  @Test
  public void transform_badEncoding_fails() {
    var input = KeyValue.newBuilder().setKey("testKey").setStringValue("+&^").build();

    var ex = assertThrows(TransformationException.class, () -> transformation.transform(input));

    assertThat(ex.getMessage()).isEqualTo("Input is not in Hex");
  }
}
