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

package com.google.cm.mrp.dataprocessor.converters;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.util.ProtoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Objects;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaConverterTest {

  @Test
  public void convertToColumnNames_Success() throws IOException {
    Schema testSchema =
        ProtoUtils.getProtoFromJson(
            Resources.toString(
                Objects.requireNonNull(getClass().getResource("schema.json")), UTF_8),
            Schema.class);

    String[] columnNames = SchemaConverter.convertToColumnNames(testSchema);

    assertThat(ImmutableList.copyOf(columnNames))
        .containsExactly("email", "phone", "first_name", "last_name", "zip_code", "country_code");
  }
}
