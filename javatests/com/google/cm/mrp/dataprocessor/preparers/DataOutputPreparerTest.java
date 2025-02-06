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

package com.google.cm.mrp.dataprocessor.preparers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.dataprocessor.formatters.DataOutputFormatter;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.DataMatchResult;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.util.ProtoUtils;
import com.google.common.io.Resources;
import java.util.Objects;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class DataOutputPreparerTest {

  private Schema schema;

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataOutputFormatter mockDataOutputFormatter;

  @Before
  public void setup() throws Exception {
    schema = getSchema("testdata/mic_ecw_parsed_schema.json");
  }

  @Test
  public void dataOutputPreparer_returnOriginalDataMatchResults() {
    DataMatchResult originalDataMatchResult =
        DataMatchResult.create(
            DataChunk.builder().setSchema(schema).build(), MatchStatistics.emptyInstance());
    Optional<DataOutputFormatter> dataOutputFormatter = Optional.empty();
    DataOutputPreparer dataOutputPreparer = new DataOutputPreparerImpl(dataOutputFormatter);

    DataMatchResult returnedDataMatchResult = dataOutputPreparer.prepare(originalDataMatchResult);

    assertSame(originalDataMatchResult, returnedDataMatchResult);
  }

  @Test
  public void dataOutputPreparer_formatDataMatchResults() {
    DataMatchResult originalDataMatchResult =
        DataMatchResult.create(
            DataChunk.builder().setSchema(schema).build(), MatchStatistics.emptyInstance());
    DataChunk expectedDataChunk = DataChunk.builder().setSchema(schema).build();
    when(mockDataOutputFormatter.format(any())).thenReturn(expectedDataChunk);
    Optional<DataOutputFormatter> dataOutputFormatter = Optional.of(mockDataOutputFormatter);
    DataOutputPreparer dataOutputPreparer = new DataOutputPreparerImpl(dataOutputFormatter);

    DataMatchResult returnedDataMatchResult = dataOutputPreparer.prepare(originalDataMatchResult);

    assertNotSame(originalDataMatchResult, returnedDataMatchResult);
    assertSame(expectedDataChunk, returnedDataMatchResult.dataChunk());
  }

  private Schema getSchema(String path) throws Exception {
    return ProtoUtils.getProtoFromJson(
        Resources.toString(Objects.requireNonNull(getClass().getResource(path)), UTF_8),
        Schema.class);
  }
}
