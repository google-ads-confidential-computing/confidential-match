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

package com.google.cm.mrp.dataprocessor;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.clients.lookupserviceclient.LookupServiceClient.LookupServiceClientException;
import com.google.cm.mrp.dataprocessor.models.DataChunk;
import com.google.cm.mrp.dataprocessor.models.DataMatchResult;
import com.google.cm.mrp.dataprocessor.models.LookupDataSourceResult;
import com.google.cm.mrp.dataprocessor.models.MatchStatistics;
import com.google.cm.mrp.dataprocessor.preparers.DataOutputPreparer;
import com.google.cm.mrp.dataprocessor.preparers.DataSourcePreparer;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.writers.DataWriter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DataProcessorTaskTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataReader mockDataReader;
  @Mock private DataSourcePreparer mockDataSourcePreparer;
  @Mock private DataOutputPreparer mockDataOutputPreparer;
  @Mock private LookupDataSource mockLookupDataSource;
  @Mock private DataWriter mockDataWriter;
  @Mock private DataMatcher mockDataMatcher;
  @Mock private DataChunk mockDataChunk;
  @Mock private DataChunk mockErrorChunk;
  @Mock private LookupDataSourceResult mockLookupDataSourceResult;

  @Test
  public void run_readsAndProcessesAllDataChunks()
      throws IOException, LookupServiceClientException {
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockLookupDataSourceResult.erroredLookupResults()).thenReturn(Optional.empty());
    var result = DataMatchResult.create(mockDataChunk, MatchStatistics.emptyInstance());
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);

    DataProcessorTask.run(
        mockDataReader,
        mockLookupDataSource,
        mockDataMatcher,
        mockDataWriter,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    verify(mockDataReader, times(3)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader).close();
    verify(mockDataReader).getName();
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriter).close();
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verifyNoMoreInteractions(
        mockDataReader,
        mockDataSourcePreparer,
        mockDataWriter,
        mockDataMatcher,
        mockDataChunk,
        mockErrorChunk,
        mockLookupDataSource);
  }

  @Test
  public void run_whenLookupServiceFailures_usesErrorMatcherDataChunk()
      throws IOException, LookupServiceClientException {
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockDataSourcePreparer.prepare(any())).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockLookupDataSourceResult.erroredLookupResults()).thenReturn(Optional.of(mockErrorChunk));
    when(mockErrorChunk.records()).thenReturn(ImmutableList.of());
    var result = DataMatchResult.create(mockDataChunk, MatchStatistics.emptyInstance());
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataOutputPreparer.prepare(any())).thenReturn(result);

    DataProcessorTask.run(
        mockDataReader,
        mockLookupDataSource,
        mockDataMatcher,
        mockDataWriter,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    verify(mockDataReader, times(3)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader).close();
    verify(mockDataReader).getName();
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriter).close();
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verify(mockErrorChunk, times(2)).records();
    verifyNoMoreInteractions(
        mockDataReader,
        mockDataWriter,
        mockDataMatcher,
        mockDataChunk,
        mockErrorChunk,
        mockLookupDataSource);
  }

  @Test
  public void run_withDataPreparerPresent_readsAndProcessesAllDataChunks()
      throws IOException, LookupServiceClientException {
    when(mockDataReader.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockDataReader.next()).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockDataSourcePreparer.prepare(any())).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockLookupDataSourceResult.erroredLookupResults()).thenReturn(Optional.empty());
    var result = DataMatchResult.create(mockDataChunk, MatchStatistics.emptyInstance());
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    when(mockDataOutputPreparer.prepare(any())).thenReturn(result);

    DataProcessorTask.run(
        mockDataReader,
        mockLookupDataSource,
        mockDataMatcher,
        mockDataWriter,
        Optional.empty(),
        Optional.of(mockDataSourcePreparer),
        Optional.of(mockDataOutputPreparer));

    verify(mockDataReader, times(3)).hasNext();
    verify(mockDataReader, times(2)).next();
    verify(mockDataReader).close();
    verify(mockDataReader).getName();
    verify(mockDataMatcher, times(2)).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataSourcePreparer, times(2)).prepare(mockDataChunk);
    verify(mockDataWriter).close();
    verify(mockDataWriter, times(2)).write(mockDataChunk);
    verify(mockLookupDataSource, times(2)).lookup(mockDataChunk, Optional.empty());
    verifyNoMoreInteractions(
        mockDataReader,
        mockDataWriter,
        mockDataMatcher,
        mockDataChunk,
        mockErrorChunk,
        mockLookupDataSource);
  }

  @Test
  public void run_whenStreamDataSourceThrows_fails() throws IOException {
    when(mockDataReader.hasNext()).thenReturn(true);
    when(mockDataReader.next()).thenThrow(RuntimeException.class);

    assertThrows(
        RuntimeException.class,
        () ->
            DataProcessorTask.run(
                mockDataReader,
                mockLookupDataSource,
                mockDataMatcher,
                mockDataWriter,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

    verify(mockDataReader).hasNext();
    verify(mockDataReader).next();
    verify(mockDataReader).close();
    verify(mockDataReader).getName();
    verify(mockDataWriter).close();
    verifyNoMoreInteractions(
        mockDataReader,
        mockDataSourcePreparer,
        mockDataWriter,
        mockDataMatcher,
        mockDataChunk,
        mockErrorChunk,
        mockLookupDataSource);
  }

  @Test
  public void run_whenDataMatcherThrows_fails() throws IOException, LookupServiceClientException {
    when(mockDataReader.hasNext()).thenReturn(true);
    when(mockDataReader.next()).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockLookupDataSourceResult.erroredLookupResults()).thenReturn(Optional.empty());
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk)))
        .thenThrow(RuntimeException.class);

    assertThrows(
        RuntimeException.class,
        () ->
            DataProcessorTask.run(
                mockDataReader,
                mockLookupDataSource,
                mockDataMatcher,
                mockDataWriter,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

    verify(mockDataReader).hasNext();
    verify(mockDataReader).next();
    verify(mockDataReader).getName();
    verify(mockDataReader).close();
    verify(mockDataMatcher).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriter).close();
    verify(mockLookupDataSource).lookup(mockDataChunk, Optional.empty());
    verifyNoMoreInteractions(
        mockDataReader,
        mockDataSourcePreparer,
        mockDataWriter,
        mockDataMatcher,
        mockDataChunk,
        mockErrorChunk,
        mockLookupDataSource);
  }

  @Test
  public void run_whenDataWriterThrows_fails() throws IOException, LookupServiceClientException {
    when(mockDataReader.hasNext()).thenReturn(true);
    when(mockDataReader.next()).thenReturn(mockDataChunk).thenReturn(mockDataChunk);
    when(mockLookupDataSource.lookup(mockDataChunk, Optional.empty()))
        .thenReturn(mockLookupDataSourceResult);
    when(mockLookupDataSourceResult.lookupResults()).thenReturn(mockDataChunk);
    when(mockLookupDataSourceResult.erroredLookupResults()).thenReturn(Optional.empty());
    var result = DataMatchResult.create(mockDataChunk, MatchStatistics.emptyInstance());
    when(mockDataMatcher.match(eq(mockDataChunk), eq(mockDataChunk))).thenReturn(result);
    doThrow(RuntimeException.class).when(mockDataWriter).write(mockDataChunk);

    assertThrows(
        RuntimeException.class,
        () ->
            DataProcessorTask.run(
                mockDataReader,
                mockLookupDataSource,
                mockDataMatcher,
                mockDataWriter,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));

    verify(mockDataReader).hasNext();
    verify(mockDataReader).next();
    verify(mockDataReader).close();
    verify(mockDataReader).getName();
    verify(mockDataMatcher).match(eq(mockDataChunk), eq(mockDataChunk));
    verify(mockDataWriter).write(mockDataChunk);
    verify(mockDataWriter).close();
    verify(mockLookupDataSource).lookup(mockDataChunk, Optional.empty());
    verifyNoMoreInteractions(
        mockDataReader,
        mockDataSourcePreparer,
        mockDataWriter,
        mockDataMatcher,
        mockDataChunk,
        mockErrorChunk,
        mockLookupDataSource);
  }
}
