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

package com.google.cm.mrp.dataprocessor.models;

import com.google.auto.value.AutoValue;
import java.util.Optional;

/** Contains results from LookupServerDataSource. */
@AutoValue
public abstract class LookupDataSourceResult {

  /** Creates a new instance. */
  public static LookupDataSourceResult create(DataChunk lookupResultsChunk) {
    var builder = new AutoValue_LookupDataSourceResult.Builder();
    builder.setLookupResults(lookupResultsChunk);
    return builder.build();
  }

  /** Contains lookup results in a {@link DataChunk} */
  public abstract DataChunk lookupResults();

  /** Contains lookup results with errors in a {@link DataChunk} */
  public abstract Optional<DataChunk> erroredLookupResults();

  /** Returns a new builder. */
  public static LookupDataSourceResult.Builder builder() {
    return new AutoValue_LookupDataSourceResult.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets LookupResultsChunk */
    public abstract Builder setLookupResults(DataChunk results);

    /** Sets lookup results with errors in a {@link DataChunk}, if any */
    public abstract Builder setErroredLookupResults(Optional<DataChunk> errorResults);

    /** Creates a new {@link LookupDataSourceResult} from the builder. */
    public abstract LookupDataSourceResult build();
  }
}
