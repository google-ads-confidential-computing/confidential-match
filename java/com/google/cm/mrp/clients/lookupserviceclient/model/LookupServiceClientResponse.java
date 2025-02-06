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

package com.google.cm.mrp.clients.lookupserviceclient.model;

import com.google.auto.value.AutoValue;
import com.google.cm.lookupserver.api.LookupProto.LookupResult;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** A response from the lookup service client. */
@AutoValue
public abstract class LookupServiceClientResponse {

  /** Returns a new builder. */
  public static LookupServiceClientResponse.Builder builder() {
    return new AutoValue_LookupServiceClientResponse.Builder();
  }

  /** Results returned from the lookup service. */
  public abstract ImmutableList<LookupResult> results();

  /** Builder for {@link LookupServiceClientResponse}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Create a new {@link LookupServiceClientResponse} from the builder. */
    public abstract LookupServiceClientResponse build();

    /** Set the list of data records. */
    public abstract LookupServiceClientResponse.Builder setResults(List<LookupResult> results);

    /** Builder for the list of data records. */
    protected abstract ImmutableList.Builder<LookupResult> resultsBuilder();

    /** Add a data record. */
    public LookupServiceClientResponse.Builder addResult(LookupResult result) {
      resultsBuilder().add(result);
      return this;
    }

    /** Add a data record. */
    public LookupServiceClientResponse.Builder addResult(LookupResult.Builder result) {
      return addResult(result.build());
    }
  }
}
