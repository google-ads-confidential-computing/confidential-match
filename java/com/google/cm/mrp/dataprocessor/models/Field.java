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

/**
 * Encapsulates a field within a DataRecord from a key and a value.
 *
 * <p>We need this AutoValue class, because KeyValue proto objects cannot be used in HashSets and in
 * HashMaps as keys. (See b/111348251). This is due to the stability, the way, and the timing of
 * hashCode calculation in proto objects. If we construct two proto objects with the same values,
 * for example one using blank newBuilder() and another one using an existing object
 * newBuilder(clonedObject), then their hashCodes may not match, even though all the fields are the
 * same.
 *
 * <p>We want to build a HashSet of fields, so that we can find matches quickly using the contains()
 * method.
 */
@AutoValue
public abstract class Field {
  public abstract String key();

  public abstract String value();

  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_Field.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /** Create a new {@link Field} from the builder. */
    public abstract Field build();

    /** Set the key. */
    public abstract Builder setKey(String key);

    /** Set the value. */
    public abstract Builder setValue(String value);
  }
}
