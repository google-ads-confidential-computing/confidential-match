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

package com.google.cm.mrp.clients.lookupserviceclient;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.google.inject.BindingAnnotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executor;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;

/** Guice annotations for dependency injection */
public final class Annotations {

  private Annotations() {}

  /** Annotation for binding an {@link Executor} for the lookup service client to use. */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  public @interface LookupServiceClientExecutor {}

  /** Annotation for binding the cluster group ID for the lookup service client. */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  public @interface LookupServiceClientClusterGroupId {}

  /**
   * Annotation for binding the maximum number of records included in each request the lookup
   * service client makes to each individual lookup service shard.
   */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  public @interface LookupServiceClientMaxRecordsPerRequest {}

  /**
   * Annotation for binding a {@link CloseableHttpAsyncClient} for the lookup service shard client
   * to use.
   */
  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  public @interface LookupServiceShardClientHttpClient {}
}
