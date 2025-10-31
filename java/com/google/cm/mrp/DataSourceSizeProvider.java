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

package com.google.cm.mrp;

import com.google.cm.mrp.api.CreateJobParametersProto.JobParameters.DataOwnerList;
import java.util.Optional;

/** Provides the size of a particular data source before job processing */
public interface DataSourceSizeProvider {

  /** Determines whether any one data source location is at least a given size in bytes */
  boolean isAtLeastSize(
      long bytesSize, DataOwnerList dataSourceList, Optional<String> accountIdentity);
}
