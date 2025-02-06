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

package com.google.cm.mrp.dataprocessor.destinations;

import java.io.File;
import java.io.IOException;

/**
 * Interface representing data destination that can be used to upload processed data files by the
 * DataProcessor.
 */
public interface DataDestination {
  /** Upload the given file to the data destination location with a given name. */
  void write(File file, String name) throws IOException;
}
