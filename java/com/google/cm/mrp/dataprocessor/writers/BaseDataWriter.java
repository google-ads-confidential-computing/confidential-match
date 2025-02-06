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

package com.google.cm.mrp.dataprocessor.writers;

import com.google.cm.mrp.JobProcessorException;
import java.io.File;
import org.slf4j.Logger;

/** Abstract {@link DataWriter} for implementations with files. */
public abstract class BaseDataWriter implements DataWriter {

  /** Used for writing out files. */
  protected File file;

  /** Returns the logger used for a given implementation. */
  protected abstract Logger getLogger();

  /** Deletes files. */
  protected void deleteFile() throws JobProcessorException {
    if (!file.delete()) {
      String message =
          String.format(
              "Unable to delete the local file: %s after uploading", file.getAbsolutePath());
      getLogger().error(message);
    }
  }

  /*
   * This method returns a new filename given the original filename and a sequential number.
   * E.g.: filename: 'folder1/folder2/filename.txt' and number: 3
   *       will return: 'filename_3.txt'
   * We need to strip the folder names from the filename, otherwise we will have nested folder
   *    from the input folder inside the output folder.
   */
  protected static String getFilename(String name, int number) {
    String filename = name;
    int index = name.lastIndexOf(File.separator);
    if (index != -1) {
      filename = name.substring(index + 1);
    }

    index = filename.lastIndexOf('.');
    if (index == -1) {
      return filename + '_' + number;
    } else {
      return filename.substring(0, index) + '_' + number + filename.substring(index);
    }
  }
}
