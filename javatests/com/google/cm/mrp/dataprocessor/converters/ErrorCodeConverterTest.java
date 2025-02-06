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

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.LOOKUP_SERVICE_INVALID_ERROR;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ErrorCodeConverterTest {

  @Test
  public void convertToJobResultCode_success() {
    String decryptionCode = "2415853572";

    JobResultCode result = ErrorCodeConverter.convertToJobResultCode(decryptionCode);

    assertThat(result).isEqualTo(DECRYPTION_ERROR);
  }

  @Test
  public void convertToJobResultCode_UnknownCode_ThrowsException() {
    String code = "unknown";

    var ex =
        assertThrows(
            JobProcessorException.class, () -> ErrorCodeConverter.convertToJobResultCode(code));

    assertThat(ex.getMessage()).isEqualTo("Unexpected lookupServerErrorReason: unknown");
    assertThat(ex.getErrorCode()).isEqualTo(LOOKUP_SERVICE_INVALID_ERROR);
  }
}
