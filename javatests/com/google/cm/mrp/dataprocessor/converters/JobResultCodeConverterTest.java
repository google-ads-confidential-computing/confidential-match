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

import static com.google.common.truth.Truth.assertThat;

import com.google.cm.mrp.api.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.backend.JobResultCodeProto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JobResultCodeConverterTest {

  @Test
  public void convert_success() {
    for (var backendCode : JobResultCodeProto.JobResultCode.values()) {
      if (backendCode == JobResultCodeProto.JobResultCode.UNRECOGNIZED) {
        continue;
      }
      if (backendCode.getNumber() > 1 && backendCode.getNumber() < 100) {
        assertThat(JobResultCodeConverter.convert(backendCode))
            .isEqualTo(JobResultCode.RETRY_IN_PROGRESS);
      } else {
        assertThat(JobResultCodeConverter.convert(backendCode))
            .isEqualTo(JobResultCode.valueOf(backendCode.name()));
      }
    }
  }
}
