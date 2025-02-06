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

package com.google.cm.mrp.clients.cryptoclient.converters;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cm.mrp.JobProcessorException;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.CoordinatorKey;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class AeadProviderParametersConverterTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Test
  public void convertToAeadProviderParameters_success() {
    String testWip = "wip";
    var gcpEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setWrappedEncryptionKeys(
                WrappedEncryptionKeys.newBuilder()
                    .setEncryptedDek("dek")
                    .setKekUri("kek")
                    .setGcpWrappedKeys(GcpWrappedKeys.newBuilder().setWipProvider(testWip)))
            .build();

    AeadProviderParameters result =
        AeadProviderParametersConverter.convertToAeadProviderParameters(gcpEncryptionKeys);

    assertThat(result.gcpParameters()).isPresent();
    assertThat(result.gcpParameters().get().wipProvider()).isEqualTo(testWip);
    assertThat(result.gcpParameters().get().serviceAccountToImpersonate()).isEmpty();
  }

  @Test
  public void convertToAeadProviderParametersInvalidKey_throws() {
    var coordEncryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.getDefaultInstance())
            .build();

    var ex =
        assertThrows(
            JobProcessorException.class,
            () ->
                AeadProviderParametersConverter.convertToAeadProviderParameters(
                    coordEncryptionKeys));

    assertThat(ex.getErrorCode()).isEqualTo(CRYPTO_CLIENT_CONFIGURATION_ERROR);
    assertThat(ex.getMessage())
        .isEqualTo("AeadProviderParameters attempted to convert invalid DataRecordEncryptionKeys.");
  }
}
