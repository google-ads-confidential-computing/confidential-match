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

package com.google.cm.mrp.clients.cryptoclient.gcp;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK_FORMAT;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.clients.cryptoclient.exceptions.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.exceptions.UncheckedAeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters.GcpParameters;
import com.google.cm.mrp.clients.cryptoclient.models.DekKeysetReader;
import com.google.crypto.tink.Aead;
import com.google.protobuf.ByteString;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class GcpAeadProviderTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String TEST_WIP = "WIP";
  private static final AeadProviderParameters TEST_PARAMETERS =
      AeadProviderParameters.builder()
          .setGcpParameters(GcpParameters.builder().setWipProvider(TEST_WIP).build())
          .build();

  @Mock private Aead mockAead;

  private GcpAeadProvider aeadProvider;

  @Before
  public void setup() {
    aeadProvider = new GcpAeadProvider();
  }

  @Test
  public void getAeadSelector_noPrefix_success() throws Exception {
    CloudAeadSelector result = aeadProvider.getAeadSelector(TEST_PARAMETERS);
    // Verify KmsKekUri can be read
    Aead aead = result.getAead("projects/test/locations/us-central1/keyRings/test/cryptoKeys/test");
  }

  @Test
  public void getAeadSelector_invalidKekFormat_fail() throws Exception {
    CloudAeadSelector result = aeadProvider.getAeadSelector(TEST_PARAMETERS);

    UncheckedAeadProviderException ex =
        assertThrows(UncheckedAeadProviderException.class, () -> result.getAead("invalid"));

    assertThat(ex.getJobResultCode()).isEqualTo(INVALID_KEK_FORMAT);
    assertThat(ex).hasMessageThat().isEqualTo("Invalid format for KEK.");
  }

  @Test
  public void readKeysetHandle_invalidKekFormat_fail() throws Exception {
    when(mockAead.decrypt(any(), any()))
        .thenThrow(new IllegalArgumentException("Parameter name must conform to the pattern"));

    DekKeysetReader dekReader = new DekKeysetReader(ByteString.copyFromUtf8("test"));
    AeadProviderException ex =
        assertThrows(
            AeadProviderException.class, () -> aeadProvider.readKeysetHandle(dekReader, mockAead));

    assertThat(ex.getJobResultCode()).isEqualTo(INVALID_KEK_FORMAT);
    assertThat(ex).hasMessageThat().isEqualTo("Invalid format for KEK.");
  }
}
