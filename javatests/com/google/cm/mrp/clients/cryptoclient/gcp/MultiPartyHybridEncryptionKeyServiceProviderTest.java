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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.CoordinatorKeyInfo;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.AeadProviderFactory;
import com.google.cm.mrp.clients.cryptoclient.HybridEncryptionKeyServiceProvider.HybridEncryptionKeyServiceProviderException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class MultiPartyHybridEncryptionKeyServiceProviderTest {
  private static final String ENDPOINT_1 = "testEndpoint1";
  private static final String ENDPOINT_2 = "testEndpoint2";
  private static final String WIP_1 = "testWip1";
  private static final String WIP_2 = "testWip2";
  private static final String SA_1 = "testIdentity1";
  private static final String SA_2 = "testIdentity2";
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private AeadProvider mockAeadProvider;
  @Mock private AeadProviderFactory mockAeadProviderFactory;
  @Mock private CloudAeadSelector mockAeadSelector;
  @Captor private ArgumentCaptor<AeadProviderParameters> parametersCaptor;
  private MultiPartyHybridEncryptionKeyServiceProvider decryptionKeyServiceProvider;

  @Before
  public void setUp() {
    when(mockAeadProviderFactory.createGcpAeadProvider()).thenReturn(mockAeadProvider);
    decryptionKeyServiceProvider =
        new MultiPartyHybridEncryptionKeyServiceProvider(mockAeadProviderFactory);
  }

  @Test
  public void getHybridEncryptionKeyService_success()
      throws HybridEncryptionKeyServiceProviderException, AeadProviderException {
    when(mockAeadProvider.getAeadSelector(parametersCaptor.capture())).thenReturn(mockAeadSelector);
    CoordinatorKeyInfo coordinatorKeyInfo =
        CoordinatorKeyInfo.newBuilder()
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_1)
                    .setKmsWipProvider(WIP_1)
                    .setKmsIdentity(SA_1))
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_2)
                    .setKmsWipProvider(WIP_2)
                    .setKmsIdentity(SA_2))
            .build();

    var testService =
        decryptionKeyServiceProvider.getHybridEncryptionKeyService(coordinatorKeyInfo);

    assertThat(testService).isNotNull();
    var parametersList = parametersCaptor.getAllValues();
    assertThat(
            parametersList.stream()
                .map(p -> p.gcpParameters().get().wipProvider())
                .collect(Collectors.toList()))
        .containsExactly(WIP_1, WIP_2);
    assertThat(
            parametersList.stream()
                .map(p -> p.gcpParameters().get().serviceAccountToImpersonate())
                .collect(Collectors.toList()))
        .containsExactly(Optional.of(SA_1), Optional.of(SA_2));
  }

  @Test
  public void getHybridEncryptionKeyService_successWithEmptyKmsIdentity()
      throws HybridEncryptionKeyServiceProviderException, AeadProviderException {
    when(mockAeadProvider.getAeadSelector(parametersCaptor.capture())).thenReturn(mockAeadSelector);
    CoordinatorKeyInfo coordinatorKeyInfo =
        CoordinatorKeyInfo.newBuilder()
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_1)
                    .setKmsWipProvider(WIP_1))
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_2)
                    .setKmsWipProvider(WIP_2))
            .build();

    var testService =
        decryptionKeyServiceProvider.getHybridEncryptionKeyService(coordinatorKeyInfo);

    assertThat(testService).isNotNull();
    var parametersList = parametersCaptor.getAllValues();
    assertThat(
            parametersList.stream()
                .map(p -> p.gcpParameters().get().wipProvider())
                .collect(Collectors.toList()))
        .containsExactly(WIP_1, WIP_2);
    assertThat(
            parametersList.stream()
                .map(p -> p.gcpParameters().get().serviceAccountToImpersonate())
                .collect(Collectors.toList()))
        .containsExactly(Optional.empty(), Optional.empty());
  }

  @Test
  public void getHybridEncryptionKeyService_failureThrowsException() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(mockAeadSelector);
    CoordinatorKeyInfo coordinatorKeyInfo =
        CoordinatorKeyInfo.newBuilder()
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_1)
                    .setKmsWipProvider(WIP_1)
                    .setKmsIdentity(SA_1))
            .build();

    assertThrows(
        HybridEncryptionKeyServiceProviderException.class,
        () -> decryptionKeyServiceProvider.getHybridEncryptionKeyService(coordinatorKeyInfo));
  }

  @Test
  public void getHybridEncryptionKeyService_whenAeadProviderFailsThenThrows() throws Exception {
    when(mockAeadProvider.getAeadSelector(any())).thenThrow(AeadProviderException.class);
    CoordinatorKeyInfo coordinatorKeyInfo =
        CoordinatorKeyInfo.newBuilder()
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_1)
                    .setKmsWipProvider(WIP_1)
                    .setKmsIdentity(SA_1))
            .addCoordinatorInfo(
                CoordinatorInfo.newBuilder()
                    .setKeyServiceEndpoint(ENDPOINT_2)
                    .setKmsWipProvider(WIP_2)
                    .setKmsIdentity(SA_2))
            .build();

    assertThrows(
        HybridEncryptionKeyServiceProviderException.class,
        () -> decryptionKeyServiceProvider.getHybridEncryptionKeyService(coordinatorKeyInfo));
  }
}
