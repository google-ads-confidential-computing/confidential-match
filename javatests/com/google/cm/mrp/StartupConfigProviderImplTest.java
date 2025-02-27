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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.ApplicationProto.ApplicationId;
import com.google.common.collect.ImmutableMap;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.ParameterClient.ParameterClientException;
import com.google.scp.shared.clients.configclient.model.ErrorReason;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class StartupConfigProviderImplTest {

  @Rule
  public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock
  private ParameterClient mockParameterClient;
  private StartupConfigProvider configProvider;

  @Before
  public void setup() throws Exception {
    configProvider = new StartupConfigProviderImpl(mockParameterClient);
  }

  @Test
  public void getStartupConfig_noParametersReturnDefault() throws Exception {
    StartupConfig startupConfig = configProvider.getStartupConfig();

    assertThat(startupConfig.conscryptEnabled()).isEqualTo(false);
    assertThat(startupConfig.loggingLevel()).isEqualTo(Optional.empty());
    assertThat(startupConfig.notificationTopics()).isEqualTo(ImmutableMap.of());
  }

  @Test
  public void getStartupConfig_nonEmptyTopicsReturnsExpectedMap() throws Exception {
    when(mockParameterClient.getParameter(
        Parameter.NOTIFICATION_TOPIC_PREFIX + ApplicationId.MIC.name(),
        Optional.of(Parameter.CFM_PREFIX),
        true))
        .thenReturn(Optional.of("fake_mic_topic"));
    when(mockParameterClient.getParameter(
        Parameter.NOTIFICATION_TOPIC_PREFIX + ApplicationId.CUSTOMER_MATCH.name(),
        Optional.of(Parameter.CFM_PREFIX),
        true))
        .thenReturn(Optional.of("fake_customer_match_topic"));

    StartupConfig startupConfig = configProvider.getStartupConfig();

    assertThat(startupConfig.notificationTopics()).isEqualTo(
        ImmutableMap.of(
            "mic", "fake_mic_topic", "customer_match", "fake_customer_match_topic"));
  }

  @Test
  public void getStartupConfig_returnsLoggingParameter() throws Exception {
    when(mockParameterClient.getParameter(
        Parameter.LOGGING_LEVEL.name(),
        Optional.of(Parameter.CFM_PREFIX),
        true))
        .thenReturn(Optional.of("WARN"));

    StartupConfig startupConfig = configProvider.getStartupConfig();

    assertThat(startupConfig.loggingLevel()).isEqualTo(Optional.of("WARN"));
  }

  @Test
  public void getStartupConfig_callsParameterClientForAllValidApplicationIds() throws Exception {
    configProvider.getStartupConfig();

    for (ApplicationId applicationId : ApplicationId.values()) {
      if (applicationId != ApplicationId.APPLICATION_ID_UNSPECIFIED) {
        verify(mockParameterClient, times(1))
            .getParameter(
                Parameter.NOTIFICATION_TOPIC_PREFIX + applicationId.name(),
                Optional.of(Parameter.CFM_PREFIX), true);
      }
    }
    verify(mockParameterClient, never())
        .getParameter(
            Parameter.NOTIFICATION_TOPIC_PREFIX + ApplicationId.APPLICATION_ID_UNSPECIFIED,
            Optional.of(Parameter.CFM_PREFIX), true);
  }

  @Test
  public void getStartupConfig_parameterClientThrowsException() throws Exception {
    when(mockParameterClient.getParameter(
        Parameter.NOTIFICATION_TOPIC_PREFIX + ApplicationId.MIC.name(),
        Optional.of(Parameter.CFM_PREFIX),
        true))
        .thenThrow(new ParameterClientException("Parameter Client Error", ErrorReason.FETCH_ERROR));

    var ex = assertThrows(RuntimeException.class, () -> configProvider.getStartupConfig());
    assertThat(ex.getMessage()).contains("Unable to fetch flag");
  }
}
