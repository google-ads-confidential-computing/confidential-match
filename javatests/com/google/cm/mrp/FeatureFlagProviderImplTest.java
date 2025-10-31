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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class FeatureFlagProviderImplTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private ParameterClient mockParameterClient;
  private FeatureFlagProvider featureFlagProvider;

  @Before
  public void setup() throws Exception {
    featureFlagProvider = new FeatureFlagProviderImpl(mockParameterClient);
  }

  @Test
  public void getFeatureFlags_success() throws Exception {
    when(mockParameterClient.getParameter(
            Parameter.MIC_FEATURE_ENABLED.name(), Optional.of(Parameter.CFM_PREFIX), true))
        .thenReturn(Optional.of("true"));

    FeatureFlags flags = featureFlagProvider.getFeatureFlags();

    assertThat(flags.enableMIC()).isEqualTo(true);
    assertThat(flags.largeJobThresholdBytes()).isEqualTo(1024 * 1024 * 1024);
    verify(mockParameterClient, times(1))
        .getParameter(
            Parameter.MIC_FEATURE_ENABLED.name(), Optional.of(Parameter.CFM_PREFIX), true);
  }

  @Test
  public void getFeatureFlags_cacheSuccess() throws Exception {
    when(mockParameterClient.getParameter(
            Parameter.MIC_FEATURE_ENABLED.name(), Optional.of(Parameter.CFM_PREFIX), true))
        .thenReturn(Optional.of("true"));

    FeatureFlags option1 = featureFlagProvider.getFeatureFlags();
    FeatureFlags option2 = featureFlagProvider.getFeatureFlags();

    assertThat(option1.enableMIC()).isEqualTo(true);
    assertThat(option2.enableMIC()).isEqualTo(true);
    verify(mockParameterClient, times(1))
        .getParameter(
            Parameter.MIC_FEATURE_ENABLED.name(), Optional.of(Parameter.CFM_PREFIX), true);
  }

  @Test
  public void getFeatureFlags_returnsAssignedWorkgroups() throws Exception {
    when(mockParameterClient.getParameter(
            Parameter.ASSIGNED_WORKGROUP_PREFIX + "mic", Optional.of(Parameter.CFM_PREFIX), true))
        .thenReturn(Optional.of("fake_mic_group"));
    when(mockParameterClient.getParameter(
            Parameter.ASSIGNED_WORKGROUP_PREFIX + "customer_match",
            Optional.of(Parameter.CFM_PREFIX),
            true))
        .thenReturn(Optional.of("fake_cm_group"));

    FeatureFlags featureFlags = featureFlagProvider.getFeatureFlags();

    assertThat(featureFlags.applicationIdWorkgroups())
        .isEqualTo(ImmutableMap.of("mic", "fake_mic_group", "customer_match", "fake_cm_group"));
  }

  @Test
  public void getFeatureFlags_returnsLargeJobApplicationIds() throws Exception {
    when(mockParameterClient.getParameter(
            Parameter.LARGE_JOB_APPLICATION_IDS.name(), Optional.of(Parameter.CFM_PREFIX), true))
        .thenReturn(Optional.of("mic,copla"));

    FeatureFlags featureFlags = featureFlagProvider.getFeatureFlags();

    assertThat(featureFlags.largeJobApplicationIds()).containsExactly("mic", "copla");
  }

  @Test
  public void getFeatureFlags_parameterClientThrowException() throws Exception {
    when(mockParameterClient.getParameter(
            Parameter.MIC_FEATURE_ENABLED.name(), Optional.of(Parameter.CFM_PREFIX), true))
        .thenThrow(new ParameterClientException("invalid key", ErrorReason.FETCH_ERROR));

    var ex = assertThrows(RuntimeException.class, () -> featureFlagProvider.getFeatureFlags());
    assertThat(ex.getMessage()).isEqualTo("Unable to get feature flags from cache");
  }
}
