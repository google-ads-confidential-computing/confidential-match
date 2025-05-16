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

import static com.google.cm.mrp.backend.ApplicationProto.ApplicationId.APPLICATION_ID_UNSPECIFIED;
import static com.google.cm.mrp.backend.ApplicationProto.ApplicationId.UNRECOGNIZED;
import static com.google.common.truth.Truth.assertThat;

import com.google.cm.mrp.backend.ApplicationProto.ApplicationId;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MatchConfigProviderTest {

  @Test
  public void getMatchConfig_allConfigs_success() {
    for (ApplicationId applicationId : ApplicationId.values()) {
      if (applicationId == UNRECOGNIZED || applicationId == APPLICATION_ID_UNSPECIFIED) {
        continue;
      }

      // Test matchConfig is able to be parsed successfully
      MatchConfig config = MatchConfigProvider.getMatchConfig(applicationId.name());

      assertThat(config.getApplicationId()).isEqualTo(applicationId);
      assertThat(config.getMatchConditionsList()).isNotEmpty();
      assertThat(config.hasSuccessConfig()).isTrue();
    }
  }

  @Test
  public void getMatchConfig_allConfigsLowercase_success() {
    for (ApplicationId applicationId : ApplicationId.values()) {
      if (applicationId == UNRECOGNIZED || applicationId == APPLICATION_ID_UNSPECIFIED) {
        continue;
      }

      // Test matchConfig is able to be parsed successfully
      MatchConfig config = MatchConfigProvider.getMatchConfig(applicationId.name().toLowerCase());

      assertThat(config.getApplicationId()).isEqualTo(applicationId);
      assertThat(config.getMatchConditionsList()).isNotEmpty();
      assertThat(config.hasSuccessConfig()).isTrue();
    }
  }
}
