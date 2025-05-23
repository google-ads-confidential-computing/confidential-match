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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cm.mrp.clients.cryptoclient.gcp.CredentialsHelper;
import java.io.IOException;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class CredentialsHelperTest {

  @Test
  public void getAttestedCredentialsWithWipSa_success() throws IOException {
    String wip = "wip";
    Optional<String> sa = Optional.of("serviceAccount");

    GoogleCredentials creds = CredentialsHelper.getAttestedCredentials(wip, sa);

    assertThat(creds).isNotNull();
  }

  @Test
  public void getAttestedCredentialsWithWipOnly_success() throws IOException {
    String wip = "wip";
    Optional<String> sa = Optional.empty();

    GoogleCredentials creds = CredentialsHelper.getAttestedCredentials(wip, sa);

    assertThat(creds).isNotNull();
  }
}
