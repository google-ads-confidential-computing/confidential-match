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

package com.google.cm.util.gcp;

import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auth.oauth2.IdTokenProvider.Option;
import com.google.auth.oauth2.UserCredentials;
import java.io.IOException;
import java.util.List;

/** Utility methods for use with GCP. */
public final class AuthUtils {

  private AuthUtils() {}

  /** Returns the application default credentials. */
  public static GoogleCredentials getDefaultCredentials() throws IOException {
    return GoogleCredentials.getApplicationDefault();
  }

  /** Returns identity token credentials based on the given audience and default credentials. */
  public static IdTokenCredentials getIdTokenCredentials(String audienceUrl) throws IOException {
    return getIdTokenCredentials(audienceUrl, getDefaultCredentials());
  }

  /** Returns identity token credentials based on the given audience and credentials. */
  public static IdTokenCredentials getIdTokenCredentials(
      String audienceUrl, GoogleCredentials credentials) {
    return IdTokenCredentials.newBuilder()
        .setIdTokenProvider((IdTokenProvider) credentials)
        .setTargetAudience(audienceUrl)
        .setOptions(List.of(Option.FORMAT_FULL, Option.LICENSES_TRUE, Option.INCLUDE_EMAIL))
        .build();
  }

  /** Returns the identity token value from the given audience and default credentials. */
  public static String getIdToken(String audienceUrl) throws IOException {
    return getIdToken(audienceUrl, getDefaultCredentials());
  }

  /** Returns the identity token value from the given audience and credentials. */
  public static String getIdToken(String audienceUrl, GoogleCredentials credentials)
      throws IOException {
    return getIdTokenCredentials(audienceUrl, credentials).refreshAccessToken().getTokenValue();
  }

  /** Returns the current user or service account's email. */
  public static String getEmail(GoogleCredentials credentials) {
    // For requests made locally
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials).getClientId();
    }

    // For service accounts
    if (credentials instanceof ServiceAccountSigner) {
      return ((ServiceAccountSigner) credentials).getAccount();
    }

    throw new RuntimeException("Service account could not be found.");
  }
}
