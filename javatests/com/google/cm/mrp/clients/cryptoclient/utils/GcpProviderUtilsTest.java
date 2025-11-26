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

package com.google.cm.mrp.clients.cryptoclient.utils;

import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.DEK_DECRYPTION_ERROR;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INSUFFICIENT_CUSTOMER_QUOTA;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.INVALID_KEK;
import static com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode.KEK_PERMISSION_DENIED;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.testutils.fakes.OAuthException;
import java.security.GeneralSecurityException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GcpProviderUtilsTest {

  @Test
  public void handleWipFailure_authFailed() {
    var ex = new GeneralSecurityException(new OAuthException("unauthorized_client"));

    var result = GcpProviderUtils.tryParseWipException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(JobResultCode.WIP_AUTH_FAILED);
    assertThat(result.get()).hasMessageThat().isEqualTo("WIP conditions failed.");
  }

  @Test
  public void handleWipFailure_invalidWipFormat() {
    var ex = new GeneralSecurityException(new OAuthException("invalid_request"));

    var result = GcpProviderUtils.tryParseWipException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(JobResultCode.INVALID_WIP_FORMAT);
    assertThat(result.get()).hasMessageThat().isEqualTo("WIP parameter in an invalid format.");
  }

  @Test
  public void handleWipFailure_invalidWip() {
    var ex = new GeneralSecurityException(new OAuthException("invalid_target"));

    var result = GcpProviderUtils.tryParseWipException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(JobResultCode.INVALID_WIP_PARAMETER);
    assertThat(result.get()).hasMessageThat().isEqualTo("WIP parameter invalid.");
  }

  @Test
  public void handleWipFailure_customerQuota_returnsCustomerErrorCode() {
    var ex =
        new GeneralSecurityException(
            new OAuthException(
                "Error code quota_exceeded: [Security Token Service] The request was throttled due"
                    + " to rate limit on the following metrics: 'Token exchange requests'"));

    var result = GcpProviderUtils.tryParseWipException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(INSUFFICIENT_CUSTOMER_QUOTA);
    assertThat(result.get())
        .hasMessageThat()
        .isEqualTo("WIP token could not be fetched due to customer quota limits.");
  }

  @Test
  public void handleWipFailure_undefinedError_notPresent() {
    var ex = new GeneralSecurityException(new OAuthException("Error code quota_exceeded: []"));

    var result = GcpProviderUtils.tryParseWipException(ex);

    assertThat(result).isEmpty();
  }

  @Test
  public void handleGcpKmsException_invalidKek() {
    var details = new GoogleJsonError();
    details.setCode(400);
    details.setMessage("");
    var ex =
        new GeneralSecurityException(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(400, "error", new HttpHeaders()), details));

    var result = GcpProviderUtils.tryParseGcpKmsException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(INVALID_KEK);
    assertThat(result.get())
        .hasMessageThat()
        .isEqualTo("KEK could not decrypt data, most likely incorrect KEK.");
  }

  @Test
  public void handleGcpKmsException_invalidDek() {
    var details = new GoogleJsonError();
    details.setCode(400);
    details.setMessage("Decryption failed: the ciphertext is invalid");
    var ex =
        new GeneralSecurityException(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(400, "error", new HttpHeaders()), details));

    var result = GcpProviderUtils.tryParseGcpKmsException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(DEK_DECRYPTION_ERROR);
    assertThat(result.get())
        .hasMessageThat()
        .isEqualTo("Cloud KMS marked DEK as invalid and cannot be decrypted.");
  }

  @Test
  public void handleGcpKmsException_kekPermissionDenied() {
    var details = new GoogleJsonError();
    details.setCode(403);
    details.setMessage("");
    var ex =
        new GeneralSecurityException(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(403, "error", new HttpHeaders()), details));

    var result = GcpProviderUtils.tryParseGcpKmsException(ex);

    assertThat(result).isPresent();
    assertThat(result.get().getJobResultCode()).isEqualTo(KEK_PERMISSION_DENIED);
    assertThat(result.get())
        .hasMessageThat()
        .isEqualTo("Permission denied when trying to use KEK.");
  }
}
