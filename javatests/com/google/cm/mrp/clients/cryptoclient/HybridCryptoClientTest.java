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

package com.google.cm.mrp.clients.cryptoclient;

import static com.google.cm.mrp.testutils.HybridKeyGenerator.decryptString;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridDecrypt;
import static com.google.cm.mrp.testutils.HybridKeyGenerator.getDefaultHybridEncrypt;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.CoordinatorKey;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.testutils.fakes.OAuthException;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.HybridEncrypt;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService;
import com.google.scp.operator.cpio.cryptoclient.HybridEncryptionKeyService.KeyFetchException;
import com.google.scp.operator.cpio.cryptoclient.model.ErrorReason;
import java.security.GeneralSecurityException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class HybridCryptoClientTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private HybridEncrypt mockHybridEncrypt;
  @Mock private HybridDecrypt mockHybridDecrypt;
  @Mock private HybridEncryptionKeyService mockHybridEncryptionKeyService;
  private HybridCryptoClient cryptoClient;

  @Before
  public void setup() throws Exception {
    cryptoClient = new HybridCryptoClient(mockHybridEncryptionKeyService);
  }

  @Test
  public void encrypt_success() throws Exception {
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    HybridDecrypt decrypter = getDefaultHybridDecrypt();

    String encrypted = cryptoClient.encrypt(encryptionKeys, plaintext);

    assertThat(decryptString(decrypter, encrypted)).isEqualTo(plaintext);
    verify(mockHybridEncryptionKeyService, times(1)).getEncrypter(any());
    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(any());
  }

  @Test
  public void decrypt_success() throws Exception {
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    assertThat(cryptoClient.decrypt(encryptionKeys, encrypted)).isEqualTo(plaintext);
    verify(mockHybridEncryptionKeyService, times(1)).getEncrypter(any());
    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(any());
  }

  @Test
  public void primitive_cachedSuccess() throws Exception {
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    HybridDecrypt decrypter = getDefaultHybridDecrypt();

    String encrypted = cryptoClient.encrypt(encryptionKeys, plaintext);

    assertThat(decryptString(decrypter, encrypted)).isEqualTo(plaintext);

    // Another call to `getDecrypter("123")`
    String decrypted = cryptoClient.decrypt(encryptionKeys, encrypted);
    assertThat(decrypted).isEqualTo(plaintext);

    // Verify both primitives were fetched only once.
    verify(mockHybridEncryptionKeyService, times(1)).getEncrypter(anyString());
    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(anyString());
  }

  @Test
  public void encrypt_getEncrypterFailureThrowsException() throws Exception {
    KeyFetchException exception = new KeyFetchException("what", ErrorReason.KEY_NOT_FOUND);
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenThrow(exception);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";

    assertThrows(
        CryptoClientException.class, () -> cryptoClient.encrypt(encryptionKeys, plaintext));
  }

  @Test
  public void encrypt_primitiveEncryptFailureThrowsException() throws Exception {
    when(mockHybridEncrypt.encrypt(any(), any())).thenThrow(GeneralSecurityException.class);
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(mockHybridEncrypt);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";

    assertThrows(
        CryptoClientException.class, () -> cryptoClient.encrypt(encryptionKeys, plaintext));
  }

  @Test
  public void decrypt_decryptionErrorThrowsException() throws Exception {
    when(mockHybridDecrypt.decrypt(any(), any())).thenThrow(GeneralSecurityException.class);
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(mockHybridDecrypt);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);
  }

  @Test
  public void decrypt_keyDoesNotExistException() throws Exception {
    KeyFetchException keyFetchException =
        new KeyFetchException(new RuntimeException(), ErrorReason.KEY_NOT_FOUND);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenThrow(keyFetchException);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);

    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(any());
  }

  @Test
  public void decrypt_sameKeyDoesNotExistException() throws Exception {
    KeyFetchException keyFetchException =
        new KeyFetchException(new RuntimeException(), ErrorReason.KEY_NOT_FOUND);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenThrow(keyFetchException);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    var ex1 =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex1.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);
    var ex2 =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex2.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);

    // Verify that key fetch is only attempted once for non-existent key
    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(any());
  }

  @Test
  public void decrypt_keyServiceUnavailableExceptionSendsRetriableErrorCode() throws Exception {
    KeyFetchException keyFetchException =
        new KeyFetchException(new RuntimeException(), ErrorReason.KEY_SERVICE_UNAVAILABLE);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenThrow(keyFetchException);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));

    // Verify that the CryptoClientException being thrown has retriable error code
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.COORDINATOR_KEY_SERVICE_ERROR);
  }

  @Test
  public void decrypt_invalidWipThrowsException() throws Exception {
    KeyFetchException keyFetchException =
        new KeyFetchException(
            new GeneralSecurityException(new OAuthException("invalid_target")),
            ErrorReason.KEY_DECRYPTION_ERROR);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenThrow(keyFetchException);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.INVALID_WIP_PARAMETER);

    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(any());
  }

  @Test
  public void decrypt_wipConditionFailedException() throws Exception {
    KeyFetchException keyFetchException =
        new KeyFetchException(
            new GeneralSecurityException(new OAuthException("unauthorized_client")),
            ErrorReason.KEY_DECRYPTION_ERROR);
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenThrow(keyFetchException);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("123").build())
            .build();
    String plaintext = "TestString";
    String encrypted = encryptString(getDefaultHybridEncrypt(), plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.WIP_AUTH_FAILED);

    verify(mockHybridEncryptionKeyService, times(1)).getDecrypter(any());
  }

  @Test
  public void decrypt_cannotDecodeBase64Value_throws() throws Exception {
    when(mockHybridEncryptionKeyService.getEncrypter(any())).thenReturn(getDefaultHybridEncrypt());
    when(mockHybridEncryptionKeyService.getDecrypter(any())).thenReturn(getDefaultHybridDecrypt());
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("cannotDecodeBase64Value"))
            .build();

    var ex =
        assertThrows(
            CryptoClientException.class,
            () -> cryptoClient.decrypt(encryptionKeys, "CannotDecodeThis!"));

    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DECODING_ERROR);
    verify(mockHybridEncryptionKeyService, never()).getEncrypter(any());
    verify(mockHybridEncryptionKeyService, never()).getDecrypter(any());
    verifyNoMoreInteractions(mockHybridEncryptionKeyService, mockHybridEncrypt, mockHybridDecrypt);
  }

  @Test
  public void decrypt_cannotDecodeBase64Dek_throws() throws Exception {
    when(mockHybridEncryptionKeyService.getDecrypter(any()))
        .thenThrow(IllegalArgumentException.class);
    DataRecordEncryptionKeys encryptionKeys =
        DataRecordEncryptionKeys.newBuilder()
            .setCoordinatorKey(CoordinatorKey.newBuilder().setKeyId("cannotDecodeBase64Dek"))
            .build();

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, "unused"));

    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DECRYPTION_ERROR);
    assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    verify(mockHybridEncryptionKeyService).getDecrypter(any());
    verifyNoMoreInteractions(mockHybridEncryptionKeyService, mockHybridEncrypt, mockHybridDecrypt);
  }
}
