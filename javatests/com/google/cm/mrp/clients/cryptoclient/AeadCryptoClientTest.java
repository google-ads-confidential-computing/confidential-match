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

import static com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.KeyType.XCHACHA20_POLY1305;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.decryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.encryptDek;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.encryptString;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateAeadUri;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.generateXChaChaKeyset;
import static com.google.cm.mrp.testutils.AeadKeyGenerator.getDefaultAeadSelector;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys;
import com.google.cm.mrp.backend.DataRecordEncryptionFieldsProto.DataRecordEncryptionKeys.WrappedEncryptionKeys.GcpWrappedKeys;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.EncryptionKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo;
import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata.WrappedKeyInfo.GcpWrappedKeyInfo;
import com.google.cm.mrp.backend.JobResultCodeProto.JobResultCode;
import com.google.cm.mrp.clients.cryptoclient.AeadProvider.AeadProviderException;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient.CryptoClientException;
import com.google.cm.mrp.clients.cryptoclient.models.AeadProviderParameters;
import com.google.cm.mrp.testutils.fakes.OAuthException;
import com.google.crypto.tink.Aead;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.security.GeneralSecurityException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class AeadCryptoClientTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  private static final String TEST_WIP = "testWip";
  private static final EncryptionKeyInfo TEST_KEY_INFO =
      EncryptionKeyInfo.newBuilder()
          .setWrappedKeyInfo(
              WrappedKeyInfo.newBuilder()
                  .setKeyType(XCHACHA20_POLY1305)
                  .setGcpWrappedKeyInfo(GcpWrappedKeyInfo.newBuilder().setWipProvider(TEST_WIP)))
          .build();
  private static final AeadProviderParameters TEST_PARAMETERS =
      AeadProviderParameters.forWipProvider(TEST_WIP);

  @Mock private Aead mockAead;
  @Mock private CloudAeadSelector mockAeadSelector;

  @Mock private AeadProvider mockAeadProvider;

  @Test
  public void new_unidentifiedKeyTypeThrows() throws Exception {
    var encryptionKeyInfo =
        EncryptionKeyInfo.newBuilder()
            .setWrappedKeyInfo(WrappedKeyInfo.getDefaultInstance())
            .build();
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    var ex =
        assertThrows(
            CryptoClientException.class,
            () -> new AeadCryptoClient(mockAeadProvider, encryptionKeyInfo));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.UNSUPPORTED_DEK_KEY_TYPE);
    assertThat(ex.getMessage()).endsWith("Unsupported key type: UNSPECIFIED");
  }

  @Test
  public void encrypt_withDataRecordEncryptionKeysSuccess() throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String plaintext = "TestString";

    String encrypted = cryptoClient.encrypt(encryptionKeys, plaintext);

    assertThat(decryptString(encryptedDek, encrypted)).isEqualTo(plaintext);
  }

  @Test
  public void encrypt_withDataRecordEncryptionsKey_formatPrefixAndWhitespace_Success()
      throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    String testKek = " " + generateAeadUri().substring(10); // gcp-kms:// is 10 chars
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, " " + encryptedDek + " ");
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String plaintext = "TestString";

    String encrypted = cryptoClient.encrypt(encryptionKeys, plaintext);

    assertThat(decryptString(encryptedDek, encrypted)).isEqualTo(plaintext);
  }

  @Test
  public void decrypt_withDataRecordEncryptionKeysSuccess() throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);

    assertThat(cryptoClient.decrypt(encryptionKeys, encrypted)).isEqualTo(plaintext);
  }

  @Test
  public void decrypt_withDataRecordEncryptionKey_formatPrefixAndWhitespace_Success()
      throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    String testKek = " " + generateAeadUri().substring(10); // gcp-kms:// is 10 chars
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, " " + encryptedDek + " ");
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);

    assertThat(cryptoClient.decrypt(encryptionKeys, encrypted)).isEqualTo(plaintext);
  }

  @Test
  public void decrypt_whenDefaultWipMissing_useDataRecordEncryptionWip() throws Exception {
    String rowLevelWip = "rowWip";
    var encryptionKeyInfo =
        EncryptionKeyInfo.newBuilder()
            .setWrappedKeyInfo(
                WrappedKeyInfo.newBuilder()
                    .setKeyType(XCHACHA20_POLY1305)
                    .setGcpWrappedKeyInfo(GcpWrappedKeyInfo.getDefaultInstance()))
            .build();
    var testParams = AeadProviderParameters.forWipProvider(rowLevelWip);
    when(mockAeadProvider.getAeadSelector(eq(testParams))).thenReturn(getDefaultAeadSelector());
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek, rowLevelWip);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, encryptionKeyInfo);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);

    assertThat(cryptoClient.decrypt(encryptionKeys, encrypted)).isEqualTo(plaintext);
  }

  @Test
  public void decrypt_whenNoCloudWrappedKeys_throws() throws Exception {
    var encryptionKeyInfo =
        EncryptionKeyInfo.newBuilder()
            .setWrappedKeyInfo(WrappedKeyInfo.newBuilder().setKeyType(XCHACHA20_POLY1305))
            .build();
    when(mockAeadProvider.getAeadSelector(any())).thenReturn(getDefaultAeadSelector());
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, encryptionKeyInfo);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.CRYPTO_CLIENT_CONFIGURATION_ERROR);
  }

  @Test
  public void decrypt_aeadProviderException_failure() throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenThrow(AeadProviderException.class);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DEK_DECRYPTION_ERROR);
  }

  @Test
  public void encrypt_failureThrowsException() throws Exception {
    when(mockAead.decrypt(any(), any())).thenThrow(GeneralSecurityException.class);
    when(mockAeadSelector.getAead(any())).thenReturn(mockAead);
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(mockAeadSelector);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    assertThrows(
        CryptoClientException.class, () -> cryptoClient.encrypt(encryptionKeys, "TestString"));
  }

  @Test
  public void decrypt_failureThrowsException() throws Exception {
    when(mockAead.decrypt(any(), any())).thenThrow(GeneralSecurityException.class);
    when(mockAeadSelector.getAead(any())).thenReturn(mockAead);
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(mockAeadSelector);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    assertThrows(
        CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
  }

  @Test
  public void decrypt_cannotDecodeBase64_throws() throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String encryptedDek = encryptDek(generateXChaChaKeyset());
    var encryptionKeys = getDataRecordEncryptionKeys(generateAeadUri(), encryptedDek);
    String badEncrypted = encryptString(encryptedDek, "TestString") + "abcd!";

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, badEncrypted));

    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DECODING_ERROR);
    verifyNoMoreInteractions(mockAead, mockAeadSelector);
  }

  @Test
  public void decryptDek_generalFailureThrowsException() throws Exception {
    when(mockAead.decrypt(any(), any())).thenThrow(GeneralSecurityException.class);
    when(mockAeadSelector.getAead(any())).thenReturn(mockAead);
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(mockAeadSelector);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DEK_DECRYPTION_ERROR);
  }

  @Test
  public void decryptDek_failureForSameKek_ThrowsException() throws Exception {
    when(mockAead.decrypt(any(), any())).thenThrow(GeneralSecurityException.class);
    when(mockAeadSelector.getAead(any())).thenReturn(mockAead);
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(mockAeadSelector);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DEK_DECRYPTION_ERROR);
    assertThrows(
        CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DEK_DECRYPTION_ERROR);
    verify(mockAeadSelector, times(1)).getAead(any());
  }

  @Test
  public void decryptDek_InvalidWipThrowsException() throws Exception {
    when(mockAead.decrypt(any(), any()))
        .thenThrow(new GeneralSecurityException(new OAuthException("invalid_target")));
    when(mockAeadSelector.getAead(any())).thenReturn(mockAead);
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(mockAeadSelector);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.INVALID_WIP_PARAMETER);
  }

  @Test
  public void decryptDek_WipConditionFailedThrowsException() throws Exception {
    when(mockAead.decrypt(any(), any()))
        .thenThrow(new GeneralSecurityException(new OAuthException("unauthorized_client")));
    when(mockAeadSelector.getAead(any())).thenReturn(mockAead);
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(mockAeadSelector);
    String testKek = generateAeadUri();
    var keyset = generateXChaChaKeyset();
    var encryptedDek = encryptDek(keyset);
    var encryptionKeys = getDataRecordEncryptionKeys(testKek, encryptedDek);
    String plaintext = "TestString";
    String encrypted = encryptString(encryptedDek, plaintext);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, encrypted));
    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.WIP_AUTH_FAILED);
  }

  @Test
  public void decryptDek_unsupportedKeyTypeThrows() throws Exception {
    var keysetProto = com.google.crypto.tink.proto.Keyset.newBuilder();
    keysetProto.addKeyBuilder().getKeyDataBuilder().setTypeUrl("unsupported");
    when(mockAead.decrypt(any(), any())).thenReturn(keysetProto.build().toByteArray());
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS))).thenReturn(unused -> mockAead);
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);

    var ex =
        assertThrows(
            CryptoClientException.class,
            () -> cryptoClient.decrypt(getDataRecordEncryptionKeys("unused", "unused"), "unused"));

    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DEK_KEY_TYPE_MISMATCH);
  }

  @Test
  public void decryptDek_cannotDecodeBase64_throws() throws Exception {
    when(mockAeadProvider.getAeadSelector(eq(TEST_PARAMETERS)))
        .thenReturn(getDefaultAeadSelector());
    var cryptoClient = new AeadCryptoClient(mockAeadProvider, TEST_KEY_INFO);
    String encryptedDek = encryptDek(generateXChaChaKeyset());
    var encryptionKeys = getDataRecordEncryptionKeys(generateAeadUri(), encryptedDek + "!abcd");

    var ex =
        assertThrows(
            CryptoClientException.class, () -> cryptoClient.decrypt(encryptionKeys, "unused"));

    assertThat(ex.getErrorCode()).isEqualTo(JobResultCode.DECODING_ERROR);
    verifyNoMoreInteractions(mockAead, mockAeadSelector);
  }

  private DataRecordEncryptionKeys getDataRecordEncryptionKeys(String kek, String dek) {
    return getDataRecordEncryptionKeys(kek, dek, /* wip= */ "");
  }

  private DataRecordEncryptionKeys getDataRecordEncryptionKeys(String kek, String dek, String wip) {
    var wrappedKeysBuilder =
        DataRecordEncryptionKeys.WrappedEncryptionKeys.newBuilder()
            .setKekUri(kek)
            .setEncryptedDek(dek);
    if (!wip.isBlank()) {
      wrappedKeysBuilder.setGcpWrappedKeys(GcpWrappedKeys.newBuilder().setWipProvider(wip));
    }
    return DataRecordEncryptionKeys.newBuilder()
        .setWrappedEncryptionKeys(wrappedKeysBuilder.build())
        .build();
  }
}
