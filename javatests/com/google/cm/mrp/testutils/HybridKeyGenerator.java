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

package com.google.cm.mrp.testutils;

import static com.google.cm.mrp.testutils.AeadKeyGenerator.getAeadFromJsonKeyset;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.crypto.tink.hybrid.HpkeParameters.AeadId.CHACHA20_POLY1305;
import static com.google.crypto.tink.hybrid.HpkeParameters.KdfId.HKDF_SHA256;
import static com.google.crypto.tink.hybrid.HpkeParameters.KemId.DHKEM_X25519_HKDF_SHA256;
import static com.google.crypto.tink.hybrid.HpkeParameters.Variant.NO_PREFIX;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.HybridEncrypt;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.TinkJsonProtoKeysetFormat;
import com.google.crypto.tink.hybrid.HpkeParameters;
import com.google.crypto.tink.hybrid.HybridConfig;
import java.security.GeneralSecurityException;

/** Generates Hybrids for testing */
public final class HybridKeyGenerator {
  private static final HybridEncrypt DEFAULT_HYBRID_ENCRYPT;
  private static final HybridDecrypt DEFAULT_HYBRID_DECRYPT;

  static {
    try {
      HybridConfig.register();
      KeysetHandle privateKeysetHandle = generateHybridKeyset();
      DEFAULT_HYBRID_ENCRYPT = getHybridEncrypt(privateKeysetHandle);
      DEFAULT_HYBRID_DECRYPT = getHybridDecrypt(privateKeysetHandle);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Error initializing tink.");
    }
  }

  /** Generates an Hybrid KeysetHandle */
  public static KeysetHandle generateHybridKeyset() throws GeneralSecurityException {
    return KeysetHandle.generateNew(
        HpkeParameters.builder()
            .setKemId(DHKEM_X25519_HKDF_SHA256)
            .setKdfId(HKDF_SHA256)
            .setAeadId(CHACHA20_POLY1305)
            .setVariant(NO_PREFIX)
            .build());
  }

  /** Generates a HybridEncrypt primitive from the private KeysetHandle */
  public static HybridEncrypt getHybridEncrypt(KeysetHandle keysetHandle) {
    try {
      return keysetHandle.getPublicKeysetHandle().getPrimitive(HybridEncrypt.class);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to create fake HybridDecrypt", e);
    }
  }

  /** Generates a HybridDecrypt primitive from the private KeysetHandle */
  public static HybridDecrypt getHybridDecrypt(KeysetHandle keysetHandle) {
    try {
      return keysetHandle.getPrimitive(HybridDecrypt.class);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to create fake HybridDecrypt", e);
    }
  }

  /** Gets the default HybridEncrypt primitive */
  public static HybridEncrypt getDefaultHybridEncrypt() {
    return DEFAULT_HYBRID_ENCRYPT;
  }

  /** Gets the default HybridDecrypt primitive */
  public static HybridDecrypt getDefaultHybridDecrypt() {
    return DEFAULT_HYBRID_DECRYPT;
  }

  /** Encrypts an unencoded string using an HybridEncrypt and returns the base64-encoded result. */
  public static String encryptString(HybridEncrypt encrypter, String plaintext) throws Exception {
    try {
      var ciphertext = encrypter.encrypt(plaintext.getBytes(), new byte[0]);
      return base64().encode(ciphertext);
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to encrypt using hybrid encrypter", e);
    }
  }

  /**
   * Decrypts a base64-encoded bytestring using an HybridDecrypt and returns the plaintext result.
   */
  public static String decryptString(HybridDecrypt decrypter, String ciphertext) throws Exception {
    try {
      var encryptedBytes = base64().decode(ciphertext);
      var decryptedBytes = decrypter.decrypt(encryptedBytes, new byte[0]);
      return new String(decryptedBytes, UTF_8);
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to decrypt using hybrid decrypter", e);
    }
  }

  /** Gets HybridEncrypt from cleartext JSON */
  public static HybridEncrypt getHybridEncryptFromJsonKeyset(
      String hybridKeyset, String aeadKeyset) {
    return getHybridEncryptFromJsonKeyset(hybridKeyset, getAeadFromJsonKeyset(aeadKeyset));
  }

  /** Gets HybridEncrypt from cleartext JSON */
  public static HybridEncrypt getHybridEncryptFromJsonKeyset(String hybridKeyset, Aead aead) {
    try {
      return TinkJsonProtoKeysetFormat.parseEncryptedKeyset(hybridKeyset, aead, new byte[0])
          .getPublicKeysetHandle()
          .getPrimitive(HybridEncrypt.class);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to get HybridEncrypt from JSON cleartext key", e);
    }
  }

  /** Gets HybridDecrypt from cleartext JSON */
  public static HybridDecrypt getHybridDecryptFromJsonKeyset(
      String hybridKeyset, String aeadKeyset) {
    return getHybridDecryptFromJsonKeyset(hybridKeyset, getAeadFromJsonKeyset(aeadKeyset));
  }

  /** Gets HybridDecrypt from cleartext JSON */
  public static HybridDecrypt getHybridDecryptFromJsonKeyset(String hybridKeyset, Aead aead) {
    try {
      return TinkJsonProtoKeysetFormat.parseEncryptedKeyset(hybridKeyset, aead, new byte[0])
          .getPrimitive(HybridDecrypt.class);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to get HybridDecrypt from JSON cleartext key", e);
    }
  }
}
