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

import static com.google.cm.util.ProtoUtils.getJsonFromProto;
import static com.google.cm.util.ProtoUtils.getProtoFromJson;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.crypto.tink.aead.XChaCha20Poly1305Parameters.Variant.TINK;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cm.mrp.backend.EncodingTypeProto.EncodingType;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.BinaryKeysetReader;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.TinkJsonProtoKeysetFormat;
import com.google.crypto.tink.TinkProtoKeysetFormat;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.XChaCha20Poly1305Parameters;
import com.google.crypto.tink.proto.EncryptedKeyset;
import com.google.protobuf.ByteString;
import com.google.scp.shared.crypto.tink.CloudAeadSelector;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.UUID;

/** Generates Aeads for testing */
public final class AeadKeyGenerator {
  private static final Aead DEFAULT_AEAD;

  static {
    try {
      AeadConfig.register();
      DEFAULT_AEAD = createAead();
    } catch (Exception e) {
      throw new RuntimeException("Error initializing tink.");
    }
  }

  /** Returns an AEAD selector with the default KEK */
  public static CloudAeadSelector getDefaultAeadSelector() {
    return unused -> DEFAULT_AEAD;
  }

  /** Generates an XChaChaKeyset DEK and encrypts it with the default KEK. */
  public static String generateEncryptedDek() throws GeneralSecurityException {
    return encryptDek(generateXChaChaKeyset());
  }

  /** Generates an XChaChaKeyset */
  public static KeysetHandle generateXChaChaKeyset() throws GeneralSecurityException {
    return KeysetHandle.generateNew(XChaCha20Poly1305Parameters.create(TINK));
  }

  /** Generates a DEK and returns its Aead primitive */
  public static Aead createAead() {
    try {
      return generateXChaChaKeyset().getPrimitive(Aead.class);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encrypts a DEK KeysetHandle with an Aead and returns encrypted Keyset */
  public static String encryptDek(KeysetHandle keysetHandle, Aead aead) {
    try {
      String json =
          TinkJsonProtoKeysetFormat.serializeEncryptedKeyset(keysetHandle, aead, new byte[0]);
      EncryptedKeyset keyset = getProtoFromJson(json, EncryptedKeyset.class);
      return base64().encode(keyset.getEncryptedKeyset().toByteArray());
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Failed to create fake DEK", e);
    }
  }

  /** Encrypts a DEK with the default KEK */
  public static String encryptDek(KeysetHandle keysetHandle) {
    return encryptDek(keysetHandle, DEFAULT_AEAD);
  }

  /** Encrypts a DEK KeysetHandle with the default KEK */
  public static String encryptKeyset(KeysetHandle keysetHandle) {
    try {
      byte[] ciphertext =
          TinkProtoKeysetFormat.serializeEncryptedKeyset(keysetHandle, DEFAULT_AEAD, new byte[0]);
      return base64().encode(ciphertext);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to create fake DEK", e);
    }
  }

  /** Generates a test KMS URI */
  public static String generateAeadUri() {
    return "gcp-kms://projects/" + UUID.randomUUID();
  }

  /**
   * Encrypts an unencoded string using a DEK (encrypted with the default KEK) and returns the
   * base64-encoded result.
   */
  public static String encryptStringWithBinaryDek(String dek, String plaintext) throws Exception {
    try {
      Aead aead =
          KeysetHandle.read(BinaryKeysetReader.withBytes(base64().decode(dek)), DEFAULT_AEAD)
              .getPrimitive(Aead.class);
      return base64().encode(aead.encrypt(plaintext.getBytes(), new byte[0]));
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to encrypt using dek", e);
    }
  }

  /**
   * Encrypts an unencoded string using an EncryptedKeyset (encrypted with the default KEK) and
   * returns the base64-encoded result.
   */
  public static String encryptString(String dek, String plaintext) throws Exception {
    return encryptString(dek, plaintext, EncodingType.BASE64);
  }

  /**
   * Encrypts an unencoded string using an EncryptedKeyset (encrypted with the default KEK) and
   * returns the encoded result in the given encodingType.
   */
  public static String encryptString(String dek, String plaintext, EncodingType encodingType)
      throws Exception {
    try {
      return encryptString(decryptEncryptedKeyset(DEFAULT_AEAD, dek), plaintext, encodingType);
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to encrypt using dek", e);
    }
  }

  /** Encrypts an unencoded string using an Aead and returns the base64-encoded result. */
  public static String encryptString(Aead aead, String plaintext) throws GeneralSecurityException {
    return encryptString(aead, plaintext, EncodingType.BASE64);
  }

  /**
   * Encrypts an unencoded string using an Aead and returns the encoded result in the given
   * encodingType.
   */
  public static String encryptString(Aead aead, String plaintext, EncodingType encodingType)
      throws GeneralSecurityException {
    try {
      switch (encodingType) {
        case BASE64:
          return base64().encode(aead.encrypt(plaintext.getBytes(), new byte[0]));
        case HEX:
          return base16().encode(aead.encrypt(plaintext.getBytes(), new byte[0]));
        default:
          throw new IllegalArgumentException(
              "Unsupported encoding type for encryption: " + encodingType);
      }
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to encrypt using dek", e);
    }
  }

  /**
   * Decrypts a base64-encoded bytestring using an encryptedDek in binary (encrypted with the
   * default KEK) and returns the plaintext result.
   */
  public static String decryptStringWithBinaryDek(String encryptedDek, String ciphertext)
      throws Exception {
    try {
      return decryptString(decryptEncryptedKeyset(DEFAULT_AEAD, encryptedDek), ciphertext);
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to decrypt using dek", e);
    }
  }

  /**
   * Decrypts a base64-encoded bytestring using an encryptedKeyset (encrypted with the default KEK)
   * and returns the plaintext result.
   */
  public static String decryptString(String encryptedDek, String ciphertext)
      throws GeneralSecurityException {
    return decryptString(encryptedDek, ciphertext, EncodingType.BASE64);
  }

  /**
   * Decrypts an encoded bytestring using an encryptedKeyset (encrypted with the default KEK) and
   * returns the plaintext result.
   */
  public static String decryptString(
      String encryptedDek, String ciphertext, EncodingType encodingType)
      throws GeneralSecurityException {
    try {
      return decryptString(
          decryptEncryptedKeyset(DEFAULT_AEAD, encryptedDek), ciphertext, encodingType);
    } catch (GeneralSecurityException | IOException e) {
      throw new GeneralSecurityException("Failed to decrypt using dek", e);
    }
  }

  /** Decrypts a base64-encoded bytestring using an Aead and returns the plaintext result. */
  public static String decryptString(Aead aead, String ciphertext) throws GeneralSecurityException {
    return decryptString(aead, ciphertext, EncodingType.BASE64);
  }

  /** Decrypts an encoded bytestring using an Aead and returns the plaintext result. */
  public static String decryptString(Aead aead, String ciphertext, EncodingType encodingType)
      throws GeneralSecurityException {
    try {
      switch (encodingType) {
        case BASE64:
          return new String(aead.decrypt(base64().decode(ciphertext), new byte[0]), UTF_8);
        case HEX:
          return new String(aead.decrypt(base16().decode(ciphertext), new byte[0]), UTF_8);
        default:
          throw new IllegalArgumentException(
              "Unsupported encoding type for encryption: " + encodingType);
      }
    } catch (GeneralSecurityException e) {
      throw new GeneralSecurityException("Failed to decrypt using dek", e);
    }
  }

  /** Gets KeysetHandle from cleartext JSON */
  public static KeysetHandle getKeysetFromJson(String json) {
    try {
      return CleartextKeysetHandle.read(JsonKeysetReader.withString(json));
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Failed to get Aead from JSON cleartext key", e);
    }
  }

  /** Gets Aead from cleartext JSON */
  public static Aead getAeadFromJsonKeyset(String json) {
    try {
      return getKeysetFromJson(json).getPrimitive(Aead.class);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to get Aead from JSON cleartext key", e);
    }
  }

  /** Decrypts an encrypted keyset (DEK) with a given KEK */
  public static Aead decryptEncryptedKeyset(Aead kek, String dek)
      throws IOException, GeneralSecurityException {
    var byteString = ByteString.copyFrom(base64().decode(dek));
    var proto = EncryptedKeyset.newBuilder().setEncryptedKeyset(byteString).build();
    String json = getJsonFromProto(proto);
    return TinkJsonProtoKeysetFormat.parseEncryptedKeyset(json, kek, new byte[0])
        .getPrimitive(Aead.class);
  }
}
