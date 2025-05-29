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

package com.google.cm.mrp.clients.cryptoclient.models;

import com.google.crypto.tink.KeysetReader;
import com.google.crypto.tink.proto.EncryptedKeyset;
import com.google.protobuf.ByteString;

/** Reader to allow parsing an encrypted keyset handle without unnecessary serialization steps */
public class DekKeysetReader implements KeysetReader {

  private final ByteString dek;

  public DekKeysetReader(ByteString dek) {
    this.dek = dek;
  }

  @Override
  public com.google.crypto.tink.proto.Keyset read() {
    throw new UnsupportedOperationException();
  }

  @Override
  public EncryptedKeyset readEncrypted() {
    return EncryptedKeyset.newBuilder().setEncryptedKeyset(dek).build();
  }
}
