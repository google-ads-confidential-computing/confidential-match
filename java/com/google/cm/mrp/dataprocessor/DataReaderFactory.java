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

package com.google.cm.mrp.dataprocessor;

import com.google.cm.mrp.backend.EncryptionMetadataProto.EncryptionMetadata;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.EncryptionKeyColumns;
import com.google.cm.mrp.backend.MatchConfigProto.MatchConfig.SuccessConfig.SuccessMode;
import com.google.cm.mrp.backend.SchemaProto.Schema;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.cm.mrp.dataprocessor.readers.CsvDataReader;
import com.google.cm.mrp.dataprocessor.readers.DataReader;
import com.google.cm.mrp.dataprocessor.readers.SerializedProtoDataReader;
import java.io.InputStream;
import javax.inject.Named;

/** Factory interface for {@link DataReader}. */
public interface DataReaderFactory {
  /** Factory method for constructing {@link CsvDataReader} objects. */
  @Named("csvWithoutEncryption")
  DataReader createCsvDataReader(
      InputStream inputStream, Schema schema, String name, SuccessMode successMode);

  /** Factory method for constructing {@link CsvDataReader} objects with encryption. */
  @Named("csvWithEncryption")
  DataReader createCsvDataReader(
      InputStream inputStream,
      Schema schema,
      String name,
      EncryptionKeyColumns encryptionKeyColumns,
      SuccessMode successMode,
      EncryptionMetadata encryptionMetadata,
      CryptoClient cryptoClient);

  /** Factory method for constructing {@link SerializedProtoDataReader} objects. */
  @Named("serializedProtoWithoutEncryption")
  DataReader createProtoDataReader(
      InputStream inputStream,
      Schema schema,
      String name,
      MatchConfig matchConfig,
      SuccessMode successMode);

  /** Factory method for constructing {@link SerializedProtoDataReader} objects with encryption. */
  @Named("serializedProtoWithEncryption")
  DataReader createProtoDataReader(
      InputStream inputStream,
      Schema schema,
      String name,
      MatchConfig matchConfig,
      SuccessMode successMode,
      EncryptionMetadata encryptionMetadata,
      CryptoClient cryptoClient);
}
