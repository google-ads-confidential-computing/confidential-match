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

package com.google.cm.mrp.clients.lookupserviceclient.model;

import com.google.auto.value.AutoValue;
import com.google.cm.lookupserver.api.LookupProto.EncryptionKeyInfo;
import com.google.cm.lookupserver.api.LookupProto.LookupRequest.KeyFormat;
import com.google.cm.mrp.backend.LookupDataRecordProto.LookupDataRecord;
import com.google.cm.mrp.clients.cryptoclient.CryptoClient;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

/** A request for the lookup service client. */
@AutoValue
public abstract class LookupServiceClientRequest {

  /** Returns a new builder. */
  public static Builder builder() {
    return new AutoValue_LookupServiceClientRequest.Builder();
  }

  /** Records to be matched. */
  public abstract ImmutableList<LookupDataRecord> records();

  /** Format of the lookup keys. */
  public abstract KeyFormat keyFormat();

  /** The associated data fields to be retrieved from Lookup Service. */
  public abstract ImmutableList<String> associatedDataKeys();

  /** Metadata required for encrypted lookup keys. */
  public abstract Optional<EncryptionKeyInfo> encryptionKeyInfo();

  /** CryptoClient used for batch encryption. If present, batch encryption will be used. */
  public abstract Optional<CryptoClient> cryptoClient();

  /** Builder for {@link LookupServiceClientRequest}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Create a new {@link LookupServiceClientRequest} from the builder. */
    public abstract LookupServiceClientRequest build();

    /** Set the list of data records. */
    public abstract Builder setRecords(List<LookupDataRecord> records);

    /** Builder for the list of data records. */
    protected abstract ImmutableList.Builder<LookupDataRecord> recordsBuilder();

    /** Add a data record. */
    public Builder addRecord(LookupDataRecord record) {
      recordsBuilder().add(record);
      return this;
    }

    /** Add a data record. */
    public Builder addRecord(LookupDataRecord.Builder record) {
      return addRecord(record.build());
    }

    /** Set the key format. */
    public abstract Builder setKeyFormat(KeyFormat keyFormat);

    /** Set the associated data fields to be retrieved from Lookup Service. */
    public abstract Builder setAssociatedDataKeys(List<String> associatedDataKeys);

    /** Builder for the associated data fields to be retrieved from Lookup Service. */
    protected abstract ImmutableList.Builder<String> associatedDataKeysBuilder();

    /** Add an associated data key. */
    public Builder addAssociatedDataKey(String associatedDataKey) {
      associatedDataKeysBuilder().add(associatedDataKey);
      return this;
    }

    /** Set the key metadata. */
    public abstract Builder setEncryptionKeyInfo(EncryptionKeyInfo encryptionKeyInfo);

    /** Set the crypto client to use for batch encryption. */
    public abstract Builder setCryptoClient(CryptoClient cryptoClient);
  }
}
