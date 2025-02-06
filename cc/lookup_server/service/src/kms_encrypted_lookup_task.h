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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_KMS_ENCRYPTED_LOOKUP_TASK_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_KMS_ENCRYPTED_LOOKUP_TASK_H_

#include <memory>
#include <string>

#include "cc/core/interface/async_context.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/kms_client_interface.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "protos/lookup_server/api/lookup.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Handles a lookup request encrypted with KMS wrapped keys.
 */
class KmsEncryptedLookupTask {
 public:
  explicit KmsEncryptedLookupTask(
      std::shared_ptr<MatchDataStorageInterface> match_data_storage,
      std::shared_ptr<CryptoClientInterface> aead_crypto_client)
      : match_data_storage_(match_data_storage),
        aead_crypto_client_(aead_crypto_client) {}

  /**
   * Handles a lookup request, producing a lookup response.
   *
   * @param context the AsyncContext containing the LookupRequest object and
   * the resulting LookupResponse if successful, or a failed result
   */
  void HandleRequest(scp::core::AsyncContext<proto_api::LookupRequest,
                                             proto_api::LookupResponse>
                         context) noexcept;

 private:
  /**
   * @brief Handles a callback for KMS-encrypted requests after the wrapped
   * key has been decrypted.
   *
   * @param decrypt_context the context containing the decrypted key
   * @param lookup_context the context to write the lookup response to
   */
  void OnGetCryptoKeyCallback(
      scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                              CryptoKeyInterface>
          decrypt_context,
      scp::core::AsyncContext<proto_api::LookupRequest,
                              proto_api::LookupResponse>
          lookup_context) noexcept;

  // A storage service containing match data.
  std::shared_ptr<MatchDataStorageInterface> match_data_storage_;
  // A client used for AEAD cryptographic operations.
  std::shared_ptr<CryptoClientInterface> aead_crypto_client_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_KMS_ENCRYPTED_LOOKUP_TASK_H_
