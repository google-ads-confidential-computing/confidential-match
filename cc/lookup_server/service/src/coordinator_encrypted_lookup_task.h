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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_COORDINATOR_ENCRYPTED_LOOKUP_TASK_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_COORDINATOR_ENCRYPTED_LOOKUP_TASK_H_

#include <memory>

#include "cc/core/interface/async_context.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Handles a lookup request with coordinator-encrypted and hashed keys.
 */
class CoordinatorEncryptedLookupTask {
 public:
  explicit CoordinatorEncryptedLookupTask(
      std::shared_ptr<MatchDataStorageInterface> match_data_storage,
      std::shared_ptr<CryptoClientInterface> hpke_crypto_client)
      : match_data_storage_(match_data_storage),
        hpke_crypto_client_(hpke_crypto_client) {}

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
   * @brief Handles a callback for lookup requests after the hybrid crypto key
   * has been fetched.
   *
   * @param get_crypto_key_context the context containing the crypto key
   * @param lookup_context the context to write the lookup response to
   */
  void OnGetCryptoKeyCallback(
      const scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                                    CryptoKeyInterface>& get_crypto_key_context,
      scp::core::AsyncContext<proto_api::LookupRequest,
                              proto_api::LookupResponse>
          lookup_context) noexcept;

  /**
   * @brief Performs matching for the keys in a lookup request.
   *
   * @param request the request to be matched
   * @param crypto_key the key used to decrypt the request payload
   * @return the lookup response, or an error if unsuccessful
   */
  scp::core::ExecutionResultOr<proto_api::LookupResponse> Match(
      const proto_api::LookupRequest& request,
      const CryptoKeyInterface& crypto_key) noexcept;

  /**
   * @brief Performs matching for the keys in a lookup request, in which each
   * hash is individually encrypted.
   *
   * @param request the request to be matched
   * @param crypto_key the key used to decrypt the request payload
   * @return the lookup response, or an error if unsuccessful
   */
  scp::core::ExecutionResultOr<proto_api::LookupResponse>
  MatchIndividuallyEncryptedKeys(const proto_api::LookupRequest& request,
                                 const CryptoKeyInterface& crypto_key) noexcept;

  // A storage service containing match data.
  std::shared_ptr<MatchDataStorageInterface> match_data_storage_;
  // An instance of the hybrid public-key encryption crypto client.
  std::shared_ptr<CryptoClientInterface> hpke_crypto_client_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_COORDINATOR_ENCRYPTED_LOOKUP_TASK_H_
