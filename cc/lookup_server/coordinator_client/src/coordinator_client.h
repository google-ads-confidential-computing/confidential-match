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

#ifndef CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_COORDINATOR_CLIENT_H_
#define CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_COORDINATOR_CLIENT_H_

#include <memory>

#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/private_key_client/private_key_client_interface.h"
#include "cc/public/cpio/proto/private_key_service/v1/private_key_service.pb.h"

#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A client used to fetch keys hosted on one or more coordinators.
 */
class CoordinatorClient : public CoordinatorClientInterface {
 public:
  /**
   * @brief Constructs a coordinator client.
   *
   * @param private_key_client A CPIO private key client.
   */
  explicit CoordinatorClient(
      std::shared_ptr<scp::cpio::PrivateKeyClientInterface> private_key_client);

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  /**
   * @brief Fetches a hybrid public/private keypair asynchronously.
   *
   * @param key_context the context containing info for the key to be fetched
   */
  void GetHybridKey(scp::core::AsyncContext<
                    lookup_server::proto_backend::GetHybridKeyRequest,
                    lookup_server::proto_backend::GetHybridKeyResponse>
                        key_context) noexcept override;

  /**
   * @brief Fetches a hybrid public/private keypair synchronously.
   *
   * @param request the request containing information about the key to fetch
   * @return the hybrid key, or a failure result on error
   */
  scp::core::ExecutionResultOr<
      lookup_server::proto_backend::GetHybridKeyResponse>
  GetHybridKey(lookup_server::proto_backend::GetHybridKeyRequest
                   request) noexcept override;

 private:
  /**
   * @brief Handles a callback from the CPIO list private keys operation.
   *
   * @param cpio_list_context contains results from the CPIO list operation
   * @param hybrid_key_context the caller context to write the response to
   */
  void HandleListPrivateKeysCallback(
      scp::core::AsyncContext<
          cmrt::sdk::private_key_service::v1::ListPrivateKeysRequest,
          cmrt::sdk::private_key_service::v1::ListPrivateKeysResponse>&
          cpio_list_context,
      scp::core::AsyncContext<
          lookup_server::proto_backend::GetHybridKeyRequest,
          lookup_server::proto_backend::GetHybridKeyResponse>
          hybrid_key_context) noexcept;

  std::shared_ptr<scp::cpio::PrivateKeyClientInterface> private_key_client_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_COORDINATOR_CLIENT_H_
