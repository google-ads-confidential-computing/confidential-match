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

#ifndef CC_LOOKUP_SERVER_INTERFACE_COORDINATOR_CLIENT_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_COORDINATOR_CLIENT_INTERFACE_H_

#include <string>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface for a client that fetches keys from a coordinator. */
class CoordinatorClientInterface : public scp::core::ServiceInterface {
 public:
  virtual ~CoordinatorClientInterface() = default;

  /**
   * @brief Fetches a hybrid public/private keypair asynchronously.
   *
   * @param key_context the context containing info for the key to be fetched
   */
  virtual void GetHybridKey(scp::core::AsyncContext<
                            lookup_server::proto_backend::GetHybridKeyRequest,
                            lookup_server::proto_backend::GetHybridKeyResponse>
                                key_context) noexcept = 0;

  /**
   * @brief Fetches a hybrid public/private keypair synchronously.
   *
   * @param request the request containing information about the key to fetch
   * @return the hybrid key, or a failure result on error
   */
  virtual scp::core::ExecutionResultOr<
      lookup_server::proto_backend::GetHybridKeyResponse>
  GetHybridKey(
      lookup_server::proto_backend::GetHybridKeyRequest request) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_COORDINATOR_CLIENT_INTERFACE_H_
