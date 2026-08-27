/*
 * Copyright 2026 Google LLC
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

#ifndef CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_CPIO_CACHED_COORDINATOR_CLIENT_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_CPIO_CACHED_COORDINATOR_CLIENT_H_  // NOLINT(whitespace/line_length)

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/proto/private_key_service/v1/private_key_service.pb.h"
#include "cc/public/cpio/utils/key_fetching/interface/key_fetcher_with_cache_interface.h"
#include "cc/public/cpio/utils/key_fetching/proto/key_coordinator_configuration.pb.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {

// The following functions are used to stringify the coordinators from various
// protos to use as keys in the key fetcher map and should all produce
// equivalent results for equivalent inputs.

/**
 * @brief Stringifies the endpoints from a KeyCoordinatorConfiguration proto by
 * sorting the endpoints and concatenating their string representations.
 */
std::string StringifyEndpoints(
    const google::cmrt::sdk::v1::KeyCoordinatorConfiguration&
        key_coordinator_configuration);

/**
 * @brief Stringifies the coordinators from a GetHybridKeyRequest by converting
 * each coordinator into an equivalent endpoint representation, sorting by
 * endpoint, and concatenating their string representations.
 */
std::string StringifyCoordinators(
    const google::protobuf::RepeatedPtrField<
        proto_backend::GetHybridKeyRequest::Coordinator>& coordinators);

/**
 * @brief Stringifies the coordinators from an EncryptionKeyInfo by converting
 * each coordinator into an equivalent endpoint representation, sorting by
 * endpoint, and concatenating their string representations.
 */
std::string StringifyCoordinators(
    const google::protobuf::RepeatedPtrField<
        proto_api::EncryptionKeyInfo::CoordinatorInfo>& coordinators);

/**
 * @brief A client used to fetch keys hosted on one or more coordinators.
 *
 * This caches the response from a coordinator in memory for a small duration,
 * improving performance across multiple requests using the same coordinator.
 */
class CpioCachedCoordinatorClient : public CoordinatorClientInterface {
 public:
  /**
   * @brief Constructs a coordinator client with a map of serialized Coordinator
   * proto strings to their corresponding key fetchers.
   *
   * @param async_executor the AsyncExecutor used to schedule async operations
   * @param key_fetcher_map map from serialized Coordinator proto string to key
   * fetcher
   */
  explicit CpioCachedCoordinatorClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      absl::flat_hash_map<
          std::string, std::shared_ptr<scp::cpio::KeyFetcherWithCacheInterface>>
          key_fetcher_map);

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
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  absl::flat_hash_map<std::string,
                      std::shared_ptr<scp::cpio::KeyFetcherWithCacheInterface>>
      key_fetcher_map_;
};

}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_CPIO_CACHED_COORDINATOR_CLIENT_H_
