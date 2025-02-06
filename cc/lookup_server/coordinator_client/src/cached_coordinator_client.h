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

#ifndef CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_CACHED_COORDINATOR_CLIENT_H_
#define CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_CACHED_COORDINATOR_CLIENT_H_

#include <memory>
#include <string>

#include "cc/core/common/auto_expiry_concurrent_map/src/auto_expiry_concurrent_map.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A client used to fetch keys hosted on one or more coordinators.
 *
 * This caches the response from a coordinator in memory for a small duration,
 * improving performance across multiple requests using the same coordinator.
 */
class CachedCoordinatorClient : public CoordinatorClientInterface {
 public:
  /**
   * @brief Constructs a coordinator client.
   *
   * @param async_executor the AsyncExecutor used for cache management tasks
   * @param coordinator_client the uncached coordinator client used for fetches
   */
  explicit CachedCoordinatorClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<CoordinatorClientInterface> coordinator_client);

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
   * @brief Helper to process a callback after fetching a hybrid key.
   *
   * @param coordinator_fetch_context the context containing the results from
   * fetching the coordinator key
   * @param output_context the original caller context to write the response
   * to
   * @return whether or not the decryption response was found.
   */
  void HandleGetHybridKeyCallback(
      scp::core::AsyncContext<proto_backend::GetHybridKeyRequest,
                              proto_backend::GetHybridKeyResponse>
          coordinator_fetch_context,
      scp::core::AsyncContext<proto_backend::GetHybridKeyRequest,
                              proto_backend::GetHybridKeyResponse>
          output_context) noexcept;

  /**
   * @brief Queries whether the cache has a coordinator response available, and
   * returns it if so. Returns COORDINATOR_CLIENT_CACHE_ENTRY_NOT_FOUND if not
   * found.
   *
   * @param request the request to fetch a hybrid key
   * @return the serialized response or a FailureExecutionResult
   */
  scp::core::ExecutionResultOr<proto_backend::GetHybridKeyResponse>
  GetFromCache(const proto_backend::GetHybridKeyRequest& request) noexcept;

  /**
   * @brief Inserts a coordinator response into the cache, keyed by the
   * request.
   *
   * If a response already exists for a request, returns FailureExecutionResult
   * indicating that the entry already exists.
   *
   * If the cache entry is currently being evicted, a RetryExecutionResult is
   * returned.
   *
   * @param request the request object to use as a cache key
   * @param response the response to be cached
   * @return whether or not the object was stored in the cache
   */
  scp::core::ExecutionResult InsertToCache(
      const proto_backend::GetHybridKeyRequest& request,
      const proto_backend::GetHybridKeyResponse& response) noexcept;

  // The AsyncExecutor used by the concurrent map.
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  // The uncached coordinator client.
  std::shared_ptr<CoordinatorClientInterface> coordinator_client_;
  // The cache containing successful responses for coordinator key fetches.
  scp::core::common::AutoExpiryConcurrentMap<std::string, std::string> cache_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_CACHED_COORDINATOR_CLIENT_H_
