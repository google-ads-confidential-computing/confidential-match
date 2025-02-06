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

#ifndef CC_LOOKUP_SERVER_KMS_CLIENT_SRC_CACHED_KMS_CLIENT_H_
#define CC_LOOKUP_SERVER_KMS_CLIENT_SRC_CACHED_KMS_CLIENT_H_

#include <memory>
#include <string>

#include "cc/core/common/auto_expiry_concurrent_map/src/auto_expiry_concurrent_map.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "core/interface/async_context.h"

#include "cc/lookup_server/interface/kms_client_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A client that interfaces with Cloud KMS.
 *
 * This provides caching for recently decrypted payloads for improved
 * performance.
 */
class CachedKmsClient : public KmsClientInterface {
 public:
  /**
   * @brief Constructs a cached KMS client.
   *
   * @param async_executor the AsyncExecutor used for cache management tasks
   * @param kms_client the uncached Cloud KMS client to use for requests
   */
  explicit CachedKmsClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<KmsClientInterface> kms_client);

  scp::core::ExecutionResult Init() noexcept;
  scp::core::ExecutionResult Run() noexcept;
  scp::core::ExecutionResult Stop() noexcept;

  /**
   * @brief Decrypts a payload using Cloud KMS asynchronously.
   *
   * If the request has been successfuly decrypted recently, the cached
   * response is returned, skipping the call to Cloud KMS.
   *
   * @param decrypt_context the context containing the request and callback
   * @return a SuccessExecutionResult if the operation was started successfully
   */
  scp::core::ExecutionResult Decrypt(
      scp::core::AsyncContext<DecryptRequest, std::string>
          decrypt_context) noexcept;

  /**
   * @brief Decrypts a payload using Cloud KMS synchronously.
   *
   * If the request has been successfuly decrypted recently, the cached
   * response is returned, skipping the call to Cloud KMS.
   *
   * @param decrypt_request the payload to be decrypted
   * @return the decrypted payload, or a FailureExecutionResult on error
   */
  scp::core::ExecutionResultOr<std::string> Decrypt(
      const DecryptRequest& decrypt_request) noexcept;

 private:
  /**
   * @brief Helper to process a callback after calling Cloud KMS to decrypt.
   *
   * @param kms_decrypt_context the context containing the results from calling
   * Cloud KMS
   * @param output_context the original caller context to write the decryption
   * response to
   * @return whether or not the decryption response was found.
   */
  void HandleKmsDecryptionCallback(
      scp::core::AsyncContext<DecryptRequest, std::string> kms_decrypt_context,
      scp::core::AsyncContext<DecryptRequest, std::string>
          output_context) noexcept;

  /**
   * @brief Queries whether the cache has a decryption response available, and
   * returns it if so. Returns KMS_CLIENT_CACHE_ENTRY_NOT_FOUND if not found.
   *
   * @param cache_key the cache key corresponding to a DecryptRequest object
   * @return whether or not the decryption response was found.
   */
  scp::core::ExecutionResultOr<std::string> GetFromCache(
      const std::string& cache_key) noexcept;

  /**
   * @brief Inserts a decryption response into the cache, keyed by the request.
   *
   * If a response already exists for a request, returns FailureExecutionResult
   * indicating that the entry already exists.
   *
   * If the cache entry is currently being evicted, a RetryExecutionResult is
   * returned.
   *
   * @param cache_key the cache key corresponding to a DecryptRequest object
   * @param decrypt_response the response to be cached
   * @return the response stored in the cache, or an error on failure
   */
  scp::core::ExecutionResult InsertToCache(
      const std::string& cache_key,
      const std::string& decrypt_response) noexcept;

  // The AsyncExecutor used by the concurrent map.
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  // The uncached Cloud KMS client.
  std::shared_ptr<KmsClientInterface> kms_client_;
  // The cache for decrypted payloads.
  scp::core::common::AutoExpiryConcurrentMap<std::string, std::string> cache_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_KMS_CLIENT_SRC_CACHED_KMS_CLIENT_H_
