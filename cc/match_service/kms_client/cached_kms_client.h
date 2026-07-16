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

#ifndef CC_MATCH_SERVICE_KMS_CLIENT_CACHED_KMS_CLIENT_H_
#define CC_MATCH_SERVICE_KMS_CLIENT_CACHED_KMS_CLIENT_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cc/core/common/auto_expiry_concurrent_map/src/auto_expiry_concurrent_map.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/kms_client/kms_client_interface.h"

namespace google::confidential_match::match_service {

// A client that interfaces with Cloud KMS. This provides caching for recently
// decrypted payloads for improved performance.
class CachedKmsClient : public KmsClientInterface {
 public:
  // Constructs a cached KMS client.
  // Does not take ownership of the KMS client, and all pointers must refer to
  // valid objects that outlive the CachedKmsClient object.
  explicit CachedKmsClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      KmsClientInterface* kms_client);

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Decrypts a payload using Cloud KMS asynchronously.
  //
  // If the request has been successfully decrypted recently, the cached
  // response is returned, skipping the call to Cloud KMS.
  void Decrypt(google::confidential_match::AsyncContext<
               google::cmrt::sdk::kms_service::v1::DecryptRequest, std::string>
                   decrypt_context) noexcept override;

 private:
  // Helper to process a callback after calling Cloud KMS to decrypt.
  void HandleKmsDecryptionCallback(
      google::confidential_match::AsyncContext<
          google::cmrt::sdk::kms_service::v1::DecryptRequest, std::string>
          output_context,
      google::confidential_match::AsyncContext<
          google::cmrt::sdk::kms_service::v1::DecryptRequest, std::string>
          kms_decrypt_context) noexcept;

  // Queries whether the cache has a decryption response available.
  absl::StatusOr<std::string> GetFromCache(
      const std::string& cache_key) noexcept;

  // Inserts a decryption response into the cache, keyed by the request.
  absl::Status InsertToCache(const std::string& cache_key,
                             const std::string& decrypt_response) noexcept;

  // The AsyncExecutor used by the concurrent map.
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  // The uncached Cloud KMS client.
  KmsClientInterface* kms_client_;
  // The cache for decrypted payloads.
  scp::core::common::AutoExpiryConcurrentMap<std::string, std::string> cache_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_KMS_CLIENT_CACHED_KMS_CLIENT_H_
