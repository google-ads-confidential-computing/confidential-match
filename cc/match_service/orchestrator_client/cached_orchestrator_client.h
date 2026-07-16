// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_CACHED_ORCHESTRATOR_CLIENT_H_
#define CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_CACHED_ORCHESTRATOR_CLIENT_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "cc/core/async/async_context.h"
#include "cc/core/common/auto_expiry_concurrent_map/src/auto_expiry_concurrent_map.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/match_service/orchestrator_client/orchestrator_client_interface.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {

// Client that interfaces with the Orchestrator and caches responses for reuse.
class CachedOrchestratorClient : public OrchestratorClientInterface {
 public:
  explicit CachedOrchestratorClient(
      std::shared_ptr<google::scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<OrchestratorClientInterface> orchestrator_client);

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Gets the current sharding scheme from Orchestrator asynchronously.
  void GetCurrentShardingScheme(
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          cache_context) noexcept override;

 private:
  // Helper to process a callback after fetching a sharding scheme
  void OnGetCurrentShardingSchemeCallback(
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          cache_context,
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          fetch_context) noexcept;

  // Returns a GetCurrentShardingSchemeResponse from the cache, if it exists
  absl::StatusOr<orchestrator::GetCurrentShardingSchemeResponse> GetFromCache(
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          cache_context) noexcept;

  // Inserts a GetCurrentShardingSchemeResponse into the cache, keyed by the
  // GetCurrentShardingSchemeRequest
  absl::Status InsertToCache(
      const orchestrator::GetCurrentShardingSchemeRequest& request,
      const orchestrator::GetCurrentShardingSchemeResponse& response) noexcept;

  // The AsyncExecutor used by the concurrent map.
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  // The uncached client used to send requests
  std::shared_ptr<OrchestratorClientInterface> orchestrator_client_;
  // The cache containing successful responses for orchestrator request fetches.
  scp::core::common::AutoExpiryConcurrentMap<std::string, std::string> cache_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_CACHED_ORCHESTRATOR_CLIENT_H_
