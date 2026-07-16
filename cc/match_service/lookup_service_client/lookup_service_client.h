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

#ifndef CC_MATCH_SERVICE_LOOKUP_SERVICE_CLIENT_LOOKUP_SERVICE_CLIENT_H_
#define CC_MATCH_SERVICE_LOOKUP_SERVICE_CLIENT_LOOKUP_SERVICE_CLIENT_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/lookup_service_client/lookup_service_client_interface.h"
#include "cc/match_service/lookup_service_shard_client/lookup_service_shard_client_interface.h"
#include "cc/match_service/orchestrator_client/orchestrator_client_interface.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "protos/lookup_server/api/lookup.pb.h"

namespace google::confidential_match::match_service {

// Internal operation state.
struct LookupOperation : public std::enable_shared_from_this<LookupOperation> {
  explicit LookupOperation(AsyncContext<backend::LookupServiceRequest,
                                        backend::LookupServiceResponse>
                               ctx)
      : client_context(std::move(ctx)),
        pending_requests(0),
        operation_failed(false) {}

  AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>
      client_context;

  // Index: [Request number] -> Value: [List of Results from that request]
  std::vector<std::vector<lookup_server::proto_api::LookupResult>>
      shard_results;

  // Index: [Request number] -> Value: [Start time of that request]
  std::vector<absl::Time> shard_request_start_times;

  // Index: [Request number] -> Value: [Shard index for that request]
  std::vector<int> shard_request_shard_indices;

  // Atomic counter for pending requests.
  std::atomic<size_t> pending_requests;

  // Atomic flag to track whether a failure occurred. Used to ensure that the
  // finish callback is only called once.
  std::atomic<bool> operation_failed;

  // Sharding scheme used in this operation.
  std::shared_ptr<orchestrator::GetCurrentShardingSchemeResponse>
      current_scheme;

  // Total number of records in the original request
  size_t total_original_records;

  // Multimap to handle duplicate keys.
  // Key: Lookup Key string
  // Value: Original index in the request vector
  std::unordered_multimap<std::string, size_t> key_to_indices;
};

class LookupServiceClient : public LookupServiceClientInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the LookupServiceClient object.
  LookupServiceClient(
      OrchestratorClientInterface* orchestrator_client,
      LookupServiceShardClientInterface* lookup_service_shard_client,
      int max_records_per_request, absl::string_view cluster_group_id,
      scp::cpio::MetricClientInterface* metric_client = nullptr,
      absl::string_view metric_namespace = "")
      : orchestrator_client_(orchestrator_client),
        lookup_service_shard_client_(lookup_service_shard_client),
        max_records_per_request_(max_records_per_request),
        cluster_group_id_(cluster_group_id),
        metric_client_(metric_client),
        metric_namespace_(metric_namespace) {}

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  void Lookup(AsyncContext<backend::LookupServiceRequest,
                           backend::LookupServiceResponse>&
                  lookup_service_context) noexcept override;

 private:
  // Fetches the sharding scheme from the Orchestrator and triggers the lookup.
  void RefreshShardingSchemeAndPerformLookup(
      std::shared_ptr<LookupOperation> operation) noexcept;

  // Callback for when the sharding scheme is retrieved.
  void OnGetShardingSchemeCallback(
      std::shared_ptr<LookupOperation> operation,
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          context) noexcept;

  // Orchestrates the sharding process: hashing, grouping, and dispatching.
  void PerformShardedLookup(
      std::shared_ptr<LookupOperation> operation) noexcept;

  // Sends the grouped records to their respective shards asynchronously.
  void CreateAndSendShardRequests(
      std::shared_ptr<LookupOperation> operation,
      const lookup_server::proto_api::LookupRequest& base_request,
      const absl::flat_hash_map<
          int, std::vector<lookup_server::proto_api::DataRecord>>&
          records_by_shard);

  // Callback for individual lookups.
  void OnShardLookupCallback(
      std::shared_ptr<LookupOperation> operation, size_t request_index,
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& context) noexcept;

  // Aggregates results from all shards and finishes the client context.
  void FinalizeLookup(std::shared_ptr<LookupOperation> operation);

  // Orchestrator client to get sharding scheme
  OrchestratorClientInterface* orchestrator_client_;
  // Shard client to send requests to individual shards
  LookupServiceShardClientInterface* lookup_service_shard_client_;
  // Max records per request, to avoid overloading lookup service
  int max_records_per_request_;
  // Cluster group id for this instance
  const std::string cluster_group_id_;

  // Metric client to record metrics.
  scp::cpio::MetricClientInterface* metric_client_;
  // Metric namespace for metrics.
  const std::string metric_namespace_;

  // Guard current sharding scheme for concurrent requests
  absl::Mutex sharding_scheme_mutex_;
  std::shared_ptr<orchestrator::GetCurrentShardingSchemeResponse>
      sharding_scheme_ ABSL_GUARDED_BY(sharding_scheme_mutex_);
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_LOOKUP_SERVICE_CLIENT_LOOKUP_SERVICE_CLIENT_H_
