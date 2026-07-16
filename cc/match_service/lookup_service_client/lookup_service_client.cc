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

#include "cc/match_service/lookup_service_client/lookup_service_client.h"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "cc/core/async/async_context.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/util/jump_consistent_hasher.h"
#include "cc/match_service/converters/lookup_request_converter.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/metrics/metrics_util.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/match_service/backend/lookup.pb.h"

namespace google::confidential_match::match_service {

namespace {
using ::google::cmrt::sdk::metric_service::v1::Metric;
using ::google::cmrt::sdk::metric_service::v1::MetricType;
using ::google::cmrt::sdk::metric_service::v1::MetricUnit;
using ::google::confidential_match::lookup_server::proto_api::DataRecord;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::match_service::backend::Application_Name;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::
    LookupServiceRequest_KeyFormat;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::cpio::MetricClientInterface;

using LookupShardingScheme = ::google::confidential_match::match_service::
    backend::LookupServiceRequest_ShardingScheme;

const std::string& KeyFormat_Name(LookupServiceRequest_KeyFormat format) {
  return LookupServiceRequest_KeyFormat_Name(format);
}

// Helper to group records by their destination shard index using Jump
// Consistent Hashing. Consumes all DataRecords from the proto
// by moving elements out of it.
// We need 'backend_records' because the API record does not contain
// decrypted_key
absl::flat_hash_map<int, std::vector<DataRecord>> GroupRecordsByShard(
    google::protobuf::RepeatedPtrField<DataRecord>& api_records,
    const google::protobuf::RepeatedPtrField<backend::LookupDataRecord>&
        backend_records,
    const GetCurrentShardingSchemeResponse& scheme) {
  absl::flat_hash_map<int, std::vector<DataRecord>> records_by_shard;

  for (int i = 0; i < api_records.size(); ++i) {
    const auto& backend_key = backend_records.at(i).lookup_key();

    // Use decrypted_key if available, otherwise fall back to key
    const std::string& shard_key = backend_key.decrypted_key().empty()
                                       ? backend_key.key()
                                       : backend_key.decrypted_key();

    // Calculate shard index, number of buckets is number of shards
    int shard_idx = JumpConsistentHash(shard_key, scheme.shards_size());
    records_by_shard[shard_idx].push_back(std::move(api_records.at(i)));
  }
  return records_by_shard;
}

Metric CreateLookupServerCountMetric(
    const backend::LookupServiceRequest& request, int shard_idx) {
  Metric m;
  m.set_name(metrics::kLookupServerRequestCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kShardLabel] = absl::StrCat(shard_idx);
  (*m.mutable_labels())[metrics::kApplicationLabel] =
      Application_Name(request.application());
  (*m.mutable_labels())[metrics::kKeyFormatLabel] =
      KeyFormat_Name(request.key_format());
  return m;
}

Metric CreateLookupServerLatencyMetric(
    const backend::LookupServiceRequest& request, int shard_idx,
    absl::Duration latency) {
  Metric m;
  m.set_name(metrics::kLookupServerRequestLatencyMetricName);
  m.set_type(MetricType::METRIC_TYPE_HISTOGRAM);
  m.set_value(absl::StrCat(absl::ToInt64Milliseconds(latency)));
  m.set_unit(MetricUnit::METRIC_UNIT_MILLISECONDS);
  (*m.mutable_labels())[metrics::kShardLabel] = absl::StrCat(shard_idx);
  (*m.mutable_labels())[metrics::kApplicationLabel] =
      Application_Name(request.application());
  (*m.mutable_labels())[metrics::kKeyFormatLabel] =
      KeyFormat_Name(request.key_format());
  return m;
}

Metric CreateLookupServerErrorCountMetric(
    const backend::LookupServiceRequest& request, int shard_idx,
    const absl::Status& status) {
  Metric m;
  m.set_name(metrics::kLookupServerRequestErrorCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kShardLabel] = absl::StrCat(shard_idx);
  (*m.mutable_labels())[metrics::kApplicationLabel] =
      Application_Name(request.application());
  (*m.mutable_labels())[metrics::kKeyFormatLabel] =
      KeyFormat_Name(request.key_format());
  (*m.mutable_labels())[metrics::kBackendErrorReasonLabel] =
      GetBackendErrorReasonString(status);
  return m;
}

}  // namespace

absl::Status LookupServiceClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status LookupServiceClient::Run() noexcept {
  return absl::OkStatus();
}

absl::Status LookupServiceClient::Stop() noexcept {
  return absl::OkStatus();
}

void LookupServiceClient::Lookup(
    AsyncContext<backend::LookupServiceRequest, backend::LookupServiceResponse>&
        lookup_service_context) noexcept {
  // Create an operation object to track the state of this request.
  auto operation = std::make_shared<LookupOperation>(lookup_service_context);

  // Check sharding scheme, refresh if no scheme (first request)
  {
    absl::MutexLock lock(&sharding_scheme_mutex_);
    operation->current_scheme = sharding_scheme_;
  }
  if (operation->current_scheme == nullptr) {
    RefreshShardingSchemeAndPerformLookup(operation);
    return;
  }
  PerformShardedLookup(operation);
}

void LookupServiceClient::RefreshShardingSchemeAndPerformLookup(
    std::shared_ptr<LookupOperation> operation) noexcept {
  GetCurrentShardingSchemeRequest request;
  request.set_cluster_group_id(cluster_group_id_);

  AsyncContext<GetCurrentShardingSchemeRequest,
               GetCurrentShardingSchemeResponse>
      scheme_context = operation->client_context.CreateAsyncContext<
          GetCurrentShardingSchemeRequest, GetCurrentShardingSchemeResponse>();
  scheme_context.request =
      std::make_shared<GetCurrentShardingSchemeRequest>(std::move(request));
  scheme_context.callback = absl::bind_front(
      &LookupServiceClient::OnGetShardingSchemeCallback, this, operation);

  orchestrator_client_->GetCurrentShardingScheme(scheme_context);
}

void LookupServiceClient::OnGetShardingSchemeCallback(
    std::shared_ptr<LookupOperation> operation,
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& context) noexcept {
  if (!context.status.ok()) {
    std::string msg =
        absl::StrCat("Could not get sharding scheme from orchestrator: ",
                     context.status.ToString());
    LOG_ERROR(*context.logger, msg);
    operation->client_context.status =
        Status(Error::ORCHESTRATOR_SERVICE_ERROR, msg);
    operation->client_context.Finish();
    return;
  }

  // Update the global scheme.
  {
    absl::MutexLock lock(&sharding_scheme_mutex_);
    sharding_scheme_ = context.response;
    operation->current_scheme = sharding_scheme_;
  }
  PerformShardedLookup(operation);
}

void LookupServiceClient::PerformShardedLookup(
    std::shared_ptr<LookupOperation> operation) noexcept {
  // Populate the sharding scheme into the request.
  LookupShardingScheme* lookup_sharding_scheme =
      operation->client_context.request->mutable_sharding_scheme();
  lookup_sharding_scheme->set_type(operation->current_scheme->type());
  lookup_sharding_scheme->set_num_shards(
      operation->current_scheme->shards_size());

  // Convert the high-level Backend Request to the internal API Request.
  lookup_server::proto_api::LookupRequest base_request;
  if (auto status =
          ToLookupApi(*operation->client_context.request, base_request);
      !status.ok()) {
    std::string msg = absl::StrCat("Failed to convert backend lookup request: ",
                                   status.ToString());
    LOG_ERROR(*operation->client_context.logger, msg);
    operation->client_context.status =
        Status(Error::CONVERTER_PARSE_ERROR, msg);
    operation->client_context.Finish();
    return;
  }

  // Before moving the records into shards, map the keys to the original index.
  auto* mutable_records = base_request.mutable_data_records();
  operation->total_original_records = mutable_records->size();
  // Get reference to original backend records for routing logic.
  const auto& backend_records =
      operation->client_context.request->data_records();

  // Populate Key to Indices map
  for (int i = 0; i < mutable_records->size(); ++i) {
    const auto& record = mutable_records->at(i);
    // Multimap since the input might contain duplicate keys
    operation->key_to_indices.emplace(record.lookup_key().key(), i);
  }

  // Extract records and group them by shard index
  // (Note: This moves elements out of base_request)
  auto records_by_shard = GroupRecordsByShard(*mutable_records, backend_records,
                                              *operation->current_scheme);

  base_request.clear_data_records();

  // Send shard requests
  CreateAndSendShardRequests(operation, base_request, records_by_shard);
}

void LookupServiceClient::CreateAndSendShardRequests(
    std::shared_ptr<LookupOperation> operation,
    const lookup_server::proto_api::LookupRequest& base_request,
    const absl::flat_hash_map<
        int, std::vector<lookup_server::proto_api::DataRecord>>&
        records_by_shard) {
  // Get Shard Endpoints
  absl::flat_hash_map<int, std::string> shard_endpoints;
  for (const auto& shard : operation->current_scheme->shards()) {
    shard_endpoints[shard.shard_number()] = shard.server_address_uri();
  }

  // Calculate the exact number of async requests we are about to launch.
  // This allows us to use pre-allocated storage for results.
  size_t total_requests_to_send = 0;
  for (const auto& [shard_idx, records] : records_by_shard) {
    total_requests_to_send += (records.size() + max_records_per_request_ - 1) /
                              max_records_per_request_;
  }

  if (total_requests_to_send == 0) {
    operation->client_context.response =
        std::make_shared<backend::LookupServiceResponse>();
    operation->client_context.Finish();
    return;
  }

  // Pre-allocate indices based on total number of requests
  operation->shard_results.assign(total_requests_to_send, {});
  operation->shard_request_start_times.assign(total_requests_to_send,
                                              absl::UnixEpoch());
  operation->shard_request_shard_indices.assign(total_requests_to_send, -1);
  operation->pending_requests = total_requests_to_send;

  size_t current_request_index = 0;
  // Send requests
  for (const auto& [shard_idx, records] : records_by_shard) {
    std::string endpoint = shard_endpoints[shard_idx];

    // Batch records
    for (size_t i = 0; i < records.size(); i += max_records_per_request_) {
      auto batch_request = std::make_shared<LookupRequest>(base_request);
      size_t end = std::min(i + max_records_per_request_, records.size());
      for (size_t j = i; j < end; ++j) {
        *batch_request->add_data_records() = records[j];
      }

      AsyncContext<LookupRequest, LookupResponse> shard_context =
          operation->client_context
              .CreateAsyncContext<LookupRequest, LookupResponse>();
      shard_context.request = batch_request;
      shard_context.callback =
          absl::bind_front(&LookupServiceClient::OnShardLookupCallback, this,
                           operation, current_request_index);

      operation->shard_request_start_times[current_request_index] = absl::Now();
      operation->shard_request_shard_indices[current_request_index] = shard_idx;

      metrics::PutMetric(operation->client_context.logger, metric_client_,
                         metric_namespace_,
                         CreateLookupServerCountMetric(
                             *operation->client_context.request, shard_idx));

      lookup_service_shard_client_->Lookup(shard_context, endpoint);
      ++current_request_index;
    }
  }
}

void LookupServiceClient::OnShardLookupCallback(
    std::shared_ptr<LookupOperation> operation, size_t request_index,
    AsyncContext<LookupRequest, LookupResponse>& context) noexcept {
  const absl::Duration request_duration =
      absl::Now() - operation->shard_request_start_times[request_index];
  const int shard_idx = operation->shard_request_shard_indices[request_index];

  metrics::PutMetric(
      operation->client_context.logger, metric_client_, metric_namespace_,
      CreateLookupServerLatencyMetric(*operation->client_context.request,
                                      shard_idx, request_duration));

  if (!context.status.ok()) {
    metrics::PutMetric(
        operation->client_context.logger, metric_client_, metric_namespace_,
        CreateLookupServerErrorCountMetric(*operation->client_context.request,
                                           shard_idx, context.status));
    // TODO(b/467439162): Add support for invalid schemes
    bool has_previous_failure = operation->operation_failed.exchange(true);
    if (has_previous_failure) {
      // The finish callback was already called with the failure, terminate.
      return;
    }
    std::string msg =
        absl::StrCat("Could not get response from Lookup Service: ",
                     context.status.ToString());
    LOG_ERROR(*operation->client_context.logger, msg);
    operation->client_context.status = Status(Error::LOOKUP_SERVICE_ERROR, msg);
    operation->client_context.Finish();
    return;
  } else {
    // Write to pre-allocated slot
    const auto& results = context.response->lookup_results();
    operation->shard_results[request_index].assign(results.begin(),
                                                   results.end());
  }

  // Decrease pending counter
  size_t pending = operation->pending_requests.fetch_sub(1);
  // If this was the last pending request, finalize.
  // This will only be reached if all threads were successful.
  if (pending == 1) {
    FinalizeLookup(operation);
  }
}

void LookupServiceClient::FinalizeLookup(
    std::shared_ptr<LookupOperation> operation) {
  // Create vector big enough for all input records
  std::vector<LookupResult> ordered_results(operation->total_original_records);

  // Iterate through all results from all shards.
  for (const auto& batch : operation->shard_results) {
    for (const auto& result : batch) {
      // Use client_data_record key to find the index it belongs to.
      const std::string& key = result.client_data_record().lookup_key().key();

      // Find the original index for this key.
      auto search = operation->key_to_indices.find(key);
      if (search != operation->key_to_indices.end()) {
        size_t original_index = search->second;

        // Place the result in its original index.
        ordered_results[original_index] = result;

        // Remove this entry from multimap to handle duplicate keys
        operation->key_to_indices.erase(search);
      } else {
        std::string msg =
            absl::StrCat("Received lookup result for unknown key.");
        LOG_ERROR(*operation->client_context.logger, msg);
        operation->client_context.status =
            Status(Error::LOOKUP_SERVICE_ERROR, msg);
        operation->client_context.Finish();
        return;
      }
    }
  }

  LookupResponse complete_api_response;
  auto* results_list = complete_api_response.mutable_lookup_results();

  for (const auto& result : ordered_results) {
    *results_list->Add() = result;
  }

  // Convert internal API response back to the Backend Response format.
  auto backend_response = std::make_shared<backend::LookupServiceResponse>();
  if (auto status = ToBackend(complete_api_response, *backend_response);
      !status.ok()) {
    std::string msg =
        "Could not convert Lookup Shard client Response to backend";
    LOG_ERROR(*operation->client_context.logger, msg);
    operation->client_context.status =
        Status(Error::CONVERTER_PARSE_ERROR, msg);
  } else {
    operation->client_context.response = backend_response;
  }

  operation->client_context.Finish();
}

}  // namespace google::confidential_match::match_service
