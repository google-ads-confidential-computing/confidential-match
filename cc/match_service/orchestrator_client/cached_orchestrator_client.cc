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

#include "cc/match_service/orchestrator_client/cached_orchestrator_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_types.h"

#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/match_service/error/error.h"
#include "cc/public/core/interface/execution_result.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::FinishContext;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::AutoExpiryConcurrentMap;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST;

// How long responses are cached for in seconds.
constexpr int kCacheEntryLifetimeSeconds = 3600;

// Callback invoked by the cache before removing stale entries.
void OnBeforeCacheGarbageCollection(
    std::string& key, std::string& value,
    std::function<void(bool)> should_delete_entry) {
  should_delete_entry(true);
}

// Converts a GetCurrentShardingSchemeRequest into a key used to cache a
// response.
std::string GetCacheKey(
    const GetCurrentShardingSchemeRequest& sharding_scheme_request) {
  return sharding_scheme_request.SerializeAsString();
}

// Converts a GetCurrentShardingSchemeResponse into a serialized form for
// storage.
std::string GetCacheValue(
    const GetCurrentShardingSchemeResponse& sharding_scheme_response) {
  return sharding_scheme_response.SerializeAsString();
}

// Converts a serialized cached response to a response object.
absl::StatusOr<GetCurrentShardingSchemeResponse> ParseCacheValue(
    absl::string_view cache_value) {
  GetCurrentShardingSchemeResponse response;
  if (!response.ParseFromString(cache_value)) {
    return Status(Error::ORCHESTRATOR_RESPONSE_DESERIALIZATION_ERROR,
                  "Could not parse sharding scheme response from cache.");
  }
  return {response};
}

}  // namespace

CachedOrchestratorClient::CachedOrchestratorClient(
    std::shared_ptr<google::scp::core::AsyncExecutorInterface> async_executor,
    std::shared_ptr<OrchestratorClientInterface> orchestrator_client)
    : async_executor_(std::move(async_executor)),
      orchestrator_client_(std::move(orchestrator_client)),
      cache_(kCacheEntryLifetimeSeconds,
             /* extend_entry_lifetime_on_access= */ true,
             /* block_entry_while_eviction= */ true,
             absl::bind_front(&OnBeforeCacheGarbageCollection),
             async_executor_) {}

absl::Status CachedOrchestratorClient::Init() noexcept {
  if (auto result = cache_.Init(); !result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to init orchestrator cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status CachedOrchestratorClient::Run() noexcept {
  if (auto result = cache_.Run(); !result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to run orchestrator cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status CachedOrchestratorClient::Stop() noexcept {
  if (auto result = cache_.Stop(); !result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to stop orchestrator cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

void CachedOrchestratorClient::GetCurrentShardingScheme(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& cache_context) noexcept {
  absl::StatusOr<GetCurrentShardingSchemeResponse> cached_response_or =
      GetFromCache(cache_context);
  if (cached_response_or.ok()) {
    cache_context.response = std::make_shared<GetCurrentShardingSchemeResponse>(
        std::move(*cached_response_or));
    cache_context.Finish();
    return;
  }

  // No entry found from the cache. Fetch the sharding scheme
  auto fetch_context =
      cache_context.CreateAsyncContext<GetCurrentShardingSchemeRequest,
                                       GetCurrentShardingSchemeResponse>();
  fetch_context.request = cache_context.request;
  fetch_context.callback = absl::bind_front(
      &CachedOrchestratorClient::OnGetCurrentShardingSchemeCallback, this,
      cache_context);

  orchestrator_client_->GetCurrentShardingScheme(fetch_context);
}

void CachedOrchestratorClient::OnGetCurrentShardingSchemeCallback(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& cache_context,
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& fetch_context) noexcept {
  if (!fetch_context.status.ok()) {
    cache_context.status =
        Annotate(fetch_context.status,
                 "Failed to fetch sharding scheme from underlying client.");
    LOG_ERROR(*fetch_context.logger, "%v", cache_context.status);
    cache_context.Finish();
    return;
  }

  const auto& sharding_scheme_response = *fetch_context.response;
  absl::Status cache_result =
      InsertToCache(*cache_context.request, sharding_scheme_response);
  if (!cache_result.ok()) {
    std::string msg = absl::StrCat(
        "Successfully retrieved a sharding scheme, "
        "but unable to cache the response. Error: %s",
        cache_result);
    LOG_WARNING(*fetch_context.logger, msg);
  }

  cache_context.response = fetch_context.response;
  cache_context.Finish();
}

absl::Status CachedOrchestratorClient::InsertToCache(
    const GetCurrentShardingSchemeRequest& request,
    const GetCurrentShardingSchemeResponse& response) noexcept {
  std::pair<std::string, std::string> key_value_pair(GetCacheKey(request),
                                                     GetCacheValue(response));
  std::string existing_response_value;
  ExecutionResult insert_result =
      cache_.Insert(key_value_pair, existing_response_value);
  if (!insert_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  GetErrorMessage(insert_result.status_code));
  }
  return absl::OkStatus();
}

absl::StatusOr<GetCurrentShardingSchemeResponse>
CachedOrchestratorClient::GetFromCache(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& cache_context) noexcept {
  std::string serialized_response;
  ExecutionResult find_result =
      cache_.Find(GetCacheKey(*cache_context.request), serialized_response);
  if (find_result.status_code == SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST) {
    return Status(Error::NOT_FOUND,
                  "The sharding scheme was not found in the cache.");
  } else if (!find_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  GetErrorMessage(find_result.status_code));
  }
  absl::StatusOr<GetCurrentShardingSchemeResponse> status_or =
      ParseCacheValue(serialized_response);
  if (!status_or.ok()) {
    std::string msg = absl::StrCat(
        "Unable to deserialize a cached GetCurrentShardingSchemeResponse "
        "object.");
    LOG_ERROR(*cache_context.logger, msg);
  }
  return status_or;
}

}  // namespace google::confidential_match::match_service
