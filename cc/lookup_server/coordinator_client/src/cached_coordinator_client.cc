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

#include "cc/lookup_server/coordinator_client/src/cached_coordinator_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "cc/core/common/auto_expiry_concurrent_map/src/error_codes.h"
#include "cc/core/common/concurrent_map/src/error_codes.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/utils/sync_utils/src/sync_utils.h"

#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::RetryExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::
    SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_ALREADY_EXISTS;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST;
using ::google::scp::cpio::SyncUtils;
using ::std::placeholders::_1;
using ::std::placeholders::_2;
using ::std::placeholders::_3;

constexpr absl::string_view kComponentName = "CachedCoordinatorClient";
// Defines how long items are kept in the cache for (in seconds).
constexpr int kDecryptionCacheEntryLifetimeSec = 3600;

// Callback invoked by the cache before removing stale entries.
void OnBeforeCacheGarbageCollection(
    std::string& key, std::string& value,
    std::function<void(bool)> should_delete_entry) {
  should_delete_entry(true);
}

// Converts a coordinator fetch request into a key used to cache a response.
std::string GetCacheKey(const GetHybridKeyRequest& key_request) {
  return key_request.SerializeAsString();
}

// Converts a coordinator fetch response into a serialized form for storage.
std::string GetCacheValue(const GetHybridKeyResponse& key_response) {
  return key_response.SerializeAsString();
}

// Converts a serialized cached response to a response object.
ExecutionResultOr<GetHybridKeyResponse> ParseCacheValue(
    absl::string_view cache_value) {
  GetHybridKeyResponse response;
  if (!response.ParseFromString(cache_value)) {
    FailureExecutionResult result(
        COORDINATOR_CLIENT_CACHE_DESERIALIZATION_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Unable to deserialize a cached coordinator response object.")
    return result;
  }
  return response;
}

}  // namespace

CachedCoordinatorClient::CachedCoordinatorClient(
    std::shared_ptr<AsyncExecutorInterface> async_executor,
    std::shared_ptr<CoordinatorClientInterface> coordinator_client)
    : async_executor_(async_executor),
      coordinator_client_(coordinator_client),
      cache_(kDecryptionCacheEntryLifetimeSec,
             /* extend_entry_lifetime_on_access= */ true,
             /* block_entry_while_eviction= */ true,
             std::bind(&OnBeforeCacheGarbageCollection, _1, _2, _3),
             async_executor) {}

ExecutionResult CachedCoordinatorClient::Init() noexcept {
  return cache_.Init();
}

ExecutionResult CachedCoordinatorClient::Run() noexcept {
  return cache_.Run();
}

ExecutionResult CachedCoordinatorClient::Stop() noexcept {
  return cache_.Stop();
}

ExecutionResultOr<GetHybridKeyResponse> CachedCoordinatorClient::GetHybridKey(
    GetHybridKeyRequest request) noexcept {
  GetHybridKeyResponse response;
  ExecutionResult result =
      SyncUtils::AsyncToSync2<GetHybridKeyRequest, GetHybridKeyResponse>(
          std::bind(
              // NOLINTNEXTLINE(whitespace/parens)
              static_cast<void (CachedCoordinatorClient::*)(
                  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>)>(
                  &CachedCoordinatorClient::GetHybridKey),
              this, _1),
          request, response);
  RETURN_AND_LOG_IF_FAILURE(result, kComponentName, kZeroUuid,
                            "Coordinator client failed to fetch the key.");
  return response;
}

void CachedCoordinatorClient::GetHybridKey(
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        key_context) noexcept {
  ExecutionResultOr<GetHybridKeyResponse> cached_response_or =
      GetFromCache(*key_context.request);
  if (cached_response_or.Successful()) {
    key_context.result = SuccessExecutionResult();
    key_context.response =
        std::make_shared<GetHybridKeyResponse>(std::move(*cached_response_or));
    key_context.Finish();
    return;
  }

  // No entry fetched from the cache. Fetch the coordinator key
  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse> get_context;
  get_context.request = key_context.request;
  get_context.callback =
      std::bind(&CachedCoordinatorClient::HandleGetHybridKeyCallback, this, _1,
                key_context);
  coordinator_client_->GetHybridKey(get_context);
}

void CachedCoordinatorClient::HandleGetHybridKeyCallback(
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        coordinator_fetch_context,
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        output_context) noexcept {
  if (!coordinator_fetch_context.result.Successful()) {
    output_context.result = coordinator_fetch_context.result;
    output_context.Finish();
    return;
  }

  ExecutionResult cache_result = InsertToCache(
      *output_context.request, *coordinator_fetch_context.response);
  if (!cache_result.Successful()) {
    SCP_WARNING_CONTEXT(
        kComponentName, coordinator_fetch_context,
        absl::StrFormat("Successfully retrieved a coordinator key, "
                        "but unable to cache the response. Error: %s",
                        GetErrorMessage(cache_result.status_code)));
  }

  output_context.result = SuccessExecutionResult();
  output_context.response = coordinator_fetch_context.response;
  output_context.Finish();
}

ExecutionResult CachedCoordinatorClient::InsertToCache(
    const GetHybridKeyRequest& request,
    const GetHybridKeyResponse& response) noexcept {
  std::pair<std::string, std::string> key_value_pair(GetCacheKey(request),
                                                     GetCacheValue(response));
  std::string existing_response_value;
  ExecutionResult result =
      cache_.Insert(key_value_pair, existing_response_value);
  if (result.status_code == SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED) {
    return RetryExecutionResult(COORDINATOR_CLIENT_CACHE_ENTRY_BEING_DELETED);
  } else if (result.status_code == SC_CONCURRENT_MAP_ENTRY_ALREADY_EXISTS) {
    return FailureExecutionResult(
        COORDINATOR_CLIENT_CACHE_ENTRY_ALREADY_EXISTS);
  } else if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Failed to cache the coordinator response.");
    return FailureExecutionResult(COORDINATOR_CLIENT_CACHE_ENTRY_INSERT_ERROR);
  }

  return SuccessExecutionResult();
}

ExecutionResultOr<GetHybridKeyResponse> CachedCoordinatorClient::GetFromCache(
    const GetHybridKeyRequest& request) noexcept {
  std::string serialized_response;
  ExecutionResult find_result =
      cache_.Find(GetCacheKey(request), serialized_response);
  if (find_result.status_code == SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST) {
    return FailureExecutionResult(SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST);
  } else if (find_result.status_code ==
             SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED) {
    return RetryExecutionResult(COORDINATOR_CLIENT_CACHE_ENTRY_BEING_DELETED);
  } else if (!find_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, find_result,
              "Failed to fetch the cached coordinator response.");
    return FailureExecutionResult(COORDINATOR_CLIENT_CACHE_ENTRY_FETCH_ERROR);
  }

  return ParseCacheValue(serialized_response);
}

}  // namespace google::confidential_match::lookup_server
