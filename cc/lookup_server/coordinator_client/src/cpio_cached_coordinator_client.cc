// Copyright 2026 Google LLC
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

#include "cc/lookup_server/coordinator_client/src/cpio_cached_coordinator_client.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/errors.h"
#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::private_key_service::v1::PrivateKeyEndpoint;
using ::google::cmrt::sdk::v1::KeyCoordinatorConfiguration;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::protobuf::RepeatedPtrField;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::AsyncPriority;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::cpio::Key;
using ::google::scp::cpio::KeyFetcherWithCacheInterface;

constexpr absl::string_view kComponentName = "CpioCachedCoordinatorClient";

// A struct to hold the information about an endpoint.
// This is used to sort the endpoints in the StringifyEndpointInfos function.
struct EndpointInfo {
  std::string_view endpoint;
  std::string_view wip_provider;
  std::string_view audience_url;

  bool operator<(const EndpointInfo& other) const {
    if (endpoint != other.endpoint) {
      return endpoint < other.endpoint;
    }
    if (wip_provider != other.wip_provider) {
      return wip_provider < other.wip_provider;
    }
    return audience_url < other.audience_url;
  }
};

std::string StringifyEndpointInfos(std::vector<EndpointInfo> endpoints) {
  std::sort(endpoints.begin(), endpoints.end());
  std::string result;
  for (const auto& endpoint : endpoints) {
    absl::StrAppend(&result, endpoint.endpoint, endpoint.wip_provider,
                    endpoint.audience_url);
  }
  return result;
}

ExecutionResult IsRequestValid(const GetHybridKeyRequest& request) {
  if (request.key_id().empty()) {
    auto result =
        FailureExecutionResult(COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "The coordinator request is missing a key ID.");
    return result;
  }
  return SuccessExecutionResult();
}

ExecutionResultOr<std::shared_ptr<KeyFetcherWithCacheInterface>>
ResolveKeyFetcher(
    const GetHybridKeyRequest& request,
    const absl::flat_hash_map<std::string,
                              std::shared_ptr<KeyFetcherWithCacheInterface>>&
        key_fetcher_map) {
  if (key_fetcher_map.empty()) {
    auto result =
        FailureExecutionResult(COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result, "Key fetcher map is empty.");
    return result;
  }

  std::string lookup_key = StringifyCoordinators(request.coordinators());
  auto it = key_fetcher_map.find(lookup_key);
  if (it != key_fetcher_map.end() && it->second != nullptr) {
    return it->second;
  }

  auto result =
      FailureExecutionResult(COORDINATOR_CLIENT_UNKNOWN_COORDINATOR_ERROR);
  SCP_ERROR(kComponentName, kZeroUuid, result,
            "No matching coordinator key fetcher found for the request.");
  return result;
}

}  // namespace

std::string StringifyEndpoints(
    const KeyCoordinatorConfiguration& key_coordinator_configuration) {
  std::vector<EndpointInfo> endpoints;
  endpoints.reserve(key_coordinator_configuration.private_key_endpoints_size());
  for (const auto& endpoint :
       key_coordinator_configuration.private_key_endpoints()) {
    endpoints.push_back({endpoint.endpoint(), endpoint.gcp_wip_provider(),
                         endpoint.gcp_cloud_function_url()});
  }
  return StringifyEndpointInfos(std::move(endpoints));
}

std::string StringifyCoordinators(
    const RepeatedPtrField<GetHybridKeyRequest::Coordinator>& coordinators) {
  std::vector<EndpointInfo> endpoints;
  endpoints.reserve(coordinators.size());
  for (const auto& coordinator : coordinators) {
    endpoints.push_back({coordinator.key_service_endpoint(),
                         coordinator.kms_wip_provider(),
                         coordinator.key_service_audience_url()});
  }
  return StringifyEndpointInfos(std::move(endpoints));
}

std::string StringifyCoordinators(
    const RepeatedPtrField<proto_api::EncryptionKeyInfo::CoordinatorInfo>&
        coordinators) {
  std::vector<EndpointInfo> endpoints;
  endpoints.reserve(coordinators.size());
  for (const auto& coordinator : coordinators) {
    endpoints.push_back({coordinator.key_service_endpoint(),
                         coordinator.kms_wip_provider(),
                         coordinator.key_service_audience_url()});
  }
  return StringifyEndpointInfos(std::move(endpoints));
}

CpioCachedCoordinatorClient::CpioCachedCoordinatorClient(
    std::shared_ptr<AsyncExecutorInterface> async_executor,
    absl::flat_hash_map<std::string,
                        std::shared_ptr<KeyFetcherWithCacheInterface>>
        key_fetcher_map)
    : async_executor_(async_executor),
      key_fetcher_map_(std::move(key_fetcher_map)) {}

ExecutionResult CpioCachedCoordinatorClient::Init() noexcept {
  if (async_executor_ == nullptr || key_fetcher_map_.empty()) {
    auto result =
        FailureExecutionResult(COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Missing required parameters.");
    return result;
  }
  return SuccessExecutionResult();
}

ExecutionResult CpioCachedCoordinatorClient::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult CpioCachedCoordinatorClient::Stop() noexcept {
  return SuccessExecutionResult();
}

ExecutionResultOr<GetHybridKeyResponse>
CpioCachedCoordinatorClient::GetHybridKey(
    GetHybridKeyRequest request) noexcept {
  RETURN_IF_FAILURE(IsRequestValid(request));

  ASSIGN_OR_LOG_AND_RETURN(
      std::shared_ptr<KeyFetcherWithCacheInterface> fetcher,
      ResolveKeyFetcher(request, key_fetcher_map_), kComponentName, kZeroUuid,
      "Failed to resolve key fetcher for the request.");

  ASSIGN_OR_LOG_AND_RETURN(Key key, fetcher->GetKey(request.key_id()),
                           kComponentName, kZeroUuid,
                           "KeyFetcherWithCache failed to fetch the key.");

  GetHybridKeyResponse response;
  response.mutable_hybrid_key()->set_key_id(key.key_id);
  response.mutable_hybrid_key()->set_public_key(key.public_key);
  response.mutable_hybrid_key()->set_private_key(key.private_key);
  return response;
}

void CpioCachedCoordinatorClient::GetHybridKey(
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        key_context) noexcept {
  if (async_executor_ == nullptr) {
    auto result =
        FailureExecutionResult(COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result, "AsyncExecutor is null.");
    key_context.result = result;
    key_context.Finish();
    return;
  }

  auto execution_result = async_executor_->Schedule(
      [this, key_context]() mutable {
        ExecutionResultOr<GetHybridKeyResponse> response_or =
            GetHybridKey(*key_context.request);
        if (!response_or.Successful()) {
          key_context.result = response_or.result();
          key_context.Finish();
          return;
        }
        key_context.result = SuccessExecutionResult();
        key_context.response =
            std::make_shared<GetHybridKeyResponse>(std::move(*response_or));
        key_context.Finish();
      },
      AsyncPriority::Normal);

  if (!execution_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, execution_result,
              "Failed to schedule GetHybridKey operation on AsyncExecutor.");
    key_context.result = execution_result;
    key_context.Finish();
  }
}

}  // namespace google::confidential_match::lookup_server
