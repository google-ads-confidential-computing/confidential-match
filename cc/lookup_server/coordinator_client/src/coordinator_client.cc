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

#include "cc/lookup_server/coordinator_client/src/coordinator_client.h"

#include <memory>

#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/private_key_client/private_key_client_interface.h"
#include "cc/public/cpio/proto/private_key_service/v1/private_key_service.pb.h"
#include "cc/public/cpio/utils/sync_utils/src/sync_utils.h"

#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::private_key_service::v1::ListPrivateKeysRequest;
using ::google::cmrt::sdk::private_key_service::v1::ListPrivateKeysResponse;
using ::google::cmrt::sdk::private_key_service::v1::PrivateKey;
using ::google::cmrt::sdk::private_key_service::v1::PrivateKeyEndpoint;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyRequest;
using ::google::confidential_match::lookup_server::proto_backend::
    GetHybridKeyResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::cpio::PrivateKeyClientInterface;
using ::google::scp::cpio::SyncUtils;
using ::std::placeholders::_1;

constexpr absl::string_view kComponentName = "CoordinatorClient";

// Helper to check whether a GetHybridKeyRequest object is valid.
ExecutionResult IsRequestValid(const GetHybridKeyRequest& request) {
  if (request.key_id().empty()) {
    auto result =
        FailureExecutionResult(COORDINATOR_CLIENT_KEY_NOT_FOUND_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "The coordinator request is missing a key ID.");
    return result;
  }

  if (request.coordinators_size() == 0) {
    auto result =
        FailureExecutionResult(COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "The coordinator request is missing coordinator parameters.");
    return result;
  }

  for (const auto& coordinator : request.coordinators()) {
    if (coordinator.key_service_endpoint().empty()) {
      auto result =
          FailureExecutionResult(COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR);
      SCP_ERROR(
          kComponentName, kZeroUuid, result,
          "The coordinator request contains an empty key service endpoint.");
      return result;
    }
  }

  return SuccessExecutionResult();
}

}  // namespace

CoordinatorClient::CoordinatorClient(
    std::shared_ptr<PrivateKeyClientInterface> private_key_client)
    : private_key_client_(private_key_client) {}

ExecutionResult CoordinatorClient::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult CoordinatorClient::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult CoordinatorClient::Stop() noexcept {
  return SuccessExecutionResult();
}

void CoordinatorClient::GetHybridKey(
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        key_context) noexcept {
  ExecutionResult validation_result = IsRequestValid(*key_context.request);
  if (!validation_result.Successful()) {
    key_context.result = validation_result;
    key_context.Finish();
    return;
  }

  AsyncContext<ListPrivateKeysRequest, ListPrivateKeysResponse> cpio_context;
  cpio_context.request = std::make_shared<ListPrivateKeysRequest>();
  *cpio_context.request->add_key_ids() = key_context.request->key_id();

  for (const auto& coordinator : key_context.request->coordinators()) {
    PrivateKeyEndpoint* endpoint = cpio_context.request->add_key_endpoints();
    endpoint->set_endpoint(coordinator.key_service_endpoint());
    endpoint->set_account_identity(coordinator.account_identity());
    endpoint->set_gcp_wip_provider(coordinator.kms_wip_provider());
    endpoint->set_gcp_cloud_function_url(
        coordinator.key_service_audience_url());
  }

  cpio_context.callback = std::bind(
      &CoordinatorClient::HandleListPrivateKeysCallback, this, _1, key_context);
  private_key_client_->ListPrivateKeys(cpio_context);
}

void CoordinatorClient::HandleListPrivateKeysCallback(
    AsyncContext<ListPrivateKeysRequest, ListPrivateKeysResponse>&
        cpio_list_context,
    AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>
        hybrid_key_context) noexcept {
  if (!cpio_list_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, cpio_list_context, cpio_list_context.result,
        "CPIO private key client failed to fetch the coordinator key.");
    hybrid_key_context.result =
        FailureExecutionResult(COORDINATOR_CLIENT_KEY_FETCH_ERROR);
    hybrid_key_context.Finish();
    return;
  }

  PrivateKey* private_key = nullptr;
  for (PrivateKey& key : *cpio_list_context.response->mutable_private_keys()) {
    if (key.key_id() == hybrid_key_context.request->key_id()) {
      private_key = &key;
      break;
    }
  }
  if (private_key == nullptr) {
    ExecutionResult not_found_result =
        FailureExecutionResult(COORDINATOR_CLIENT_KEY_NOT_FOUND_ERROR);
    SCP_ERROR_CONTEXT(
        kComponentName, cpio_list_context, not_found_result,
        "Failed to find a coordinator key for the provided key ID.");
    hybrid_key_context.result = not_found_result;
    hybrid_key_context.Finish();
    return;
  }

  hybrid_key_context.result = SuccessExecutionResult();
  hybrid_key_context.response = std::make_shared<GetHybridKeyResponse>();
  hybrid_key_context.response->mutable_hybrid_key()->set_allocated_key_id(
      private_key->release_key_id());
  hybrid_key_context.response->mutable_hybrid_key()->set_allocated_public_key(
      private_key->release_public_key());
  hybrid_key_context.response->mutable_hybrid_key()->set_allocated_private_key(
      private_key->release_private_key());

  hybrid_key_context.Finish();
}

ExecutionResultOr<GetHybridKeyResponse> CoordinatorClient::GetHybridKey(
    GetHybridKeyRequest request) noexcept {
  GetHybridKeyResponse response;
  ExecutionResult result =
      SyncUtils::AsyncToSync2<GetHybridKeyRequest, GetHybridKeyResponse>(
          std::bind(
              // NOLINTNEXTLINE(whitespace/parens)
              static_cast<void (CoordinatorClient::*)(
                  AsyncContext<GetHybridKeyRequest, GetHybridKeyResponse>)>(
                  &CoordinatorClient::GetHybridKey),
              this, _1),
          request, response);
  RETURN_AND_LOG_IF_FAILURE(result, kComponentName, kZeroUuid,
                            "Coordinator client failed to fetch the key.");
  return response;
}

}  // namespace google::confidential_match::lookup_server
