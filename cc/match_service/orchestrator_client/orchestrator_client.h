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

#ifndef CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_ORCHESTRATOR_CLIENT_H_
#define CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_ORCHESTRATOR_CLIENT_H_

#include <string>

#include <core/interface/http_client_interface.h>
#include "absl/status/status.h"
#include "cc/core/async/async_context.h"
#include "cc/match_service/auth_token_client/auth_token_client_interface.h"
#include "cc/match_service/orchestrator_client/orchestrator_client_interface.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {

// Client responsible for interfacing with the Orchestrator.
class OrchestratorClient : public OrchestratorClientInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the OrchestratorClient object.
  explicit OrchestratorClient(
      match_service::AuthTokenClientInterface* auth_token_client,
      scp::core::HttpClientInterface* http2_client,
      absl::string_view orchestrator_host_address)
      : auth_token_client_(*auth_token_client),
        http2_client_(*http2_client),
        orchestrator_host_address_(std::string(orchestrator_host_address)) {}

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Gets the current sharding scheme from Orchestrator asynchronously.
  void GetCurrentShardingScheme(
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          get_sharding_scheme_context) noexcept override;

 private:
  // Helper to handle the callback containing auth credentials.
  void OnGetCurrentShardingSchemeAuthCallback(
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          get_sharding_scheme_context,
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
          get_auth_token_context) noexcept;

  // Helper to handle the callback containing the GetCurrentShardingScheme HTTP
  // response.
  void OnGetCurrentShardingSchemeHttpCallback(
      AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                   orchestrator::GetCurrentShardingSchemeResponse>&
          get_sharding_scheme_context,
      ExecutionResultAsyncContext<scp::core::HttpRequest,
                                  scp::core::HttpResponse>&
          http_context) noexcept;

  // Client for retrieving authentication tokens
  match_service::AuthTokenClientInterface& auth_token_client_;

  // HTTP client used to call the orchestrator.
  scp::core::HttpClientInterface& http2_client_;
  // The host address to the Orchestrator service.
  const std::string orchestrator_host_address_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_ORCHESTRATOR_CLIENT_H_
