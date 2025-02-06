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

#ifndef CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_SRC_ORCHESTRATOR_CLIENT_H_
#define CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_SRC_ORCHESTRATOR_CLIENT_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/http_client_interface.h"
#include "cc/core/interface/http_types.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/core/data_table/src/data_table.h"
#include "cc/lookup_server/interface/orchestrator_client_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Represents a request to get an auth token. */
struct GetAuthenticationTokenRequest {
  std::string audience;
};

/** @brief Represents a response from getting an auth token. */
struct GetAuthenticationTokenResponse {
  std::string token;
};

/**
 * @brief Client responsible for interfacing with the Orchestrator.
 */
class OrchestratorClient : public OrchestratorClientInterface {
 public:
  explicit OrchestratorClient(
      std::shared_ptr<scp::core::HttpClientInterface> http1_client,
      std::shared_ptr<scp::core::HttpClientInterface> http2_client,
      absl::string_view orchestrator_host_address)
      : http1_client_(http1_client),
        http2_client_(http2_client),
        orchestrator_host_address_(orchestrator_host_address) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  // Gets the latest data export from Orchestrator synchronously.
  scp::core::ExecutionResultOr<GetDataExportInfoResponse> GetDataExportInfo(
      const GetDataExportInfoRequest& request) noexcept override;

  // Gets the latest data export from Orchestrator asynchronously.
  scp::core::ExecutionResult GetDataExportInfo(
      scp::core::AsyncContext<GetDataExportInfoRequest,
                              GetDataExportInfoResponse>&
          get_data_export_context) noexcept override;

 protected:
  // Helper to handle the callback containing auth credentials.
  void OnGetDataExportInfoAuthCallback(
      scp::core::AsyncContext<GetDataExportInfoRequest,
                              GetDataExportInfoResponse>&
          get_data_export_context,
      scp::core::AsyncContext<GetAuthenticationTokenRequest,
                              GetAuthenticationTokenResponse>&
          get_auth_token_context) noexcept;

  // Helper to handle the callback containing the Orchestrator HTTP response.
  void OnGetDataExportInfoHttpCallback(
      scp::core::AsyncContext<GetDataExportInfoRequest,
                              GetDataExportInfoResponse>&
          get_data_export_context,
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  // Retrieves the authentication token used to get the data export.
  scp::core::ExecutionResult GetAuthenticationToken(
      scp::core::AsyncContext<GetAuthenticationTokenRequest,
                              GetAuthenticationTokenResponse>&
          get_authentication_token_context) noexcept;

  // Helper to handle the callback from the get authentication token method.
  void OnGetAuthenticationTokenCallback(
      scp::core::AsyncContext<GetAuthenticationTokenRequest,
                              GetAuthenticationTokenResponse>&
          get_authentication_token_context,
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  // HTTP client used to fetch auth credentials.
  std::shared_ptr<scp::core::HttpClientInterface> http1_client_;
  // HTTP client used to call the orchestrator.
  std::shared_ptr<scp::core::HttpClientInterface> http2_client_;
  // The host address to the Orchestrator service.
  const std::string orchestrator_host_address_;
};
}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_SRC_ORCHESTRATOR_CLIENT_H_
