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

#ifndef CC_MATCH_SERVICE_LOOKUP_SERVICE_SHARD_CLIENT_LOOKUP_SERVICE_SHARD_CLIENT_H_  // NOLINT(whitespace/line_length)
#define CC_MATCH_SERVICE_LOOKUP_SERVICE_SHARD_CLIENT_LOOKUP_SERVICE_SHARD_CLIENT_H_  // NOLINT(whitespace/line_length)

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "cc/core/async/async_context.h"
#include "cc/core/interface/http_client_interface.h"
#include "cc/match_service/auth_token_client/auth_token_client_interface.h"
#include "cc/match_service/lookup_service_shard_client/lookup_service_shard_client_interface.h"
#include "protos/lookup_server/api/lookup.pb.h"

namespace google::confidential_match::match_service {

// Client responsible for interfacing with a Lookup Service Shard.
class LookupServiceShardClient : public LookupServiceShardClientInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the LookupServiceShardClient object.
  explicit LookupServiceShardClient(
      AuthTokenClientInterface* auth_token_client,
      scp::core::HttpClientInterface* http2_client,
      absl::string_view lookup_service_audience,
      bool disable_service_account_email_retrieval = false,
      absl::string_view service_account_email = "")
      : auth_token_client_(*auth_token_client),
        http2_client_(*http2_client),
        lookup_service_audience_(std::string(lookup_service_audience)),
        disable_service_account_email_retrieval_(
            disable_service_account_email_retrieval),
        service_account_email_(std::string(service_account_email)) {}

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Sends a LookupRequest to the lookup service shard asynchronously.
  void Lookup(
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& lookup_context,
      absl::string_view lookup_service_shard_address) noexcept override;

 private:
  // Helper to handle the callback containing auth credentials.
  void OnLookupAuthTokenCallback(
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& lookup_context,
      std::string lookup_service_shard_address,
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
          get_auth_token_context) noexcept;

  void OnLookupServiceAccountEmailCallback(
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& lookup_context,
      std::string lookup_service_shard_address, absl::string_view auth_token,
      AsyncContext<GetServiceAccountEmailRequest,
                   GetServiceAccountEmailResponse>&
          get_service_account_email_context) noexcept;

  // Helper to handle the callback containing the Lookup HTTP response.
  void OnLookupHttpCallback(
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& lookup_context,
      ExecutionResultAsyncContext<scp::core::HttpRequest,
                                  scp::core::HttpResponse>&
          http_context) noexcept;

  // Helper to construct and send the HTTP request to the lookup shard.
  void PerformHttpLookup(
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& lookup_context,
      std::string lookup_service_shard_address, absl::string_view auth_token,
      absl::string_view service_account_email) noexcept;

  // Client for retrieving authentication tokens.
  AuthTokenClientInterface& auth_token_client_;

  // HTTP client used to call the lookup service shard.
  scp::core::HttpClientInterface& http2_client_;

  // The audience value to use when sending lookup service requests
  const std::string lookup_service_audience_;

  // Gating flag to disable service account email retrieval.
  const bool disable_service_account_email_retrieval_;

  // Injected service account email.
  const std::string service_account_email_;
};

}  // namespace google::confidential_match::match_service

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_MATCH_SERVICE_LOOKUP_SERVICE_SHARD_CLIENT_LOOKUP_SERVICE_SHARD_CLIENT_H_
