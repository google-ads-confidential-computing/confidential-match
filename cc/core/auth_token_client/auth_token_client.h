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

#ifndef CC_CORE_AUTH_TOKEN_CLIENT_AUTH_TOKEN_CLIENT_H_
#define CC_CORE_AUTH_TOKEN_CLIENT_AUTH_TOKEN_CLIENT_H_

#include <string>

#include "cc/core/auth_token_client/auth_token_client_interface.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/http_client_interface.h"
#include "cc/core/interface/http_types.h"

namespace google::confidential_match {

// Client responsible for getting Authentication Tokens.
class AuthTokenClient : public AuthTokenClientInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the PutDataRecordsTask object.
  explicit AuthTokenClient(scp::core::HttpClientInterface* http1_client)
      : http1_client_(*http1_client) {}

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Makes an asynchronous request to get the auth token from the host instance
  absl::Status GetAuthToken(
      scp::core::AsyncContext<GetAuthTokenRequest,
                              absl::StatusOr<GetAuthTokenResponse>>&
          get_auth_token_context) noexcept override;

 private:
  // Helper to handle the callback containing the GetAuthTokenResponse HTTP
  // response.
  void OnGetAuthTokenCallback(
      scp::core::AsyncContext<GetAuthTokenRequest,
                              absl::StatusOr<GetAuthTokenResponse>>&
          get_auth_token_context,
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  // HTTP client used to fetch auth credentials.
  scp::core::HttpClientInterface& http1_client_;
};
}  // namespace google::confidential_match

#endif  // CC_CORE_AUTH_TOKEN_CLIENT_AUTH_TOKEN_CLIENT_H_
