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

#ifndef CC_CORE_AUTH_TOKEN_CLIENT_AUTH_TOKEN_CLIENT_INTERFACE_H_
#define CC_CORE_AUTH_TOKEN_CLIENT_AUTH_TOKEN_CLIENT_INTERFACE_H_

#include "absl/status/statusor.h"
#include "cc/core/interface/async_context.h"
#include "protos/core/auth_token.pb.h"

namespace google::confidential_match {

// Interface for client that gets Authentication Tokens.
class AuthTokenClientInterface {
 public:
  virtual ~AuthTokenClientInterface() = default;

  virtual absl::Status Init() noexcept = 0;
  virtual absl::Status Run() noexcept = 0;
  virtual absl::Status Stop() noexcept = 0;

  // Makes an asynchronous request to get the auth token from the host instance
  virtual absl::Status GetAuthToken(
      scp::core::AsyncContext<GetAuthTokenRequest,
                              absl::StatusOr<GetAuthTokenResponse>>&
          get_auth_token_context) noexcept = 0;
};

}  // namespace google::confidential_match

#endif  // CC_CORE_AUTH_TOKEN_CLIENT_AUTH_TOKEN_CLIENT_INTERFACE_H_
