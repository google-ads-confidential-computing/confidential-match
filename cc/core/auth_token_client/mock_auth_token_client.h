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

#ifndef CC_CORE_AUTH_TOKEN_CLIENT_MOCK_AUTH_TOKEN_CLIENT_H_
#define CC_CORE_AUTH_TOKEN_CLIENT_MOCK_AUTH_TOKEN_CLIENT_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "cc/core/auth_token_client/auth_token_client_interface.h"
#include "cc/core/interface/async_context.h"
#include "gmock/gmock.h"

namespace google::confidential_match {

class MockAuthTokenClient : public AuthTokenClientInterface {
 public:
  MOCK_METHOD(absl::Status, Init, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Run, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Stop, (), (noexcept, override));

  MOCK_METHOD(absl::Status, GetAuthToken,
              ((scp::core::AsyncContext<GetAuthTokenRequest,
                                        absl::StatusOr<GetAuthTokenResponse>> &
                get_auth_token_context)),
              (noexcept, override));
};

}  // namespace google::confidential_match

#endif  // CC_CORE_AUTH_TOKEN_CLIENT_MOCK_AUTH_TOKEN_CLIENT_H_
