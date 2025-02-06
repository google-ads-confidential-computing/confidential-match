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

#ifndef CC_LOOKUP_SERVER_COORDINATOR_CLIENT_MOCK_MOCK_COORDINATOR_CLIENT_H_
#define CC_LOOKUP_SERVER_COORDINATOR_CLIENT_MOCK_MOCK_COORDINATOR_CLIENT_H_

#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"

#include "cc/lookup_server/interface/coordinator_client_interface.h"

namespace google::confidential_match::lookup_server {

/** A mock coordinator client for use with gMock in tests. */
class MockCoordinatorClient : public CoordinatorClientInterface {
 public:
  MOCK_METHOD(scp::core::ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Stop, (), (noexcept, override));

  void GetHybridKey(scp::core::AsyncContext<
                    lookup_server::proto_backend::GetHybridKeyRequest,
                    lookup_server::proto_backend::GetHybridKeyResponse>
                        key_context) noexcept override {
    GetHybridKeyAsync(key_context);
  }

  scp::core::ExecutionResultOr<
      lookup_server::proto_backend::GetHybridKeyResponse>
  GetHybridKey(lookup_server::proto_backend::GetHybridKeyRequest
                   request) noexcept override {
    return GetHybridKeySync(request);
  }

  MOCK_METHOD(void, GetHybridKeyAsync,
              ((scp::core::AsyncContext<
                  lookup_server::proto_backend::GetHybridKeyRequest,
                  lookup_server::proto_backend::GetHybridKeyResponse>)),
              (noexcept));
  MOCK_METHOD(scp::core::ExecutionResultOr<
                  lookup_server::proto_backend::GetHybridKeyResponse>,
              GetHybridKeySync,
              (const lookup_server::proto_backend::GetHybridKeyRequest&),
              (noexcept));
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_COORDINATOR_CLIENT_MOCK_MOCK_COORDINATOR_CLIENT_H_
