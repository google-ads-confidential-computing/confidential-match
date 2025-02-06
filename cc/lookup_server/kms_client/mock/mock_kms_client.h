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

#ifndef CC_LOOKUP_SERVER_KMS_CLIENT_MOCK_MOCK_KMS_CLIENT_H_
#define CC_LOOKUP_SERVER_KMS_CLIENT_MOCK_MOCK_KMS_CLIENT_H_

#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"

#include "cc/lookup_server/interface/kms_client_interface.h"

namespace google::confidential_match::lookup_server {

class MockKmsClient : public KmsClientInterface {
 public:
  MOCK_METHOD(scp::core::ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Stop, (), (noexcept, override));

  scp::core::ExecutionResult Decrypt(
      scp::core::AsyncContext<DecryptRequest, std::string>
          decrypt_context) noexcept override {
    return DecryptAsync(decrypt_context);
  }

  scp::core::ExecutionResultOr<std::string> Decrypt(
      const DecryptRequest& decrypt_request) noexcept override {
    return DecryptSync(decrypt_request);
  }

  MOCK_METHOD(scp::core::ExecutionResult, DecryptAsync,
              ((scp::core::AsyncContext<DecryptRequest, std::string>)),
              (noexcept));
  MOCK_METHOD(scp::core::ExecutionResultOr<std::string>, DecryptSync,
              (const DecryptRequest&), (noexcept));
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_KMS_CLIENT_MOCK_MOCK_KMS_CLIENT_H_
