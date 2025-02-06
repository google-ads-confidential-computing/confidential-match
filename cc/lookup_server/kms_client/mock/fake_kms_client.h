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

#ifndef CC_LOOKUP_SERVER_KMS_CLIENT_MOCK_FAKE_KMS_CLIENT_H_
#define CC_LOOKUP_SERVER_KMS_CLIENT_MOCK_FAKE_KMS_CLIENT_H_

#include <memory>
#include <string>

#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/kms_client_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

class FakeKmsClient : public KmsClientInterface {
 public:
  scp::core::ExecutionResult Init() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Run() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Stop() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Decrypt(
      scp::core::AsyncContext<DecryptRequest, std::string>
          decrypt_context) noexcept override {
    decrypt_context.result = scp::core::SuccessExecutionResult();
    decrypt_context.response = std::make_shared<std::string>();
    decrypt_context.Finish();
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResultOr<std::string> Decrypt(
      const DecryptRequest& decrypt_request) noexcept override {
    return "";
  }
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_KMS_CLIENT_MOCK_FAKE_KMS_CLIENT_H_
