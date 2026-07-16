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

#ifndef CC_MATCH_SERVICE_KMS_CLIENT_FAKE_KMS_CLIENT_H_
#define CC_MATCH_SERVICE_KMS_CLIENT_FAKE_KMS_CLIENT_H_

#include <memory>
#include <string>

#include "absl/status/status.h"

#include "cc/match_service/kms_client/kms_client_interface.h"

namespace google::confidential_match::match_service {

class FakeKmsClient : public KmsClientInterface {
 public:
  absl::Status Init() noexcept override { return absl::OkStatus(); }

  absl::Status Run() noexcept override { return absl::OkStatus(); }

  absl::Status Stop() noexcept override { return absl::OkStatus(); }

  void Decrypt(google::confidential_match::AsyncContext<
               google::cmrt::sdk::kms_service::v1::DecryptRequest, std::string>
                   decrypt_context) noexcept override {
    // Set a successful response (empty string by default)
    decrypt_context.response = std::make_shared<std::string>("");
    decrypt_context.Finish();
  }
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_KMS_CLIENT_FAKE_KMS_CLIENT_H_
