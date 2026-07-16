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

#ifndef CC_MATCH_SERVICE_KMS_CLIENT_MOCK_KMS_CLIENT_H_
#define CC_MATCH_SERVICE_KMS_CLIENT_MOCK_KMS_CLIENT_H_

#include <string>

#include "absl/status/status.h"
#include "gmock/gmock.h"

#include "cc/match_service/kms_client/kms_client_interface.h"

namespace google::confidential_match::match_service {

class MockKmsClient : public KmsClientInterface {
 public:
  MOCK_METHOD(absl::Status, Init, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Run, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Stop, (), (noexcept, override));

  MOCK_METHOD(
      void, Decrypt,
      ((google::confidential_match::AsyncContext<
          google::cmrt::sdk::kms_service::v1::DecryptRequest, std::string>)),
      (noexcept));
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_KMS_CLIENT_MOCK_KMS_CLIENT_H_
