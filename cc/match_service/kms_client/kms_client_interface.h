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

#ifndef CC_MATCH_SERVICE_KMS_CLIENT_KMS_CLIENT_INTERFACE_H_
#define CC_MATCH_SERVICE_KMS_CLIENT_KMS_CLIENT_INTERFACE_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"

#include "cc/core/async/async_context.h"

namespace google::confidential_match::match_service {

// Interface for a client used to interact with Cloud KMS.
class KmsClientInterface {
 public:
  virtual ~KmsClientInterface() = default;

  virtual absl::Status Init() noexcept = 0;
  virtual absl::Status Run() noexcept = 0;
  virtual absl::Status Stop() noexcept = 0;

  // Decrypts ciphertext using cloud KMS asynchronously.
  virtual void Decrypt(
      google::confidential_match::AsyncContext<
          google::cmrt::sdk::kms_service::v1::DecryptRequest, std::string>
          decrypt_context) noexcept = 0;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_KMS_CLIENT_KMS_CLIENT_INTERFACE_H_
