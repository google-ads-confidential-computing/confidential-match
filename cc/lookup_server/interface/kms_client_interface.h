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

#ifndef CC_LOOKUP_SERVER_INTERFACE_KMS_CLIENT_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_KMS_CLIENT_INTERFACE_H_

#include <string>

#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"

namespace google::confidential_match::lookup_server {

// The request object for a decryption operation.
using DecryptRequest = ::google::cmrt::sdk::kms_service::v1::DecryptRequest;

/** @brief Interface for a client used to interact with Cloud KMS. */
class KmsClientInterface : public scp::core::ServiceInterface {
 public:
  virtual ~KmsClientInterface() = default;

  /**
   * @brief Decrypts ciphertext using cloud KMS asynchonously.
   *
   * @param decrypt_context the context containing the decryption request
   * @return whether or not the decryption operation was scheduled successfully
   */
  virtual scp::core::ExecutionResult Decrypt(
      scp::core::AsyncContext<DecryptRequest, std::string>
          decrypt_context) noexcept = 0;

  /**
   * @brief Decrypts ciphertext using cloud KMS synchronously.
   *
   * @param decrypt_request the request containing the payload to be decrypted
   * @return the decrypted payload or a failure result on error
   */
  virtual scp::core::ExecutionResultOr<std::string> Decrypt(
      const DecryptRequest& decrypt_request) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_KMS_CLIENT_INTERFACE_H_
