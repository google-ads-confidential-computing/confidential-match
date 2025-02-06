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

#ifndef CC_LOOKUP_SERVER_KMS_CLIENT_SRC_KMS_CLIENT_H_
#define CC_LOOKUP_SERVER_KMS_CLIENT_SRC_KMS_CLIENT_H_

#include <memory>
#include <string>

#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/kms_client/kms_client_interface.h"
#include "core/interface/async_context.h"

#include "cc/lookup_server/interface/kms_client_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A client that interfaces directly with Cloud KMS.
 *
 * This is a thin wrapper layer around the SCP CPIO KMS client.
 */
class KmsClient : public KmsClientInterface {
 public:
  /**
   * @brief Constructs a KMS client.
   */
  KmsClient()
      : kms_client_(scp::cpio::KmsClientFactory::Create(
            scp::cpio::KmsClientOptions())) {}

  /**
   * @brief Constructs a KMS client from a CPIO KMS client. Used for tests only.
   *
   * This KmsClient will call Init()/Run()/Stop() on the provided CPIO client.
   */
  explicit KmsClient(
      std::shared_ptr<scp::cpio::KmsClientInterface> cpio_kms_client)
      : kms_client_(cpio_kms_client) {}

  scp::core::ExecutionResult Init() noexcept;
  scp::core::ExecutionResult Run() noexcept;
  scp::core::ExecutionResult Stop() noexcept;

  /**
   * @brief Decrypts a payload using Cloud KMS asynchronously.
   *
   * @param decrypt_context the context containing the request and callback
   * @return a SuccessExecutionResult if the operation was started successfully
   */
  scp::core::ExecutionResult Decrypt(
      scp::core::AsyncContext<DecryptRequest, std::string>
          decrypt_context) noexcept;

  /**
   * @brief Decrypts a payload using Cloud KMS synchronously.
   *
   * @param decrypt_request the payload to be decrypted
   * @return the decrypted payload, or a FailureExecutionResult on error
   */
  scp::core::ExecutionResultOr<std::string> Decrypt(
      const DecryptRequest& decrypt_request) noexcept;

 private:
  // The wrapped CPIO KMS client.
  std::shared_ptr<scp::cpio::KmsClientInterface> kms_client_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_KMS_CLIENT_SRC_KMS_CLIENT_H_
