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

#ifndef CC_MATCH_SERVICE_KMS_CLIENT_KMS_CLIENT_H_
#define CC_MATCH_SERVICE_KMS_CLIENT_KMS_CLIENT_H_

#include <memory>
#include <string>
#include <cstdint>

#include "absl/status/status.h"
#include "cc/public/cpio/interface/kms_client/kms_client_interface.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

// A client that interfaces directly with Cloud KMS.
// This is a thin wrapper layer around the SCP CPIO KMS client.
class KmsClient : public KmsClientInterface {
 public:
  // Constructs a KMS client with default options.
  KmsClient();

  // Constructs a KMS client from a CPIO KMS client. Used for tests only.
  explicit KmsClient(
      std::shared_ptr<scp::cpio::KmsClientInterface> cpio_kms_client);

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Decrypts a payload using Cloud KMS asynchronously.
  void Decrypt(AsyncContext<google::cmrt::sdk::kms_service::v1::DecryptRequest,
                            std::string>
                   decrypt_context) noexcept override;

 private:
  // Callback handler for the underlying CPIO KMS client.
  void OnDecryptCallback(
      AsyncContext<google::cmrt::sdk::kms_service::v1::DecryptRequest,
                   std::string>
          decrypt_context,
      ExecutionResultAsyncContext<
          google::cmrt::sdk::kms_service::v1::DecryptRequest,
          google::cmrt::sdk::kms_service::v1::DecryptResponse>&
          cpio_context) noexcept;

  // Maps the CPIO error code to the backend error reason for wrapped keys.
  backend::Error::Reason MapWrappedKeyFetchingError(uint64_t cpio_error_code);

  // The wrapped CPIO KMS client.
  std::shared_ptr<scp::cpio::KmsClientInterface> kms_client_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_KMS_CLIENT_KMS_CLIENT_H_
