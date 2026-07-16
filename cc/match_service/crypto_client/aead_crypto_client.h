/*
 * Copyright 2026 Google LLC
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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_AEAD_CRYPTO_CLIENT_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_AEAD_CRYPTO_CLIENT_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "protos/core/encryption_key_info.pb.h"

namespace google::confidential_match::match_service {

// Provides a CryptoKey object used for encryption and decryption. */
class AeadCryptoClient : public CryptoClientInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the AeadCryptoClient object.
  explicit AeadCryptoClient(
      KmsClientInterface* aws_kms_client, KmsClientInterface* gcp_kms_client,
      const std::vector<std::string>& kms_default_signatures,
      absl::string_view aws_kms_default_audience)
      : aws_kms_client_(*aws_kms_client),
        gcp_kms_client_(*gcp_kms_client),
        kms_default_signatures_(kms_default_signatures),
        aws_kms_default_audience_(aws_kms_default_audience) {}

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  void GetCryptoKey(AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
                        key_context) noexcept override;

 private:
  // Helper to build a request for decrypting a wrapped KMS key.
  google::cmrt::sdk::kms_service::v1::DecryptRequest BuildKmsDecryptRequest(
      const EncryptionKeyInfo& encryption_key_info) noexcept;

  // Callback to handle the response from the KMS client.
  void OnDecryptWrappedKmsKeyCallback(
      AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> key_context,
      AsyncContext<google::cmrt::sdk::kms_service::v1::DecryptRequest,
                   std::string>
          kms_decrypt_context) noexcept;

  // AWS KMS client.
  KmsClientInterface& aws_kms_client_;
  // GCP KMS client.
  KmsClientInterface& gcp_kms_client_;
  // A list of signatures to send with AWS KMS requests.
  const std::vector<std::string> kms_default_signatures_;
  // The default audience to send with AWS KMS requests.
  std::string aws_kms_default_audience_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_AEAD_CRYPTO_CLIENT_H_
