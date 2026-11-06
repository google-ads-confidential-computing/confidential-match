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

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_CPIO_AEAD_CRYPTO_CLIENT_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_CPIO_AEAD_CRYPTO_CLIENT_H_

#include <memory>
#include <utility>

#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/utils/key_fetching/proto/cloud_wrapped_key.pb.h"
#include "cc/public/cpio/utils/key_fetching/src/wrapped_key_handler_with_cache_interface.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Provides a CryptoKey object used for encryption and decryption using
 * CPIO's WrappedKeyHandlerWithCacheInterface.
 */
class CpioAeadCryptoClient : public CryptoClientInterface {
 public:
  explicit CpioAeadCryptoClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<scp::cpio::WrappedKeyHandlerWithCacheInterface>
          aws_wrapped_key_handler,
      std::shared_ptr<scp::cpio::WrappedKeyHandlerWithCacheInterface>
          gcp_wrapped_key_handler)
      : async_executor_(std::move(async_executor)),
        aws_wrapped_key_handler_(std::move(aws_wrapped_key_handler)),
        gcp_wrapped_key_handler_(std::move(gcp_wrapped_key_handler)) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  void GetCryptoKey(scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                                            CryptoKeyInterface>
                        key_context) noexcept override;

 private:
  void GetCryptoKeyAsync(
      scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                              CryptoKeyInterface>
          key_context) noexcept;

  google::cmrt::sdk::v1::CloudWrappedKey BuildCloudWrappedKey(
      const proto_backend::EncryptionKeyInfo& encryption_key_info) noexcept;

  // An instance of the async executor.
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  // An instance of the AWS wrapped key handler with cache.
  std::shared_ptr<scp::cpio::WrappedKeyHandlerWithCacheInterface>
      aws_wrapped_key_handler_;
  // An instance of the GCP wrapped key handler with cache.
  std::shared_ptr<scp::cpio::WrappedKeyHandlerWithCacheInterface>
      gcp_wrapped_key_handler_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_CPIO_AEAD_CRYPTO_CLIENT_H_

