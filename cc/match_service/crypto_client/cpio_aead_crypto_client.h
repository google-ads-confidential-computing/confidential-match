// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_CPIO_AEAD_CRYPTO_CLIENT_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_CPIO_AEAD_CRYPTO_CLIENT_H_

#include "absl/status/status.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/cpio/utils/key_fetching/src/wrapped_key_handler_with_cache_interface.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "protos/core/encryption_key_info.pb.h"

namespace google::confidential_match::match_service {

// Provides a CryptoKey object used for encryption and decryption using CPIO's
// WrappedKeyHandlerWithCacheInterface.
class CpioAeadCryptoClient : public CryptoClientInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the CpioAeadCryptoClient object.
  CpioAeadCryptoClient(
      google::scp::core::AsyncExecutorInterface* async_executor,
      google::scp::cpio::WrappedKeyHandlerWithCacheInterface*
          aws_wrapped_key_handler,
      google::scp::cpio::WrappedKeyHandlerWithCacheInterface*
          gcp_wrapped_key_handler)
      : async_executor_(*async_executor),
        aws_wrapped_key_handler_(*aws_wrapped_key_handler),
        gcp_wrapped_key_handler_(*gcp_wrapped_key_handler) {}

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  void GetCryptoKey(AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
                        key_context) noexcept override;

 private:
  // Fetches and unwraps the DEK from the appropriate wrapped key handler and
  // constructs the AEAD primitive.
  void GetCryptoKeyAsync(AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
                             key_context) noexcept;

  google::scp::core::AsyncExecutorInterface& async_executor_;
  google::scp::cpio::WrappedKeyHandlerWithCacheInterface&
      aws_wrapped_key_handler_;
  google::scp::cpio::WrappedKeyHandlerWithCacheInterface&
      gcp_wrapped_key_handler_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_CPIO_AEAD_CRYPTO_CLIENT_H_

