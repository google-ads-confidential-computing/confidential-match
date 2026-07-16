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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_CRYPTO_CLIENT_INTERFACE_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_CRYPTO_CLIENT_INTERFACE_H_

#include "absl/status/status.h"

#include "cc/core/async/async_context.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "protos/core/encryption_key_info.pb.h"

namespace google::confidential_match::match_service {

// Interface for a cryptography client. */
class CryptoClientInterface {
 public:
  virtual ~CryptoClientInterface() = default;

  virtual absl::Status Init() noexcept = 0;
  virtual absl::Status Run() noexcept = 0;
  virtual absl::Status Stop() noexcept = 0;

  // Gets a crypto key using information in EncryptionKeyInfo.
  virtual void GetCryptoKey(AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
                                key_context) noexcept = 0;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_CRYPTO_CLIENT_INTERFACE_H_
