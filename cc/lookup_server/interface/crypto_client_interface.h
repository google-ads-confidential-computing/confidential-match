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

#ifndef CC_LOOKUP_SERVER_INTERFACE_CRYPTO_CLIENT_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_CRYPTO_CLIENT_INTERFACE_H_

#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface for a cryptography client. */
class CryptoClientInterface : public scp::core::ServiceInterface {
 public:
  virtual ~CryptoClientInterface() = default;

  /**
   * @brief Gets a crypto key using information in EncryptionKeyInfo.
   *
   * @param key_context the context containing the requested encryption key info
   */
  virtual void GetCryptoKey(
      scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                              CryptoKeyInterface>
          key_context) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_CRYPTO_CLIENT_INTERFACE_H_
