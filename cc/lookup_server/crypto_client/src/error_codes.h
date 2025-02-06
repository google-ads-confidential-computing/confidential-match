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

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

#include "cc/lookup_server/public/src/error_codes.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0108 for crypto client.
REGISTER_COMPONENT_CODE(CRYPTO_CLIENT, 0x0108)

DEFINE_ERROR_CODE(AEAD_CONFIG_REGISTER_ERROR, CRYPTO_CLIENT, 0x0001,
                  "Crypto client failed to register Aead config.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(CRYPTO_CLIENT_KEYSET_READ_ERROR, CRYPTO_CLIENT, 0x0002,
                  "Crypto client failed to read keyset.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(CRYPTO_CLIENT_GET_AEAD_ERROR, CRYPTO_CLIENT, 0x0003,
                  "Crypto client failed to get Aead from Keyset.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(CRYPTO_CLIENT_ENCRYPT_ERROR, CRYPTO_CLIENT, 0x0004,
                  "Crypto client failed to encrypt data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(CRYPTO_CLIENT_DECRYPT_ERROR, CRYPTO_CLIENT, 0x0005,
                  "Crypto client failed to decrypt data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(CRYPTO_CLIENT_NOT_RUNNING_ERROR, CRYPTO_CLIENT, 0x0006,
                  "Crypto client is not running, so methods cannot be used.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

MAP_TO_PUBLIC_ERROR_CODE(CRYPTO_CLIENT_KEYSET_READ_ERROR, PUBLIC_CRYPTO_ERROR);
MAP_TO_PUBLIC_ERROR_CODE(CRYPTO_CLIENT_GET_AEAD_ERROR, PUBLIC_CRYPTO_ERROR);
MAP_TO_PUBLIC_ERROR_CODE(CRYPTO_CLIENT_ENCRYPT_ERROR, PUBLIC_CRYPTO_ERROR);
MAP_TO_PUBLIC_ERROR_CODE(CRYPTO_CLIENT_DECRYPT_ERROR, PUBLIC_CRYPTO_ERROR);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC__ERROR_CODES_H_
