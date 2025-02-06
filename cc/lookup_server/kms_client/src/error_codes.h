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

#ifndef CC_LOOKUP_SERVER_KMS_CLIENT_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_KMS_CLIENT_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

#include "cc/lookup_server/public/src/error_codes.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0114 for KMS client.
REGISTER_COMPONENT_CODE(KMS_CLIENT, 0x0114)

DEFINE_ERROR_CODE(KMS_CLIENT_DECRYPTION_ERROR, KMS_CLIENT, 0x0001,
                  "Failed to decrypt the payload.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(KMS_CLIENT_CACHE_ENTRY_NOT_FOUND, KMS_CLIENT, 0x0003,
                  "Failed to find the entry within the cache.",
                  scp::core::errors::HttpStatusCode::NOT_FOUND)

DEFINE_ERROR_CODE(KMS_CLIENT_CACHE_ENTRY_FETCH_ERROR, KMS_CLIENT, 0x0004,
                  "The entry could not be retrieved from the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(KMS_CLIENT_CACHE_ENTRY_INSERT_ERROR, KMS_CLIENT, 0x0005,
                  "The entry could not be inserted into the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(KMS_CLIENT_CACHE_ENTRY_ALREADY_EXISTS, KMS_CLIENT, 0x0006,
                  "The entry already exists within the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(KMS_CLIENT_CACHE_ENTRY_BEING_DELETED, KMS_CLIENT, 0x0007,
                  "The entry is currently being deleted from the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

MAP_TO_PUBLIC_ERROR_CODE(KMS_CLIENT_DECRYPTION_ERROR, PUBLIC_CRYPTO_ERROR);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_KMS_CLIENT_SRC_ERROR_CODES_H_
