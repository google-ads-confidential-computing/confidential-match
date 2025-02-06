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

#ifndef CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

#include "cc/lookup_server/public/src/error_codes.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0115 for coordinator client.
REGISTER_COMPONENT_CODE(COORDINATOR_CLIENT, 0x0115)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_KEY_FETCH_ERROR, COORDINATOR_CLIENT,
                  0x0001,
                  "Failed to fetch the key from the provided coordinator(s).",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_KEY_NOT_FOUND_ERROR, COORDINATOR_CLIENT,
                  0x0002, "Failed to find a key with the provided key ID.",
                  scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_CACHE_ENTRY_NOT_FOUND, COORDINATOR_CLIENT,
                  0x0003, "Failed to find the entry within the cache.",
                  scp::core::errors::HttpStatusCode::NOT_FOUND)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_CACHE_ENTRY_FETCH_ERROR,
                  COORDINATOR_CLIENT, 0x0004,
                  "The entry could not be retrieved from the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_CACHE_ENTRY_INSERT_ERROR,
                  COORDINATOR_CLIENT, 0x0005,
                  "The entry could not be inserted into the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_CACHE_ENTRY_ALREADY_EXISTS,
                  COORDINATOR_CLIENT, 0x0006,
                  "The entry already exists within the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_CACHE_ENTRY_BEING_DELETED,
                  COORDINATOR_CLIENT, 0x0007,
                  "The entry is currently being deleted from the cache.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(COORDINATOR_CLIENT_CACHE_DESERIALIZATION_ERROR,
                  COORDINATOR_CLIENT, 0x0008,
                  "Failed to deserialize a cached coordinator response.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    COORDINATOR_CLIENT_MISSING_PARAMETERS_ERROR, COORDINATOR_CLIENT, 0x0009,
    "One or more required parameters are missing from the request.",
    scp::core::errors::HttpStatusCode::BAD_REQUEST)

MAP_TO_PUBLIC_ERROR_CODE(COORDINATOR_CLIENT_KEY_FETCH_ERROR,
                         PUBLIC_CRYPTO_ERROR);
MAP_TO_PUBLIC_ERROR_CODE(COORDINATOR_CLIENT_KEY_NOT_FOUND_ERROR,
                         PUBLIC_CRYPTO_ERROR);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_COORDINATOR_CLIENT_SRC_ERROR_CODES_H_
