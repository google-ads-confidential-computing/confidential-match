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

#ifndef CC_LOOKUP_SERVER_PUBLIC_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_PUBLIC_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0FFF for public error codes.
REGISTER_COMPONENT_CODE(PUBLIC, 0x0FFF)

DEFINE_ERROR_CODE(PUBLIC_GENERIC_ERROR, PUBLIC, 0x0001,
                  "An error occurred while attempting to process the request.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PUBLIC_INVALID_ARGUMENT_ERROR, PUBLIC, 0x0002,
                  "The request contains invalid arguments.",
                  scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(PUBLIC_INVALID_SHARDING_SCHEME_ERROR, PUBLIC, 0x0003,
                  "The provided sharding scheme is invalid.",
                  scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(
    PUBLIC_CRYPTO_ERROR, PUBLIC, 0x0004,
    "An encryption/decryption error occurred while processing the request.",
    scp::core::errors::HttpStatusCode::BAD_REQUEST)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_PUBLIC_SRC_ERROR_CODES_H_
