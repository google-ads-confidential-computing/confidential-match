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

#ifndef CC_LOOKUP_SERVER_SERVER_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_SERVER_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0100 for server.
REGISTER_COMPONENT_CODE(LOOKUP_SERVER, 0x0100)

DEFINE_ERROR_CODE(
    LOOKUP_SERVER_ALREADY_RUNNING, LOOKUP_SERVER, 0x0001,
    "Lookup Server is already running.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    LOOKUP_SERVER_NOT_RUNNING, LOOKUP_SERVER, 0x0002,
    "Lookup Server is not currently running.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    INVALID_HTTP2_SERVER_PRIVATE_KEY_FILE_PATH, LOOKUP_SERVER, 0x0003,
    "Invalid private key file path.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    INVALID_HTTP2_SERVER_CERT_FILE_PATH, LOOKUP_SERVER, 0x0004,
    "Invalid cert file path.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVER_SRC_ERROR_CODES_H_
