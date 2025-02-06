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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match::lookup_server {

// Register component code as 0x0101 for Lookup Service
REGISTER_COMPONENT_CODE(LOOKUP_SERVICE, 0x0101)

DEFINE_ERROR_CODE(LOOKUP_SERVICE_INVALID_REQUEST, LOOKUP_SERVICE, 0x0001,
                  "The request is invalid.",
                  google::scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(
    LOOKUP_SERVICE_INTERNAL_ERROR, LOOKUP_SERVICE, 0x0002,
    "An internal error occurred.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(LOOKUP_SERVICE_INVALID_REQUEST_SCHEME, LOOKUP_SERVICE, 0x0003,
                  "The request scheme is not valid (likely out of date).",
                  google::scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(LOOKUP_SERVICE_INVALID_KEY_FORMAT, LOOKUP_SERVICE, 0x0004,
                  "The key format is not valid (likely unspecified).",
                  google::scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(LOOKUP_SERVICE_HEADER_NOT_FOUND, LOOKUP_SERVICE, 0x0005,
                  "The header was not found.",
                  google::scp::core::errors::HttpStatusCode::NOT_FOUND)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_ERROR_CODES_H_
