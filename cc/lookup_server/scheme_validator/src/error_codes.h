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

#ifndef CC_LOOKUP_SERVER_SCHEME_VALIDATOR_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_SCHEME_VALIDATOR_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0111 for scheme validator.
REGISTER_COMPONENT_CODE(SCHEME_VALIDATOR, 0x0111)

DEFINE_ERROR_CODE(
    SCHEME_VALIDATOR_INVALID_REQUEST_SCHEME, SCHEME_VALIDATOR, 0x0001,
    "The request scheme is not valid for the current server sharding scheme.",
    scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(SCHEME_VALIDATOR_UNSUPPORTED_SCHEME_TYPE, SCHEME_VALIDATOR,
                  0x0002, "The sharding scheme type is not supported.",
                  scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(SCHEME_VALIDATOR_PENDING_SCHEME_NOT_SET, SCHEME_VALIDATOR,
                  0x0003,
                  "The pending scheme must be set before calling apply.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SCHEME_VALIDATOR_SRC_ERROR_CODES_H_
