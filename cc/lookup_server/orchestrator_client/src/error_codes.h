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

#ifndef CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0107 for orchestrator client.
REGISTER_COMPONENT_CODE(ORCHESTRATOR_CLIENT, 0x0107)

DEFINE_ERROR_CODE(ORCHESTRATOR_CLIENT_PARSE_ERROR, ORCHESTRATOR_CLIENT, 0x0001,
                  "Failed to parse response from the Orchestrator.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_SRC_ERROR_CODES_H_
