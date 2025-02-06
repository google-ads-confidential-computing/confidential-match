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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_LOADER_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_LOADER_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0105 for match data loader.
REGISTER_COMPONENT_CODE(MATCH_DATA_LOADER, 0x0105)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_ALREADY_RUNNING, MATCH_DATA_LOADER, 0x0001,
                  "Match Data Loader is already running.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_ALREADY_STOPPED, MATCH_DATA_LOADER, 0x0002,
                  "Match Data Loader is already stopped.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_INVALID_PATH, MATCH_DATA_LOADER, 0x0003,
                  "Failed to load the match data from the provided path.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_INVALID_BASE64_DATA, MATCH_DATA_LOADER,
                  0x0004, "Unable to parse match data (invalid base-64).",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_INVALID_PROTO_DATA, MATCH_DATA_LOADER,
                  0x0005, "Unable to parse match data (invalid proto bytes).",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_DEQUEUE_ERROR, MATCH_DATA_LOADER, 0x0006,
                  "Unable to dequeue match data from stream.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_LOADER_INVALID_ENCRYPTED_DEK, MATCH_DATA_LOADER,
                  0x0007, "Unable to parse encrypted DEK into keyset.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_MATCH_DATA_LOADER_SRC_ERROR_CODES_H_
