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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0103 for match data storage.
REGISTER_COMPONENT_CODE(MATCH_DATA_STORAGE, 0x0103)

DEFINE_ERROR_CODE(MATCH_DATA_STORAGE_FETCH_ERROR, MATCH_DATA_STORAGE, 0x0001,
                  "Failed to fetch the match data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_STORAGE_INSERT_ERROR, MATCH_DATA_STORAGE, 0x0002,
                  "Failed to insert the match data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_STORAGE_REPLACE_ERROR, MATCH_DATA_STORAGE, 0x0003,
                  "Failed to replace the match data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_STORAGE_REMOVE_ERROR, MATCH_DATA_STORAGE, 0x0004,
                  "Failed to remove the match data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_STORAGE_INVALID_KEY_ERROR, MATCH_DATA_STORAGE,
                  0x0005, "Failed to parse the match data key.",
                  scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(
    MATCH_DATA_STORAGE_REPLACE_KEY_MISMATCH, MATCH_DATA_STORAGE, 0x0006,
    "Match data rows must match the provided key during replacement.",
    scp::core::errors::HttpStatusCode::BAD_REQUEST)

DEFINE_ERROR_CODE(
    MATCH_DATA_STORAGE_UPDATE_ALREADY_IN_PROGRESS, MATCH_DATA_STORAGE, 0x0007,
    "Failed to start update since another one is currently in progress.",
    scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_SRC_ERROR_CODES_H_
