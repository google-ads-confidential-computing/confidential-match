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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0102 for match data provider.
REGISTER_COMPONENT_CODE(MATCH_DATA_PROVIDER, 0x0102)

DEFINE_ERROR_CODE(MATCH_DATA_PROVIDER_FETCH_ERROR, MATCH_DATA_PROVIDER, 0x0001,
                  "Failed to fetch requested data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_PROVIDER_INVALID_ARGUMENT, MATCH_DATA_PROVIDER,
                  0x0002, "The provided arguments are not valid.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_PROVIDER_PARSE_ERROR, MATCH_DATA_PROVIDER, 0x0003,
                  "Unable to parse stream data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_PROVIDER_LIST_ERROR, MATCH_DATA_PROVIDER, 0x0004,
                  "Unable to list match data fragments from provided location.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(MATCH_DATA_PROVIDER_DEQUEUE_ERROR, MATCH_DATA_PROVIDER,
                  0x0005, "Unable to dequeue match data batch from substream.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    MATCH_DATA_PROVIDER_QUEUE_COMPLETED_ERROR, MATCH_DATA_PROVIDER, 0x0006,
    "Unable to dequeue because the operation is completed (no items remain).",
    scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_ERROR_CODES_H_
