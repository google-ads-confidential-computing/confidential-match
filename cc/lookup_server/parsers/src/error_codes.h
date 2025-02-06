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

#ifndef CC_LOOKUP_SERVER_PARSERS_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_PARSERS_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0106 for parsers.
REGISTER_COMPONENT_CODE(PARSER, 0x0106)

DEFINE_ERROR_CODE(
    PARSER_CANNOT_ENQUEUE, PARSER, 0x0001,
    "Unable to enqueue more data, stream finished or reached size limit.",
    scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PARSER_CANNOT_DEQUEUE, PARSER, 0x0002,
                  "Encountered issue while dequeueing data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    PARSER_STREAM_FINISHED, PARSER, 0x0003,
    "Unable to perform the operation because the stream has finished.",
    scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PARSER_INVALID_BASE64_DATA, PARSER, 0x0004,
                  "Unable to parse match data (invalid base-64).",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PARSER_INVALID_PROTO_DATA, PARSER, 0x0005,
                  "Unable to parse match data (invalid proto bytes).",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PARSER_INVALID_EXPORT_METADATA, PARSER, 0x0006,
                  "Unable to parse raw export metadata.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PARSER_INVALID_EXPORTED_DATA_ROW, PARSER, 0x0007,
                  "Unable to convert Exporter Data Row to Match Data Row.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(PARSER_DATA_DECRYPTION_FAILED, PARSER, 0x0008,
                  "Unable to parse match data (decryption failed).",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_PARSERS_SRC_ERROR_CODES_H_
