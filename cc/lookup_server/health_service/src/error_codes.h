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

#ifndef CC_LOOKUP_SERVER_HEALTH_SERVICE_SRC_ERROR_CODES_H_
#define CC_LOOKUP_SERVER_HEALTH_SERVICE_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"

namespace google::confidential_match::lookup_server {

// Registers component code as 0x0110 for health service.
REGISTER_COMPONENT_CODE(HEALTH_SERVICE, 0x0110)

DEFINE_ERROR_CODE(HEALTH_SERVICE_SERIALIZATION_ERROR, HEALTH_SERVICE, 0x0001,
                  "An error occurred while attempting to serialize data.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    HEALTH_SERVICE_STORAGE_SERVICE_ERROR, HEALTH_SERVICE, 0x0002,
    "Storage service status is not OK (likely data loading is in progress).",
    scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(HEALTH_SERVICE_MISSING_STORAGE_SERVICE, HEALTH_SERVICE,
                  0x0003,
                  "Health service is missing the match data storage service.",
                  scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_HEALTH_SERVICE_SRC_ERROR_CODES_H_
