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

#ifndef CC_CORE_DATA_TABLE_SRC_ERROR_CODES_H_
#define CC_CORE_DATA_TABLE_SRC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match {

// Registers component code as 0x0001 for concurrent data table.
REGISTER_COMPONENT_CODE(DATA_TABLE, 0x0001)

DEFINE_ERROR_CODE(DATA_TABLE_ENTRY_ALREADY_EXISTS, DATA_TABLE, 0x0001,
                  "The entry already exists in the concurrent map.",
                  google::scp::core::errors::HttpStatusCode::CONFLICT)

DEFINE_ERROR_CODE(DATA_TABLE_ENTRY_DOES_NOT_EXIST, DATA_TABLE, 0x0002,
                  "The entry does not exist.",
                  google::scp::core::errors::HttpStatusCode::NOT_FOUND)

DEFINE_ERROR_CODE(DATA_TABLE_INVALID_KEY_SIZE, DATA_TABLE, 0x0003,
                  "The size of the key is incorrect.",
                  google::scp::core::errors::HttpStatusCode::NOT_FOUND)

DEFINE_ERROR_CODE(
    VERSIONED_DATA_TABLE_NO_UPDATE_IN_PROGRESS, DATA_TABLE, 0x0101,
    "The versioned data table cannot be modified unless an "
    "update is in progress.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

DEFINE_ERROR_CODE(
    VERSIONED_DATA_TABLE_UPDATE_ALREADY_IN_PROGRESS, DATA_TABLE, 0x0102,
    "Unable to start update because another update is already in progress.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match

#endif  // CC_CORE_DATA_TABLE_SRC_ERROR_CODES_H_
