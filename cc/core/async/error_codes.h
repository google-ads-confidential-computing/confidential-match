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

#ifndef CC_CORE_ASYNC_ERROR_CODES_H_
#define CC_CORE_ASYNC_ERROR_CODES_H_

#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match {

// Registers component code as 0x0200 for async library.
REGISTER_COMPONENT_CODE(ASYNC, 0x0200)

DEFINE_ERROR_CODE(
    ASYNC_INTERNAL_ERROR, ASYNC, 0x0001, "An internal error occurred.",
    google::scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR)

}  // namespace google::confidential_match

#endif  // CC_CORE_ASYNC_ERROR_CODES_H_
