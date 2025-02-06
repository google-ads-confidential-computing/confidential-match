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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_PUBLIC_ERROR_RESPONSE_FUNCTIONS_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_PUBLIC_ERROR_RESPONSE_FUNCTIONS_H_

#include "cc/public/core/interface/execution_result.h"

#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * Generates a public-facing ErrorResponse from a failed ExecutionResult, or
 * an error if an ErrorResponse cannot be generated for the given result.
 *
 * @param result the failed ExecutionResult used to generate a response
 * @return a public-facing ErrorResponse, or a failure result on error
 */
scp::core::ExecutionResultOr<shared::api_errors::ErrorResponse>
BuildPublicErrorResponse(const scp::core::ExecutionResult& result) noexcept;

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_PUBLIC_ERROR_RESPONSE_FUNCTIONS_H_
