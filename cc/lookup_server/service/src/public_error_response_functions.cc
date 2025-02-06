// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cc/lookup_server/service/src/public_error_response_functions.h"

#include "absl/strings/string_view.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/public/src/error_codes.h"
#include "cc/lookup_server/service/src/error_codes.h"
#include "protos/shared/api/errors/code.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::shared::api_errors::Code;
using ::google::confidential_match::shared::api_errors::Details;
using ::google::confidential_match::shared::api_errors::ErrorResponse;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::errors::GetErrorHttpStatusCode;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::GetPublicErrorCode;
using ::google::scp::core::errors::HttpStatusCode;

// The error domain to use within the error details.
constexpr absl::string_view kErrorDomain = "LookupService";

}  // namespace

ExecutionResultOr<ErrorResponse> BuildPublicErrorResponse(
    const ExecutionResult& result) noexcept {
  ErrorResponse error_response;

  uint64_t public_status_code = GetPublicErrorCode(result.status_code);
  if (public_status_code == SC_OK) {
    // Shouldn't happen, result must not be successful if calling this method
    return FailureExecutionResult(LOOKUP_SERVICE_INTERNAL_ERROR);
  } else if (public_status_code == result.status_code) {
    // The same error code is returned if no public status code was found.
    // Use a generic public error as a base starting point
    public_status_code = PUBLIC_GENERIC_ERROR;
  }
  // Use the public-facing error messages in the response
  error_response.set_message(GetErrorMessage(public_status_code));

  // Prefer public error codes if one is there, else fall back to original code
  uint64_t error_reason_code = public_status_code != PUBLIC_GENERIC_ERROR
                                   ? public_status_code
                                   : result.status_code;
  Details* error_details = error_response.add_details();
  *error_details->mutable_reason() = std::to_string(error_reason_code);
  *error_details->mutable_domain() = kErrorDomain;

  HttpStatusCode http_status_code = GetErrorHttpStatusCode(error_reason_code);
  if (http_status_code == HttpStatusCode::BAD_REQUEST) {
    error_response.set_code(Code::INVALID_ARGUMENT);
  } else if (http_status_code == HttpStatusCode::UNAUTHORIZED) {
    error_response.set_code(Code::UNAUTHENTICATED);
  } else if (http_status_code == HttpStatusCode::FORBIDDEN) {
    error_response.set_code(Code::PERMISSION_DENIED);
  } else {
    error_response.set_code(Code::UNKNOWN);
  }

  return error_response;
}

}  // namespace google::confidential_match::lookup_server
