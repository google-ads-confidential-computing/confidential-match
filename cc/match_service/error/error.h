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

#ifndef CC_MATCH_SERVICE_ERROR_ERROR_H_
#define CC_MATCH_SERVICE_ERROR_ERROR_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "google/rpc/status.pb.h"
#include "grpcpp/grpcpp.h"

#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

// Creates a backend absl::Status with an attached backend ErrorReason payload.
absl::Status Status(backend::Error::Reason backend_error_reason,
                    absl::string_view backend_message) noexcept;

// Gets the backend ErrorReason payload from `status` if it exists.
std::optional<backend::Error::Reason> GetBackendErrorReason(
    const absl::Status& status) noexcept;

std::string GetBackendErrorReasonString(const absl::Status& status) noexcept;

// Gets the error reason string from `status` if it exists.
std::optional<std::string> GetErrorReason(const grpc::Status& status) noexcept;

// Annotates an existing status, returning a new status with the added message.
absl::Status Annotate(const absl::Status& status,
                      absl::string_view message) noexcept;

// Creates a grpc::Status with a serialized google.rpc.Status proto. The Status
// proto's details field contains a google.rpc.ErrorInfo proto holding
// `error_reason`. This function is intended to be used by backend-to-API status
// converters, rather than used directly.
grpc::Status GrpcStatus(grpc::StatusCode code, absl::string_view message,
                        absl::string_view error_reason) noexcept;

// Creates a google.rpc.Status proto with a details field containing a
// google.rpc.ErrorInfo proto holding `error_reason`.
rpc::Status RpcStatus(rpc::Code code, absl::string_view message,
                      absl::string_view error_reason) noexcept;

// Converts a backend status to an API status.
grpc::Status ToApiStatus(const absl::Status& status) noexcept;

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_ERROR_ERROR_H_
