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

#ifndef CC_LOOKUP_SERVER_INTERFACE_JWT_VALIDATOR_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_JWT_VALIDATOR_INTERFACE_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface for a JWT validator. */
class JwtValidatorInterface {
 public:
  virtual ~JwtValidatorInterface() = default;

  /**
   * @brief Validates a JSON web token (JWT).
   *
   * @param jwk_set the keyset in JWK format
   * @param token the JWT to be validated
   * @return a SuccessExecutionResult if the token is authorized, or a
   * FailureExecutionResult on error
   */
  virtual scp::core::ExecutionResult Validate(
      absl::string_view jwk_set, absl::string_view token) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_JWT_VALIDATOR_INTERFACE_H_
