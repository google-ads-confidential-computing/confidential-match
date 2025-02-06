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

#ifndef CC_LOOKUP_SERVER_AUTH_SRC_JWT_VALIDATOR_H_
#define CC_LOOKUP_SERVER_AUTH_SRC_JWT_VALIDATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "tink/jwt/verified_jwt.h"

#include "cc/lookup_server/interface/jwt_validator_interface.h"

namespace google::confidential_match::lookup_server {

class JwtValidator : public JwtValidatorInterface {
 public:
  JwtValidator() = delete;

  /**
   * @brief Constructs a JwtValidator.
   *
   * @param issuer the expected issuer of the JWT
   * @param audience the expected audience of the JWT
   * @param allowed_emails a list of allowed emails for the JWT
   * @param allowed_subjects a list of allowed subjects for the JWT
   * @return a unique pointer to a validator or error on initialization failure
   */
  static scp::core::ExecutionResultOr<std::unique_ptr<JwtValidator>> Create(
      absl::string_view issuer, absl::string_view audience,
      const std::vector<std::string>& allowed_emails,
      const std::vector<std::string>& allowed_subjects) noexcept;

  /**
   * @brief Validates a JSON web token (JWT).
   *
   * @param jwk_set the keyset in JWK format
   * @param token the JWT to be validated
   * @return a SuccessExecutionResult if the token is authorized, or a
   * FailureExecutionResult on error
   */
  scp::core::ExecutionResult Validate(
      absl::string_view jwk_set, absl::string_view token) noexcept override;

 private:
  JwtValidator(absl::string_view issuer, absl::string_view audience,
               const std::vector<std::string>& allowed_emails,
               const std::vector<std::string>& allowed_subjects)
      : issuer_(issuer),
        audience_(audience),
        allowed_emails_(allowed_emails.begin(), allowed_emails.end()),
        allowed_subjects_(allowed_subjects.begin(), allowed_subjects.end()) {}

  /**
   * Checks whether the subject or email within a verified JWT is authorized to
   * send a request to Lookup Server.
   *
   * @param verified_jwt a JWT whose signature has been validated
   * @return a SuccessExecutionResult if the token is authorized, or a
   * FailureExecutionResult otherwise
   */
  scp::core::ExecutionResult AuthorizeClaims(
      crypto::tink::VerifiedJwt verified_jwt) noexcept;

  const std::string issuer_;
  const std::string audience_;
  const absl::flat_hash_set<std::string> allowed_emails_;
  const absl::flat_hash_set<std::string> allowed_subjects_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_AUTH_SRC_JWT_VALIDATOR_H_
