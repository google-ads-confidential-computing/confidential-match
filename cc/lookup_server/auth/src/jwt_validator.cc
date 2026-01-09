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

#include "cc/lookup_server/auth/src/jwt_validator.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"
#include "tink/jwt/jwk_set_converter.h"
#include "tink/jwt/jwt_public_key_verify.h"
#include "tink/jwt/jwt_signature_config.h"
#include "tink/jwt/jwt_validator.h"
#include "tink/jwt/verified_jwt.h"
#include "tink/keyset_handle.h"

#include "cc/lookup_server/auth/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::crypto::tink::JwkSetToPublicKeysetHandle;
using ::crypto::tink::JwtPublicKeyVerify;
using ::crypto::tink::JwtSignatureRegister;
using ::crypto::tink::JwtValidatorBuilder;
using ::crypto::tink::KeysetHandle;
using ::crypto::tink::RawJwtBuilder;
using ::crypto::tink::VerifiedJwt;
using ::crypto::tink::util::Status;
using ::crypto::tink::util::StatusOr;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "JwtValidator";
// The expected value for the "type" field within a JWT.
constexpr absl::string_view kJwtType = "JWT";
// The key used for the email field within the JWT.
constexpr absl::string_view kEmailField = "email";
// The permitted range in clock skew when validating a token.
static constexpr absl::Duration kJwtClockSkew = absl::Minutes(5);

}  // namespace

scp::core::ExecutionResultOr<std::unique_ptr<JwtValidator>>
JwtValidator::Create(
    absl::string_view issuer, absl::string_view audience,
    const std::vector<std::string>& allowed_emails,
    const std::vector<std::string>& allowed_subjects) noexcept {
  // Register the module exactly once when the first object is first constructed
  static const Status register_status = JwtSignatureRegister();
  if (!register_status.ok()) {
    auto failure_result = FailureExecutionResult(AUTH_INITIALIZATION_ERROR);
    SCP_ERROR(
        kComponentName, kZeroUuid, failure_result,
        absl::StrCat("Failed to register the Tink JWT signature primitive: ",
                     register_status.ToString()));
    return failure_result;
  }

  return std::unique_ptr<JwtValidator>(
      new JwtValidator(issuer, audience, allowed_emails, allowed_subjects));
}

ExecutionResult JwtValidator::Validate(absl::string_view jwk_set,
                                       absl::string_view token) noexcept {
  StatusOr<std::unique_ptr<KeysetHandle>> keyset_handle_or =
      JwkSetToPublicKeysetHandle(jwk_set);
  if (!keyset_handle_or.ok()) {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrCat("Failed to parse the JWK into a keyset handle: ",
                          keyset_handle_or.status().ToString()));
    return FailureExecutionResult(AUTH_INVALID_JWK);
  }

  StatusOr<::crypto::tink::JwtValidator> jwt_validator_or =
      JwtValidatorBuilder()
          .ExpectTypeHeader(kJwtType)
          .ExpectIssuer(issuer_)
          .ExpectAudience(audience_)
          .SetClockSkew(kJwtClockSkew)
          .ExpectIssuedInThePast()
          .Build();
  if (!jwt_validator_or.ok()) {
    auto failure_result = FailureExecutionResult(AUTH_INITIALIZATION_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, failure_result,
              absl::StrCat("Failed to initialize the JWT validator: ",
                           jwt_validator_or.status().ToString()));
    return failure_result;
  }

  StatusOr<std::unique_ptr<JwtPublicKeyVerify>> jwt_verifier_or =
      (*keyset_handle_or)->GetPrimitive<JwtPublicKeyVerify>();
  if (!jwt_verifier_or.ok()) {
    auto failure_result = FailureExecutionResult(AUTH_INITIALIZATION_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, failure_result,
              absl::StrCat("Failed to initialize the JWT public key verifier: ",
                           jwt_verifier_or.status().ToString()));
    return failure_result;
  }

  StatusOr<VerifiedJwt> verified_jwt_or =
      (*jwt_verifier_or)->VerifyAndDecode(token, *jwt_validator_or);
  if (!verified_jwt_or.ok()) {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrCat("Failed to validate JWT: ",
                          verified_jwt_or.status().ToString()));
    return FailureExecutionResult(AUTH_INVALID_JWT);
  }

  return AuthorizeClaims(*verified_jwt_or);
}

// Checks whether the subject or email within a verified JWT is authorized to
// send a request to Lookup Server.
ExecutionResult JwtValidator::AuthorizeClaims(
    VerifiedJwt verified_jwt) noexcept {
  StatusOr<std::string> email_or = verified_jwt.GetStringClaim(kEmailField);
  if (email_or.ok() && allowed_emails_.contains(*email_or)) {
    SCP_DEBUG(
        kComponentName, kZeroUuid,
        absl::StrFormat("Incoming request authorized. (Email: %s)", *email_or));
    return SuccessExecutionResult();
  }

  StatusOr<std::string> subject_or = verified_jwt.GetSubject();
  if (subject_or.ok() && allowed_subjects_.contains(*subject_or)) {
    SCP_DEBUG(kComponentName, kZeroUuid,
              absl::StrFormat("Incoming request authorized. (Subject: %s)",
                              *subject_or));
    return SuccessExecutionResult();
  }

  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat(
               "JWT not authorized to send requests. (email: %s, subject: %s)",
               email_or.ok() ? *email_or : "None",
               subject_or.ok() ? *subject_or : "None"));
  return FailureExecutionResult(AUTH_JWT_NOT_AUTHORIZED);
}

}  // namespace google::confidential_match::lookup_server
