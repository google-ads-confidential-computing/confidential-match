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

#include "cc/lookup_server/auth/src/gcp/gcp_http_request_response_auth_interceptor.h"

#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_types.h"

#include "cc/lookup_server/auth/src/jwt_validator.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::AuthorizationMetadata;
using ::google::scp::core::AuthorizedDomain;
using ::google::scp::core::AuthorizedMetadata;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;

constexpr absl::string_view kComponentName = "GcpAuthInterceptor";

}  // namespace

ExecutionResult GcpHttpRequestResponseAuthInterceptor::PrepareRequest(
    const AuthorizationMetadata& authorization_metadata,
    HttpRequest& http_request) {
  http_request.method = HttpMethod::GET;
  http_request.query = std::make_shared<std::string>("");
  return SuccessExecutionResult();
}

ExecutionResultOr<AuthorizedMetadata>
GcpHttpRequestResponseAuthInterceptor::ObtainAuthorizedMetadataFromResponse(
    const AuthorizationMetadata& authorization_metadata,
    const HttpResponse& http_response) {
  const std::string jwk_set = http_response.body.ToString();

  ExecutionResult auth_result = jwt_validator_->Validate(
      jwk_set, authorization_metadata.authorization_token);
  if (!auth_result.Successful()) {
    SCP_INFO(
        kComponentName, kZeroUuid,
        absl::StrCat("Incoming request not authorized to call Lookup Server: ",
                     GetErrorMessage(auth_result.status_code)));
    return auth_result;
  }

  return AuthorizedMetadata{.authorized_domain =
                                std::make_shared<AuthorizedDomain>("")};
}

}  // namespace google::confidential_match::lookup_server
