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

#include "cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp/gcp_dependency_factory.h"

#include <memory>

#include "cc/core/authorization_proxy/src/authorization_proxy.h"
#include "cc/core/authorization_proxy/src/pass_thru_authorization_proxy.h"
#include "public/core/interface/execution_result.h"

#include "cc/lookup_server/auth/src/gcp/gcp_http_request_response_auth_interceptor.h"
#include "cc/lookup_server/interface/jwt_validator_interface.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::AuthorizationProxy;
using ::google::scp::core::AuthorizationProxyInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::HttpClientInterface;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr char kGcpDependencyProvider[] = "GcpDependencyProvider";
// The endpoint providing the JSON Web Keyset used to verify Google JWTs.
constexpr char kAuthServiceEndpoint[] =
    "https://www.googleapis.com/oauth2/v3/certs";

}  // namespace

ExecutionResult GcpDependencyFactory::Init() noexcept {
  SCP_INFO(kGcpDependencyProvider, kZeroUuid,
           "Initializing GCP dependency factory");
  return SuccessExecutionResult();
}

std::unique_ptr<AuthorizationProxyInterface>
GcpDependencyFactory::ConstructAuthorizationProxyClient(
    std::shared_ptr<AsyncExecutorInterface> async_executor,
    std::shared_ptr<HttpClientInterface> http_client,
    std::shared_ptr<JwtValidatorInterface> jwt_validator) noexcept {
  return std::make_unique<AuthorizationProxy>(
      kAuthServiceEndpoint, async_executor, http_client,
      std::make_unique<GcpHttpRequestResponseAuthInterceptor>(jwt_validator));
}

}  // namespace google::confidential_match::lookup_server
