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

#ifndef CC_LOOKUP_SERVER_AUTH_SRC_GCP_GCP_HTTP_REQUEST_RESPONSE_AUTH_INTERCEPTOR_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_AUTH_SRC_GCP_GCP_HTTP_REQUEST_RESPONSE_AUTH_INTERCEPTOR_H_  // NOLINT(whitespace/line_length)

#include <memory>

#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/authorization_proxy_interface.h"
#include "cc/core/interface/config_provider_interface.h"
#include "cc/core/interface/http_request_response_auth_interceptor_interface.h"
#include "cc/core/interface/type_def.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/jwt_validator_interface.h"

namespace google::confidential_match::lookup_server {

class GcpHttpRequestResponseAuthInterceptor
    : public scp::core::HttpRequestResponseAuthInterceptorInterface {
 public:
  explicit GcpHttpRequestResponseAuthInterceptor(
      std::shared_ptr<JwtValidatorInterface> jwt_validator)
      : jwt_validator_(jwt_validator) {}

  scp::core::ExecutionResult PrepareRequest(
      const scp::core::AuthorizationMetadata& authorization_metadata,
      scp::core::HttpRequest& http_request) override;

  scp::core::ExecutionResultOr<scp::core::AuthorizedMetadata>
  ObtainAuthorizedMetadataFromResponse(
      const scp::core::AuthorizationMetadata& authorization_metadata,
      const scp::core::HttpResponse& http_response) override;

 private:
  std::shared_ptr<JwtValidatorInterface> jwt_validator_;
};

}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_AUTH_SRC_GCP_GCP_HTTP_REQUEST_RESPONSE_AUTH_INTERCEPTOR_H_
