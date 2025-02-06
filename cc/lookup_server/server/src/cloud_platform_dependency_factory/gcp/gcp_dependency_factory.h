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

#ifndef CC_LOOKUP_SERVER_SERVER_SRC_CLOUD_PLATFORM_DEPENDENCY_FACTORY_GCP_GCP_DEPENDENCY_FACTORY_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_SERVER_SRC_CLOUD_PLATFORM_DEPENDENCY_FACTORY_GCP_GCP_DEPENDENCY_FACTORY_H_  // NOLINT(whitespace/line_length)

#include <memory>
#include <string>

#include "cc/core/interface/config_provider_interface.h"
#include "public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/cloud_platform_dependency_factory_interface.h"
#include "cc/lookup_server/interface/jwt_validator_interface.h"

namespace google::confidential_match::lookup_server {

// Factory to provide GCP dependent implementations.
class GcpDependencyFactory : public CloudPlatformDependencyFactoryInterface {
 public:
  GcpDependencyFactory(
      std::shared_ptr<scp::core::ConfigProviderInterface> config_provider)
      : config_provider_(config_provider) {}

  scp::core::ExecutionResult Init() noexcept override;

  std::unique_ptr<scp::core::AuthorizationProxyInterface>
  ConstructAuthorizationProxyClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<scp::core::HttpClientInterface> http_client,
      std::shared_ptr<JwtValidatorInterface> jwt_validator) noexcept override;

 private:
  std::shared_ptr<scp::core::ConfigProviderInterface> config_provider_;
};

}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_SERVER_SRC_CLOUD_PLATFORM_DEPENDENCY_FACTORY_GCP_GCP_DEPENDENCY_FACTORY_H_
