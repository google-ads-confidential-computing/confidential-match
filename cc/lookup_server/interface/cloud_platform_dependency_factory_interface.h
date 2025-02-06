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

#ifndef CC_LOOKUP_SERVER_INTERFACE_CLOUD_PLATFORM_DEPENDENCY_FACTORY_INTERFACE_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_INTERFACE_CLOUD_PLATFORM_DEPENDENCY_FACTORY_INTERFACE_H_  // NOLINT(whitespace/line_length)

#include <memory>

#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/authorization_proxy_interface.h"
#include "cc/core/interface/http_client_interface.h"
#include "cc/core/interface/initializable_interface.h"
#include "cc/core/interface/service_interface.h"

#include "cc/lookup_server/interface/jwt_validator_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Platform specific factory interface to provide platform specific
 * clients to lookup server.
 */
class CloudPlatformDependencyFactoryInterface
    : public scp::core::InitializableInterface {
 public:
  virtual ~CloudPlatformDependencyFactoryInterface() = default;

  /**
   * @brief Construct a client for lookup server for authorization.
   */
  virtual std::unique_ptr<scp::core::AuthorizationProxyInterface>
  ConstructAuthorizationProxyClient(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<scp::core::HttpClientInterface> http_client,
      std::shared_ptr<JwtValidatorInterface> jwt_validator) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_INTERFACE_CLOUD_PLATFORM_DEPENDENCY_FACTORY_INTERFACE_H_
