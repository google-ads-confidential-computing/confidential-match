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

#ifndef CC_LOOKUP_SERVER_HEALTH_SERVICE_SRC_HEALTH_SERVICE_H_
#define CC_LOOKUP_SERVER_HEALTH_SERVICE_SRC_HEALTH_SERVICE_H_

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/time/clock.h"
#include "core/interface/async_context.h"
#include "core/interface/async_executor_interface.h"
#include "core/interface/http_server_interface.h"
#include "public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/status_provider_interface.h"

namespace google::confidential_match::lookup_server {
/**
 * @brief To provide health check functionality, a health service will return
 * success execution result to all health inquiries.
 */
class HealthService : public scp::core::ServiceInterface {
 public:
  explicit HealthService(
      std::shared_ptr<scp::core::HttpServerInterface>& http_server,
      absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
          service_status_providers)
      : http_server_(http_server),
        service_status_providers_(service_status_providers) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

 protected:
  /**
   * @brief Returns success if the instance is running after potential initial
   * delays. Meant to be used by Autoscaler Health Check.
   *
   * @param http_context The http context of the operation.
   * @return core::ExecutionResult The execution result of the operation.
   */
  scp::core::ExecutionResult CheckStartupHealth(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  /**
   * @brief Returns success if the instance is running and returns service
   * statuses. Meant to be used by Load Balancer Health Check
   *
   * @param http_context The http context of the operation.
   * @return core::ExecutionResult The execution result of the operation.
   */
  scp::core::ExecutionResult CheckHealth(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  // An instance to the http server.
  std::shared_ptr<scp::core::HttpServerInterface> http_server_;
  // Mapping of service_name -> service_status_provider, which provides the
  // current status of a service.
  absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
      service_status_providers_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_HEALTH_SERVICE_SRC_HEALTH_SERVICE_H_
