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

#ifndef CC_LOOKUP_SERVER_HEALTH_SERVICE_MOCK_PASS_THROUGH_HEALTH_SERVICE_H_
#define CC_LOOKUP_SERVER_HEALTH_SERVICE_MOCK_PASS_THROUGH_HEALTH_SERVICE_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "cc/core/interface/http_server_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/health_service/src/health_service.h"
#include "cc/lookup_server/interface/status_provider_interface.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A pass-through Health Service that exposes underlying API methods
 * for testing only.
 */
class PassThroughHealthService : public HealthService {
 public:
  PassThroughHealthService(
      std::shared_ptr<scp::core::HttpServerInterface> http_server,
      absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
          service_status_providers)
      : HealthService(http_server, service_status_providers) {}

  scp::core::ExecutionResult CheckStartupHealth(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept {
    return HealthService::CheckStartupHealth(http_context);
  }

  scp::core::ExecutionResult CheckHealth(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept {
    return HealthService::CheckHealth(http_context);
  }
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_HEALTH_SERVICE_MOCK_PASS_THROUGH_HEALTH_SERVICE_H_
