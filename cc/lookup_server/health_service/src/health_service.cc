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

#include "cc/lookup_server/health_service/src/health_service.h"

#include <functional>
#include <string>

#include "absl/time/clock.h"
#include "cc/core/common/uuid/src/uuid.h"

#include "cc/lookup_server/health_service/src/error_codes.h"
#include "cc/lookup_server/service/src/json_serialization_functions.h"
#include "protos/lookup_server/api/healthcheck.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"

namespace google::confidential_match::lookup_server {
using ::google::confidential_match::lookup_server::proto_api::
    HealthcheckResponse;
using ::google::confidential_match::lookup_server::proto_backend::ServiceStatus;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpHandler;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::Uuid;
using ::std::placeholders::_1;

constexpr absl::string_view kMatchDataStorageServiceName = "MatchDataStorage";
constexpr char kCheckHealthPath[] = "/v1/healthcheck";
constexpr char kCheckStartupHealthPath[] = "/v1/startup-healthcheck";

ExecutionResult HealthService::Init() noexcept {
  HttpHandler check_health_handler =
      std::bind(&HealthService::CheckHealth, this, _1);
  std::string healthcheck_path(kCheckHealthPath);
  http_server_->RegisterResourceHandler(HttpMethod::GET, healthcheck_path,
                                        check_health_handler);

  HttpHandler check_startup_health_handler =
      std::bind(&HealthService::CheckStartupHealth, this, _1);
  std::string startup_health_path(kCheckStartupHealthPath);
  http_server_->RegisterResourceHandler(HttpMethod::GET, startup_health_path,
                                        check_startup_health_handler);
  return SuccessExecutionResult();
}

ExecutionResult HealthService::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult HealthService::Stop() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult HealthService::CheckStartupHealth(
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  // For startup, return an error HTTP status code immediately if the storage
  // is not yet healthy to signal that the service is still initializing.
  ExecutionResult storage_service_result = CheckStorageServiceHealthy();
  if (!storage_service_result.Successful()) {
    http_context.result = storage_service_result;
    http_context.Finish();
    return SuccessExecutionResult();
  }

  // Once healthy, return the standard healthcheck JSON information.
  return CheckHealth(http_context);
}

ExecutionResult HealthService::CheckHealth(
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  ExecutionResult result = SuccessExecutionResult();

  HealthcheckResponse response;
  response.set_response_id(ToString(Uuid::GenerateUuid()));
  for (const auto& name_and_status_provider : service_status_providers_) {
    proto_api::Service* lookup_service = response.add_services();
    lookup_service->set_name(name_and_status_provider.first);
    lookup_service->set_status(
        ServiceStatus_Name(name_and_status_provider.second->GetStatus()));
  }

  if (!ProtoToJsonBytesBuffer(response, &http_context.response->body).ok()) {
    result = FailureExecutionResult(HEALTH_SERVICE_SERIALIZATION_ERROR);
  }

  http_context.result = result;
  http_context.Finish();
  return SuccessExecutionResult();
}

ExecutionResult HealthService::CheckStorageServiceHealthy() noexcept {
  for (const auto& name_and_status_provider : service_status_providers_) {
    if (name_and_status_provider.first != kMatchDataStorageServiceName) {
      continue;
    }
    if (name_and_status_provider.second->GetStatus() == ServiceStatus::OK) {
      return SuccessExecutionResult();
    } else {
      // Returning this signals to instance group health checks that the
      // service is not ready yet during the initial service startup.
      return FailureExecutionResult(HEALTH_SERVICE_STORAGE_SERVICE_ERROR);
    }
  }

  // Couldn't find storage service status provider in the map.
  return FailureExecutionResult(HEALTH_SERVICE_MISSING_STORAGE_SERVICE);
}

}  // namespace google::confidential_match::lookup_server
