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

#include "cc/match_service/orchestrator_client/orchestrator_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_types.h"
#include "google/protobuf/util/json_util.h"

#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/error/error.h"
#include "protos/core/auth_token.pb.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::FinishContext;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;

// Headers used to authenticate with the Orchestrator.
constexpr absl::string_view kAuthorizationHeader = "Authorization";
constexpr absl::string_view kAuthorizationHeaderValueFormat = "Bearer %s";

// Path to the Orchestrator endpoint providing the current sharding scheme.
constexpr absl::string_view kGetCurrentShardingSchemePath =
    "/v1/shardingschemes:latest";
constexpr absl::string_view kClusterGroupQueryParameter = "clusterGroupId";

absl::Status OrchestratorClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status OrchestratorClient::Run() noexcept {
  return absl::OkStatus();
}

absl::Status OrchestratorClient::Stop() noexcept {
  return absl::OkStatus();
}

void OrchestratorClient::GetCurrentShardingScheme(
    AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                 orchestrator::GetCurrentShardingSchemeResponse>&
        get_sharding_scheme_context) noexcept {
  LOG_INFO(
      *get_sharding_scheme_context.logger,
      absl::StrFormat("Fetching current sharding scheme from Orchestrator."
                      "(Cluster group: '%s')",
                      get_sharding_scheme_context.request->cluster_group_id()));

  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>
      get_auth_token_context =
          get_sharding_scheme_context
              .CreateAsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>();
  get_auth_token_context.request = std::make_shared<GetAuthTokenRequest>();
  get_auth_token_context.request->set_audience(orchestrator_host_address_);
  get_auth_token_context.callback = absl::bind_front(
      &OrchestratorClient::OnGetCurrentShardingSchemeAuthCallback, this,
      get_sharding_scheme_context);

  auth_token_client_.GetAuthToken(get_auth_token_context);
}

void OrchestratorClient::OnGetCurrentShardingSchemeAuthCallback(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& get_sharding_scheme_context,
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        get_auth_token_context) noexcept {
  if (!get_auth_token_context.status.ok()) {
    get_sharding_scheme_context.status = Annotate(
        get_auth_token_context.status,
        "Failed to fetch the authentication token for orchestrator request.");
    LOG_ERROR(*get_sharding_scheme_context.logger, "%v",
              get_sharding_scheme_context.status);
    get_sharding_scheme_context.Finish();
    return;
  }

  ExecutionResultAsyncContext<HttpRequest, HttpResponse> http_context =
      get_auth_token_context
          .CreateExecutionResultAsyncContext<HttpRequest, HttpResponse>();
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kAuthorizationHeader),
       absl::StrFormat(kAuthorizationHeaderValueFormat,
                       get_auth_token_context.response->token())});

  http_context.request->path = std::make_shared<std::string>(
      absl::StrCat(orchestrator_host_address_, kGetCurrentShardingSchemePath));
  http_context.request->query = std::make_shared<std::string>(
      absl::StrFormat("%s=%s", kClusterGroupQueryParameter,
                      get_sharding_scheme_context.request->cluster_group_id()));

  http_context.request->method = HttpMethod::GET;
  http_context.request->body.length = 0;
  http_context.callback = absl::bind_front(
      &OrchestratorClient::OnGetCurrentShardingSchemeHttpCallback, this,
      get_sharding_scheme_context);

  auto scp_async_context = http_context.CreateScpAsyncContext();
  ExecutionResult result = http2_client_.PerformRequest(scp_async_context);
  if (!result.Successful()) {
    std::string msg =
        absl::StrFormat("Failed to perform orchestrator http request: %s",
                        GetErrorMessage(http_context.result.status_code));
    LOG_ERROR(*get_auth_token_context.logger, msg);
    get_sharding_scheme_context.status =
        Status(Error::ORCHESTRATOR_SERVICE_ERROR, msg);
    get_sharding_scheme_context.Finish();
    return;
  }
}

void OrchestratorClient::OnGetCurrentShardingSchemeHttpCallback(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 GetCurrentShardingSchemeResponse>& get_sharding_scheme_context,
    ExecutionResultAsyncContext<HttpRequest, HttpResponse>&
        http_context) noexcept {
  if (!http_context.result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to get current sharding scheme: ",
                     GetErrorMessage(http_context.result.status_code));
    LOG_ERROR(*http_context.logger, msg);
    get_sharding_scheme_context.status =
        Status(Error::ORCHESTRATOR_SERVICE_ERROR, msg);
    get_sharding_scheme_context.Finish();
    return;
  }

  std::string response_json = http_context.response->body.ToString();
  GetCurrentShardingSchemeResponse cur_sharding_scheme;
  if (!JsonStringToMessage(response_json, &cur_sharding_scheme).ok()) {
    std::string msg =
        absl::StrCat("Failed to parse GetCurrentShardingSchemeResponse proto: ",
                     response_json);
    LOG_ERROR(*http_context.logger, msg);
    get_sharding_scheme_context.status =
        Status(Error::ORCHESTRATOR_RESPONSE_DESERIALIZATION_ERROR, msg);
    get_sharding_scheme_context.Finish();
    return;
  }

  LOG_INFO(*http_context.logger,
           "Retrieved sharding scheme: '%s', shard count: %d",
           cur_sharding_scheme.type(), cur_sharding_scheme.shards_size());

  get_sharding_scheme_context.response =
      std::make_shared<GetCurrentShardingSchemeResponse>(
          std::move(cur_sharding_scheme));
  get_sharding_scheme_context.Finish();
}

}  // namespace google::confidential_match::match_service
