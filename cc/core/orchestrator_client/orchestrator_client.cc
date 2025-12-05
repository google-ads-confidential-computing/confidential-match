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

#include "cc/core/orchestrator_client/orchestrator_client.h"

#include <future>
#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_types.h"
#include "google/protobuf/util/json_util.h"
#include "protos/core/auth_token.pb.h"
#include "protos/orchestrator/api/orchestrator.pb.h"

namespace google::confidential_match {

using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeRequest;
using ::google::confidential_match::orchestrator::
    GetCurrentShardingSchemeResponse;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::scp::core::AsyncContext;
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

absl::Status OrchestratorClient::GetCurrentShardingScheme(
    scp::core::AsyncContext<
        orchestrator::GetCurrentShardingSchemeRequest,
        absl::StatusOr<orchestrator::GetCurrentShardingSchemeResponse>>&
        get_sharding_scheme_context) noexcept {
  LOG(INFO) << absl::StrFormat(
      "Fetching current sharding scheme from Orchestrator."
      "(Cluster group: '%s')",
      get_sharding_scheme_context.request->cluster_group_id());

  AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>
      get_auth_token_context;
  get_auth_token_context.request = std::make_shared<GetAuthTokenRequest>();
  get_auth_token_context.request->set_audience(orchestrator_host_address_);
  get_auth_token_context.callback = absl::bind_front(
      &OrchestratorClient::OnGetCurrentShardingSchemeAuthCallback, this,
      get_sharding_scheme_context);
  absl::Status status = auth_token_client_.GetAuthToken(get_auth_token_context);
  if (!status.ok()) {
    std::string msg =
        absl::StrCat("Failed to get auth token from client: ", status);
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    return absl::InternalError(msg);
  }
  return absl::OkStatus();
}

void OrchestratorClient::OnGetCurrentShardingSchemeAuthCallback(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 absl::StatusOr<GetCurrentShardingSchemeResponse>>&
        get_sharding_scheme_context,
    AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>&
        get_auth_token_context) noexcept {
  ExecutionResult get_auth_token_result = get_auth_token_context.result;
  if (!get_auth_token_result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to fetch the authentication token: ",
                     GetErrorMessage(get_auth_token_result.status_code));
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    get_sharding_scheme_context.response =
        std::make_shared<absl::StatusOr<GetCurrentShardingSchemeResponse>>(
            absl::InternalError(msg));
    FinishContext(SuccessExecutionResult(), get_sharding_scheme_context);
    return;
  }

  if (!get_auth_token_context.response->ok()) {
    std::string msg = absl::StrCat("Failed to fetch the authentication token: ",
                                   get_auth_token_context.response->status());
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    get_sharding_scheme_context.response =
        std::make_shared<absl::StatusOr<GetCurrentShardingSchemeResponse>>(
            absl::InternalError(msg));
    FinishContext(SuccessExecutionResult(), get_sharding_scheme_context);
    return;
  }

  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kAuthorizationHeader),
       absl::StrFormat(kAuthorizationHeaderValueFormat,
                       get_auth_token_context.response->value().token())});

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

  ExecutionResult result = http2_client_.PerformRequest(http_context);
  if (!result.Successful()) {
    std::string msg =
        absl::StrFormat("Failed to perform orchestrator http request: %s",
                        GetErrorMessage(http_context.result.status_code));
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    get_sharding_scheme_context.response =
        std::make_shared<absl::StatusOr<GetCurrentShardingSchemeResponse>>(
            absl::InternalError(msg));
    FinishContext(SuccessExecutionResult(), get_sharding_scheme_context);
    return;
  }
}

void OrchestratorClient::OnGetCurrentShardingSchemeHttpCallback(
    AsyncContext<GetCurrentShardingSchemeRequest,
                 absl::StatusOr<GetCurrentShardingSchemeResponse>>&
        get_sharding_scheme_context,
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  if (!http_context.result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to get current sharding scheme: ",
                     GetErrorMessage(http_context.result.status_code));
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    get_sharding_scheme_context.response =
        std::make_shared<absl::StatusOr<GetCurrentShardingSchemeResponse>>(
            absl::InternalError(msg));
    FinishContext(SuccessExecutionResult(), get_sharding_scheme_context);
    return;
  }

  std::string response_json = http_context.response->body.ToString();
  GetCurrentShardingSchemeResponse cur_sharding_scheme;
  if (!JsonStringToMessage(response_json, &cur_sharding_scheme).ok()) {
    std::string msg =
        absl::StrCat("Failed to parse GetCurrentShardingSchemeResponse proto: ",
                     response_json);
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    get_sharding_scheme_context.response =
        std::make_shared<absl::StatusOr<GetCurrentShardingSchemeResponse>>(
            absl::InternalError(msg));
    FinishContext(SuccessExecutionResult(), get_sharding_scheme_context);
    return;
  }

  LOG(INFO) << absl::StrFormat(
      "Retrieved sharding scheme: '%s', shard count: %d",
      cur_sharding_scheme.type(), cur_sharding_scheme.shards_size());

  get_sharding_scheme_context.response =
      std::make_shared<absl::StatusOr<GetCurrentShardingSchemeResponse>>(
          std::move(cur_sharding_scheme));
  FinishContext(SuccessExecutionResult(), get_sharding_scheme_context);
}

}  // namespace google::confidential_match
