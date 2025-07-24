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

#include "cc/lookup_server/orchestrator_client/src/orchestrator_client.h"

#include <future>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/http_types.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"
#include "google/protobuf/util/json_util.h"

#include "cc/lookup_server/orchestrator_client/src/error_codes.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    DataExportInfo;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::std::placeholders::_1;

constexpr absl::string_view kComponentName = "OrchestratorClient";

// Headers used to authenticate with the Orchestrator.
constexpr absl::string_view kAuthorizationHeader = "Authorization";
constexpr absl::string_view kAuthorizationHeaderValueFormat = "Bearer %s";

// Headers used to communicate with the VM metadata server.
constexpr absl::string_view kMetadataFlavorHeader = "Metadata-Flavor";
constexpr absl::string_view kMetadataFlavorHeaderValue = "Google";
// Path to the metadata server endpoint that provides identity tokens.
constexpr absl::string_view kMetadataServerIdentityUrl =
    "http://metadata/computeMetadata/v1/instance/service-accounts/default/"
    "identity";
constexpr absl::string_view kMetadataServerAudienceQueryParameter = "audience";

// Path to the Orchestrator endpoint providing the latest data export record.
constexpr absl::string_view kGetDataExportInfoPath =
    "/v1/dataexportrecords:latest";
constexpr absl::string_view kGetDataExportInfoClusterGroupIdQueryParameter =
    "clusterGroupId";
constexpr absl::string_view kGetDataExportInfoClusterIdQueryParameter =
    "clusterId";

}  // namespace

ExecutionResult OrchestratorClient::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult OrchestratorClient::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult OrchestratorClient::Stop() noexcept {
  return SuccessExecutionResult();
}

ExecutionResultOr<GetDataExportInfoResponse>
OrchestratorClient::GetDataExportInfo(
    const GetDataExportInfoRequest& request) noexcept {
  std::promise<ExecutionResult> result_promise;
  std::promise<std::shared_ptr<GetDataExportInfoResponse>> response_promise;

  AsyncContext<GetDataExportInfoRequest, GetDataExportInfoResponse>
      get_data_export_context;
  get_data_export_context.request =
      std::make_shared<GetDataExportInfoRequest>(request);
  get_data_export_context.callback = [&result_promise,
                                      &response_promise](auto& context) {
    result_promise.set_value(context.result);
    response_promise.set_value(context.response);
  };

  RETURN_IF_FAILURE(GetDataExportInfo(get_data_export_context));
  RETURN_IF_FAILURE(result_promise.get_future().get());
  return *response_promise.get_future().get();
}

ExecutionResult OrchestratorClient::GetDataExportInfo(
    AsyncContext<GetDataExportInfoRequest, GetDataExportInfoResponse>&
        get_data_export_context) noexcept {
  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Fetching data export metadata from Orchestrator. "
                           "(Cluster group: '%s', Cluster: '%s')",
                           get_data_export_context.request->cluster_group_id,
                           get_data_export_context.request->cluster_id));

  scp::core::AsyncContext<GetAuthenticationTokenRequest,
                          GetAuthenticationTokenResponse>
      get_auth_token_context;
  get_auth_token_context.request =
      std::make_shared<GetAuthenticationTokenRequest>();
  get_auth_token_context.request->audience = orchestrator_host_address_;
  get_auth_token_context.callback =
      std::bind(&OrchestratorClient::OnGetDataExportInfoAuthCallback, this,
                get_data_export_context, _1);
  return GetAuthenticationToken(get_auth_token_context);
}

void OrchestratorClient::OnGetDataExportInfoAuthCallback(
    AsyncContext<GetDataExportInfoRequest, GetDataExportInfoResponse>&
        get_data_export_context,
    AsyncContext<GetAuthenticationTokenRequest, GetAuthenticationTokenResponse>&
        get_auth_token_context) noexcept {
  if (!get_auth_token_context.result.Successful()) {
    SCP_ERROR_CONTEXT(kComponentName, get_auth_token_context,
                      get_auth_token_context.result,
                      "Failed to fetch the authentication token.");
    get_data_export_context.result = get_auth_token_context.result;
    get_data_export_context.Finish();
    return;
  }

  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kAuthorizationHeader),
       absl::StrFormat(kAuthorizationHeaderValueFormat,
                       get_auth_token_context.response->token)});

  http_context.request->path = std::make_shared<std::string>(
      absl::StrCat(orchestrator_host_address_, kGetDataExportInfoPath));
  http_context.request->query = std::make_shared<std::string>(
      absl::StrFormat("%s=%s&%s=%s", kGetDataExportInfoClusterIdQueryParameter,
                      get_data_export_context.request->cluster_id,
                      kGetDataExportInfoClusterGroupIdQueryParameter,
                      get_data_export_context.request->cluster_group_id));

  http_context.request->method = HttpMethod::GET;
  http_context.request->body.length = 0;

  http_context.callback =
      std::bind(&OrchestratorClient::OnGetDataExportInfoHttpCallback, this,
                get_data_export_context, _1);

  ExecutionResult result = http2_client_->PerformRequest(http_context);
  if (!result.Successful()) {
    get_data_export_context.result = result;
    get_data_export_context.Finish();
    return;
  }
}

void OrchestratorClient::OnGetDataExportInfoHttpCallback(
    AsyncContext<GetDataExportInfoRequest, GetDataExportInfoResponse>&
        get_data_export_context,
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  if (!http_context.result.Successful()) {
    SCP_ERROR_CONTEXT(
        kComponentName, http_context, http_context.result,
        "Request to Orchestrator get data export endpoint failed.");
    get_data_export_context.result = http_context.result;
    get_data_export_context.Finish();
    return;
  }

  std::string response_json = http_context.response->body.ToString();

  DataExportInfo data_export_info;
  if (!JsonStringToMessage(response_json, &data_export_info).ok()) {
    SCP_ERROR_CONTEXT(
        kComponentName, http_context, http_context.result,
        absl::StrCat("Failed to parse DataExportInfo proto: ", response_json));
    get_data_export_context.result =
        FailureExecutionResult(ORCHESTRATOR_CLIENT_PARSE_ERROR);
    get_data_export_context.Finish();
    return;
  }

  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Retrieved export metadata. "
                           "(Sharding scheme: '%s', Shard count: %d)",
                           data_export_info.sharding_scheme().type(),
                           data_export_info.sharding_scheme().num_shards()));

  get_data_export_context.response =
      std::make_shared<GetDataExportInfoResponse>();
  get_data_export_context.response->data_export_info =
      std::make_shared<DataExportInfo>(std::move(data_export_info));
  get_data_export_context.result = SuccessExecutionResult();
  get_data_export_context.Finish();
}

ExecutionResult OrchestratorClient::GetAuthenticationToken(
    scp::core::AsyncContext<GetAuthenticationTokenRequest,
                            GetAuthenticationTokenResponse>&
        get_authentication_token_context) noexcept {
  // TODO(b/271863149): Use the CPIO Auth Token Provider once released,
  // which will switch auth for the appropriate cloud environment.
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();

  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kMetadataFlavorHeader),
       std::string(kMetadataFlavorHeaderValue)});

  http_context.request->path =
      std::make_shared<std::string>(kMetadataServerIdentityUrl);
  http_context.request->query = std::make_shared<std::string>(
      absl::StrFormat("%s=%s", kMetadataServerAudienceQueryParameter,
                      get_authentication_token_context.request->audience));
  http_context.request->method = HttpMethod::GET;
  http_context.request->body.length = 0;

  http_context.callback =
      std::bind(&OrchestratorClient::OnGetAuthenticationTokenCallback, this,
                get_authentication_token_context, _1);

  return http1_client_->PerformRequest(http_context);
}

void OrchestratorClient::OnGetAuthenticationTokenCallback(
    scp::core::AsyncContext<GetAuthenticationTokenRequest,
                            GetAuthenticationTokenResponse>&
        get_authentication_token_context,
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  if (!http_context.result.Successful()) {
    get_authentication_token_context.result = http_context.result;
    get_authentication_token_context.Finish();
    return;
  }

  auto response = std::make_shared<GetAuthenticationTokenResponse>();
  response->token = http_context.response->body.ToString();

  get_authentication_token_context.response = response;
  get_authentication_token_context.result = http_context.result;
  get_authentication_token_context.Finish();
}

}  // namespace google::confidential_match::lookup_server
