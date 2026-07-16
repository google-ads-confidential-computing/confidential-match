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

#include "cc/match_service/lookup_service_shard_client/lookup_service_shard_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/async/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_types.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/error/error.h"
#include "google/protobuf/util/json_util.h"
#include "protos/core/auth_token.pb.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::errors::GetErrorMessage;

// Headers used to authenticate with the Lookup Service Shard.
constexpr absl::string_view kAuthorizationHeader = "x-auth-token";
constexpr absl::string_view kEmailHeader = "x-gscp-claimed-identity";

absl::Status LookupServiceShardClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status LookupServiceShardClient::Run() noexcept {
  return absl::OkStatus();
}

absl::Status LookupServiceShardClient::Stop() noexcept {
  return absl::OkStatus();
}

void LookupServiceShardClient::Lookup(
    AsyncContext<LookupRequest, LookupResponse>& lookup_context,
    absl::string_view lookup_service_shard_address) noexcept {
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>
      get_auth_token_context =
          lookup_context
              .CreateAsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>();
  get_auth_token_context.request = std::make_shared<GetAuthTokenRequest>();
  get_auth_token_context.request->set_audience(lookup_service_audience_);
  get_auth_token_context.callback = absl::bind_front(
      &LookupServiceShardClient::OnLookupAuthTokenCallback, this,
      lookup_context, std::string(lookup_service_shard_address));

  auth_token_client_.GetAuthTokenFullFormat(get_auth_token_context);
}

void LookupServiceShardClient::OnLookupAuthTokenCallback(
    AsyncContext<lookup_server::proto_api::LookupRequest,
                 lookup_server::proto_api::LookupResponse>& lookup_context,
    std::string lookup_service_shard_address,
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        get_auth_token_context) noexcept {
  if (!get_auth_token_context.status.ok()) {
    LOG_ERROR(*get_auth_token_context.logger,
              "Failed to fetch the authentication token: %s",
              get_auth_token_context.status.ToString());
    lookup_context.status = get_auth_token_context.status;
    lookup_context.Finish();
    return;
  }

  if (disable_service_account_email_retrieval_) {
    PerformHttpLookup(lookup_context, std::move(lookup_service_shard_address),
                      get_auth_token_context.response->token(),
                      service_account_email_);
    return;
  }

  auto get_service_account_email_context =
      get_auth_token_context.CreateAsyncContext<
          GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>();
  get_service_account_email_context.request =
      std::make_shared<GetServiceAccountEmailRequest>();
  get_service_account_email_context.callback = absl::bind_front(
      &LookupServiceShardClient::OnLookupServiceAccountEmailCallback, this,
      lookup_context, std::move(lookup_service_shard_address),
      get_auth_token_context.response->token());

  auth_token_client_.GetServiceAccountEmail(get_service_account_email_context);
}

void LookupServiceShardClient::OnLookupServiceAccountEmailCallback(
    AsyncContext<lookup_server::proto_api::LookupRequest,
                 lookup_server::proto_api::LookupResponse>& lookup_context,
    std::string lookup_service_shard_address, absl::string_view auth_token,
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        get_service_account_email_context) noexcept {
  if (!get_service_account_email_context.status.ok()) {
    LOG_ERROR(*get_service_account_email_context.logger,
              "Failed to fetch the service account email: %s",
              get_service_account_email_context.status.ToString());
    lookup_context.status = get_service_account_email_context.status;
    lookup_context.Finish();
    return;
  }

  PerformHttpLookup(lookup_context, std::move(lookup_service_shard_address),
                    auth_token,
                    get_service_account_email_context.response->email());
}

void LookupServiceShardClient::PerformHttpLookup(
    AsyncContext<lookup_server::proto_api::LookupRequest,
                 lookup_server::proto_api::LookupResponse>& lookup_context,
    std::string lookup_service_shard_address, absl::string_view auth_token,
    absl::string_view service_account_email) noexcept {
  ExecutionResultAsyncContext<HttpRequest, HttpResponse> http_context =
      lookup_context
          .CreateExecutionResultAsyncContext<HttpRequest, HttpResponse>();
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kAuthorizationHeader), std::string(auth_token)});
  http_context.request->headers->insert(
      {std::string(kEmailHeader), std::string(service_account_email)});

  http_context.request->path =
      std::make_shared<std::string>(std::move(lookup_service_shard_address));
  http_context.request->method = HttpMethod::POST;

  std::string request_json;
  auto status = MessageToJsonString(*lookup_context.request, &request_json);
  if (!status.ok()) {
    std::string msg = absl::StrCat(
        "Failed to serialize LookupRequest to JSON: ", status.ToString());
    LOG_ERROR(*lookup_context.logger, msg);
    lookup_context.status =
        Status(Error::LOOKUP_REQUEST_SERIALIZATION_ERROR, msg);
    lookup_context.Finish();
    return;
  }
  http_context.request->body =
      google::scp::core::BytesBuffer(request_json.length());
  http_context.request->body.bytes->assign(request_json.begin(),
                                           request_json.end());
  http_context.request->body.length = request_json.length();

  http_context.callback = absl::bind_front(
      &LookupServiceShardClient::OnLookupHttpCallback, this, lookup_context);

  auto scp_async_context = http_context.CreateScpAsyncContext();
  ExecutionResult result = http2_client_.PerformRequest(scp_async_context);
  if (!result.Successful()) {
    std::string msg =
        absl::StrFormat("Failed to perform lookup http request: %s",
                        GetErrorMessage(http_context.result.status_code));
    LOG_ERROR(*lookup_context.logger, msg);
    lookup_context.status =
        Status(Error::LOOKUP_SERVICE_SHARD_CLIENT_ERROR, msg);
    lookup_context.Finish();
    return;
  }
}

void LookupServiceShardClient::OnLookupHttpCallback(
    AsyncContext<LookupRequest, LookupResponse>& lookup_context,
    ExecutionResultAsyncContext<HttpRequest, HttpResponse>&
        http_context) noexcept {
  if (!http_context.result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to receive valid lookup response: ",
                     GetErrorMessage(http_context.result.status_code));
    LOG_ERROR(*http_context.logger, msg);
    lookup_context.status =
        Status(Error::LOOKUP_SERVICE_SHARD_CLIENT_ERROR, msg);
    lookup_context.Finish();
    return;
  }

  std::string response_json = http_context.response->body.ToString();

  LookupResponse lookup_response;
  if (!JsonStringToMessage(response_json, &lookup_response).ok()) {
    std::string msg =
        absl::StrCat("Failed to parse LookupResponse proto: ", response_json);
    LOG_ERROR(*http_context.logger, msg);
    lookup_context.status =
        Status(Error::LOOKUP_RESPONSE_DESERIALIZATION_ERROR, msg);
    lookup_context.Finish();
    return;
  }

  lookup_context.response =
      std::make_shared<LookupResponse>(std::move(lookup_response));
  lookup_context.Finish();
}

}  // namespace google::confidential_match::match_service
