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

#include "cc/match_service/auth_token_client/auth_token_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

#include "cc/core/async/async_context.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::errors::GetErrorMessage;
using ::std::placeholders::_1;

// Headers used to communicate with the VM metadata server.
constexpr absl::string_view kMetadataFlavorHeader = "Metadata-Flavor";
constexpr absl::string_view kMetadataFlavorHeaderValue = "Google";

// Path to the metadata server endpoint that provides identity tokens.
constexpr absl::string_view kMetadataServerIdentityUrl =
    "http://metadata/computeMetadata/v1/instance/service-accounts/default/"
    "identity";
constexpr absl::string_view kMetadataServerAudienceQueryParameter = "audience";
// Format parameter
constexpr absl::string_view kMetadataServerFormatQueryParameter = "format=full";

// Path to the metadata server endpoint that provides the service account email.
constexpr absl::string_view kMetadataServerEmailUrl =
    "http://metadata.google.internal/computeMetadata/v1/instance/"
    "service-accounts/default/email";

absl::Status AuthTokenClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status AuthTokenClient::Run() noexcept {
  return absl::OkStatus();
}

absl::Status AuthTokenClient::Stop() noexcept {
  return absl::OkStatus();
}

void AuthTokenClient::GetAuthToken(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        get_auth_token_context) noexcept {
  std::string query =
      absl::StrFormat("%s=%s", kMetadataServerAudienceQueryParameter,
                      get_auth_token_context.request->audience());

  SendAuthTokenRequest(get_auth_token_context, query);
}

void AuthTokenClient::GetAuthTokenFullFormat(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        get_auth_token_context) noexcept {
  std::string query =
      absl::StrFormat("%s=%s&%s", kMetadataServerAudienceQueryParameter,
                      get_auth_token_context.request->audience(),
                      kMetadataServerFormatQueryParameter);

  SendAuthTokenRequest(get_auth_token_context, query);
}

void AuthTokenClient::SendAuthTokenRequest(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        get_auth_token_context,
    absl::string_view query_params) noexcept {
  auto http_context =
      get_auth_token_context
          .CreateExecutionResultAsyncContext<HttpRequest, HttpResponse>();
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kMetadataFlavorHeader),
       std::string(kMetadataFlavorHeaderValue)});

  http_context.request->path =
      std::make_shared<std::string>(kMetadataServerIdentityUrl);
  http_context.request->query = std::make_shared<std::string>(query_params);
  http_context.request->method = HttpMethod::GET;
  http_context.request->body.length = 0;

  http_context.callback = absl::bind_front(
      &AuthTokenClient::OnGetAuthTokenCallback, this, get_auth_token_context);
  auto scpAsyncContext = http_context.CreateScpAsyncContext();

  ExecutionResult result = http1_client_.PerformRequest(scpAsyncContext);
  if (!result.Successful()) {
    std::string msg = absl::StrCat(
        "Failed to start request to get auth token. Status code: %s",
        GetErrorMessage(result.status_code));
    LOG_ERROR(*http_context.logger, msg);
    get_auth_token_context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, msg);
    get_auth_token_context.Finish();
  }
}

void AuthTokenClient::OnGetAuthTokenCallback(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        get_auth_token_context,
    ExecutionResultAsyncContext<HttpRequest, HttpResponse>&
        http_context) noexcept {
  if (!http_context.result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to get auth token: ",
                     GetErrorMessage(http_context.result.status_code));
    LOG_ERROR(*get_auth_token_context.logger, msg);
    get_auth_token_context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, msg);
    get_auth_token_context.Finish();
    return;
  }

  GetAuthTokenResponse response;
  response.set_token(http_context.response->body.ToString());

  get_auth_token_context.response =
      std::make_shared<GetAuthTokenResponse>(std::move(response));
  get_auth_token_context.Finish();
}

void AuthTokenClient::GetServiceAccountEmail(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        get_service_account_email_context) noexcept {
  auto http_context =
      get_service_account_email_context
          .CreateExecutionResultAsyncContext<HttpRequest, HttpResponse>();
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {std::string(kMetadataFlavorHeader),
       std::string(kMetadataFlavorHeaderValue)});

  http_context.request->path =
      std::make_shared<std::string>(kMetadataServerEmailUrl);
  http_context.request->method = HttpMethod::GET;
  http_context.request->body.length = 0;

  http_context.callback =
      absl::bind_front(&AuthTokenClient::OnGetServiceAccountEmailCallback, this,
                       get_service_account_email_context);
  auto scp_async_context = http_context.CreateScpAsyncContext();

  ExecutionResult result = http1_client_.PerformRequest(scp_async_context);
  if (!result.Successful()) {
    std::string msg = absl::StrCat(
        "Failed to start request to get service account email. Status code: %s",
        GetErrorMessage(result.status_code));
    LOG_ERROR(*http_context.logger, msg);
    get_service_account_email_context.status =
        Status(Error::AUTH_TOKEN_CLIENT_ERROR, msg);
    get_service_account_email_context.Finish();
  }
}

void AuthTokenClient::OnGetServiceAccountEmailCallback(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        get_service_account_email_context,
    ExecutionResultAsyncContext<HttpRequest, HttpResponse>&
        http_context) noexcept {
  if (!http_context.result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to get service account email: ",
                     GetErrorMessage(http_context.result.status_code));
    LOG_ERROR(*get_service_account_email_context.logger, msg);
    get_service_account_email_context.status =
        Status(Error::AUTH_TOKEN_CLIENT_ERROR, msg);
    get_service_account_email_context.Finish();
    return;
  }

  GetServiceAccountEmailResponse response;
  response.set_email(http_context.response->body.ToString());

  get_service_account_email_context.response =
      std::make_shared<GetServiceAccountEmailResponse>(std::move(response));
  get_service_account_email_context.Finish();
}

}  // namespace google::confidential_match::match_service
