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

#include "cc/core/auth_token_client/auth_token_client.h"

#include <memory>
#include <string>
#include <utility>
#include <public/core/interface/execution_result.h>

#include "absl/functional/bind_front.h"
#include "absl/log/log.h"
#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"

namespace google::confidential_match {

using ::google::scp::core::AsyncContext;
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

absl::Status AuthTokenClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status AuthTokenClient::Run() noexcept {
  return absl::OkStatus();
}

absl::Status AuthTokenClient::Stop() noexcept {
  return absl::OkStatus();
}

absl::Status AuthTokenClient::GetAuthToken(
    AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>&
        get_auth_token_context) noexcept {
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
                      get_auth_token_context.request->audience()));
  http_context.request->method = HttpMethod::GET;
  http_context.request->body.length = 0;

  http_context.callback = absl::bind_front(
      &AuthTokenClient::OnGetAuthTokenCallback, this, get_auth_token_context);

  ExecutionResult result = http1_client_.PerformRequest(http_context);
  if (!result.Successful()) {
    absl::string_view msg = absl::StrCat(
        "Failed to start request to get auth token. Status code: %s",
        GetErrorMessage(result.status_code));
    LOG(ERROR) << msg;
    get_auth_token_context.result = result;
    // TODO(b/461982384): replace with common error library
    return absl::InternalError(msg);
  }
  return absl::OkStatus();
}

void AuthTokenClient::OnGetAuthTokenCallback(
    AsyncContext<GetAuthTokenRequest, absl::StatusOr<GetAuthTokenResponse>>&
        get_auth_token_context,
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  if (!http_context.result.Successful()) {
    std::string msg =
        absl::StrCat("Failed to get auth token: ",
                     GetErrorMessage(http_context.result.status_code));
    LOG(ERROR) << msg;
    // TODO(b/461982384): replace with common error library
    get_auth_token_context.response =
        std::make_shared<absl::StatusOr<GetAuthTokenResponse>>(
            absl::InternalError(msg));
    FinishContext(SuccessExecutionResult(), get_auth_token_context);
    return;
  }

  GetAuthTokenResponse response;
  response.set_token(http_context.response->body.ToString());

  get_auth_token_context.response =
      std::make_shared<absl::StatusOr<GetAuthTokenResponse>>(
          std::move(response));
  get_auth_token_context.result = http_context.result;
  get_auth_token_context.Finish();
}

}  // namespace google::confidential_match
