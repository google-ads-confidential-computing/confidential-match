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

#include "cc/match_service/auth_token_client/cached_auth_token_client.h"

#include <atomic>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/interface/errors.h"
#include "cc/core/logger/log.h"
#include "cc/core/logger/logger.h"
#include "cc/match_service/error/error.h"
#include "cc/public/core/interface/execution_result.h"
#include "core/common/time_provider/src/time_provider.h"
#include "protos/core/auth_token.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::Timestamp;
using ::google::scp::core::common::AutoExpiryConcurrentMap;
using ::google::scp::core::common::TimeProvider;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST;

// Prefixes to distinguish between different types of cached items.
constexpr absl::string_view kAuthTokenKeyPrefix = "AT|";
constexpr absl::string_view kAuthTokenFullFormatKeyPrefix = "FULL_AT|";
constexpr absl::string_view kEmailKeyPrefix = "EMAIL|";

// Callback invoked by the cache before removing stale entries.
void OnBeforeCacheGarbageCollection(
    std::string& key, std::string& value,
    std::function<void(bool)> should_delete_entry) {
  should_delete_entry(true);
}

// Converts a GetAuthTokenRequest into a key used to cache a response.
std::string GetCacheKey(const GetAuthTokenRequest& request,
                        absl::string_view prefix) {
  return absl::StrCat(prefix, request.SerializeAsString());
}

// Converts a GetServiceAccountEmailRequest into a key used to cache a response.
std::string GetCacheKey(const GetServiceAccountEmailRequest& request) {
  return absl::StrCat(kEmailKeyPrefix, request.SerializeAsString());
}

// Converts a GetAuthTokenResponse into a serialized form for storage.
std::string GetCacheValue(const GetAuthTokenResponse& response) {
  return response.SerializeAsString();
}

// Converts a GetServiceAccountEmailResponse into a serialized form for storage.
std::string GetCacheValue(const GetServiceAccountEmailResponse& response) {
  return response.SerializeAsString();
}

// Converts a serialized cached response to a token response object.
absl::StatusOr<GetAuthTokenResponse> ParseAuthTokenCacheValue(
    absl::string_view cache_value) {
  GetAuthTokenResponse response;
  if (!response.ParseFromString(cache_value)) {
    return Status(Error::INTERNAL_ERROR,
                  "Could not parse cached GetAuthTokenResponse object.");
  }
  return response;
}

// Converts a serialized cached response to an email response object.
absl::StatusOr<GetServiceAccountEmailResponse> ParseEmailCacheValue(
    absl::string_view cache_value) {
  GetServiceAccountEmailResponse response;
  if (!response.ParseFromString(cache_value)) {
    return Status(
        Error::INTERNAL_ERROR,
        "Could not parse cached GetServiceAccountEmailResponse object.");
  }
  return response;
}

}  // namespace

CachedAuthTokenClient::CachedAuthTokenClient(
    std::shared_ptr<google::scp::core::AsyncExecutorInterface> async_executor,
    std::shared_ptr<AuthTokenClientInterface> auth_token_client,
    int cache_entry_lifetime_seconds, std::string orchestrator_audience,
    std::string lookup_service_audience, bool enable_background_refresh)
    : async_executor_(std::move(async_executor)),
      auth_token_client_(std::move(auth_token_client)),
      cache_(enable_background_refresh ?
               std::numeric_limits<size_t>::max()
               : cache_entry_lifetime_seconds,
             /* extend_entry_lifetime_on_access= */ false,
             /* block_entry_while_eviction= */ true,
             absl::bind_front(&OnBeforeCacheGarbageCollection),
             async_executor_),
      orchestrator_audience_(std::move(orchestrator_audience)),
      lookup_service_audience_(std::move(lookup_service_audience)),
      enable_background_refresh_(enable_background_refresh),
      is_running_(false),
      refresh_interval_seconds_(cache_entry_lifetime_seconds),
      threshold_ns_(std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::seconds(2 * cache_entry_lifetime_seconds))
                        .count()),
      logger_(std::make_shared<google::confidential_match::Logger>(
          google::confidential_match::GlobalLogger())) {}

absl::Status CachedAuthTokenClient::Init() noexcept {
  if (auto result = cache_.Init(); !result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to init auth token cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status CachedAuthTokenClient::Run() noexcept {
  if (!enable_background_refresh_) {
    if (auto result = cache_.Run(); !result.Successful()) {
      return Status(Error::INTERNAL_ERROR,
                    absl::StrCat("Failed to run auth token cache: ",
                                 GetErrorMessage(result.status_code)));
    }
  } else {
    is_running_ = true;
    StartBackgroundRefreshLoop();
  }
  return absl::OkStatus();
}

absl::Status CachedAuthTokenClient::Stop() noexcept {
  if (!enable_background_refresh_) {
    if (auto result = cache_.Stop(); !result.Successful()) {
      return Status(Error::INTERNAL_ERROR,
                    absl::StrCat("Failed to stop auth token cache: ",
                                 GetErrorMessage(result.status_code)));
    }
  } else {
    is_running_ = false;
    absl::MutexLock lock(&cancellation_mutex_);
    if (auth_token_cancellation_callback_) {
      auth_token_cancellation_callback_();
    }
    if (auth_token_full_format_cancellation_callback_) {
      auth_token_full_format_cancellation_callback_();
    }
  }
  return absl::OkStatus();
}

void CachedAuthTokenClient::GetAuthToken(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        cache_context) noexcept {
  if (enable_background_refresh_) {
    std::string token;
    Timestamp last_refresh_ns = 0;
    {
      absl::ReaderMutexLock lock(&auth_token_mutex_);
      token = active_auth_token_;
      last_refresh_ns = last_auth_token_refresh_time_ns_;
    }
    auto now_ns = TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
    if (!token.empty() &&
        cache_context.request->audience() == orchestrator_audience_ &&
        now_ns >= last_refresh_ns &&
        (now_ns - last_refresh_ns < threshold_ns_)) {
      auto response = std::make_shared<GetAuthTokenResponse>();
      response->set_token(std::move(token));
      cache_context.response = std::move(response);
      cache_context.status = absl::OkStatus();
      cache_context.Finish();
      return;
    }
    // Fallback: fetch on-demand
    std::string msg = absl::StrCat(
        "Falling back to on-demand auth token fetch call for audience: ",
        cache_context.request->audience(), ". Token empty: ", token.empty(),
        ", time since last refresh (ns): ",
        (now_ns >= last_refresh_ns ? std::to_string(now_ns - last_refresh_ns)
                                   : "N/A"));
    LOG_INFO(*cache_context.logger, msg);
    auto fetch_context =
        cache_context
            .CreateAsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>();
    fetch_context.request = cache_context.request;
    fetch_context.callback = [this, cache_context,
                              target_aud = cache_context.request->audience()](
                                 auto& finished_context) mutable {
      if (finished_context.status.ok() &&
          target_aud == orchestrator_audience_) {
        absl::MutexLock lock(&auth_token_mutex_);
        active_auth_token_ = finished_context.response->token();
        last_auth_token_refresh_time_ns_ =
            TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
      }
      cache_context.response = finished_context.response;
      cache_context.status = finished_context.status;
      cache_context.Finish();
    };
    auth_token_client_->GetAuthToken(fetch_context);
    return;
  }
  absl::StatusOr<GetAuthTokenResponse> cached_response_or =
      GetAuthTokenFromCache(cache_context, kAuthTokenKeyPrefix);
  if (cached_response_or.ok()) {
    cache_context.response =
        std::make_shared<GetAuthTokenResponse>(std::move(*cached_response_or));
    cache_context.Finish();
    return;
  }

  // No entry found from the cache. Fetch the auth token.
  LOG_INFO(*cache_context.logger, "Issuing auth token fetch call.");
  auto fetch_context =
      cache_context
          .CreateAsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>();
  fetch_context.request = cache_context.request;
  fetch_context.callback =
      absl::bind_front(&CachedAuthTokenClient::OnGetAuthTokenCallback, this,
                       cache_context, kAuthTokenKeyPrefix);

  auth_token_client_->GetAuthToken(fetch_context);
}

void CachedAuthTokenClient::GetAuthTokenFullFormat(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        cache_context) noexcept {
  if (enable_background_refresh_) {
    std::string token;
    Timestamp last_refresh_ns = 0;
    {
      absl::ReaderMutexLock lock(&auth_token_full_format_mutex_);
      token = active_auth_token_full_format_;
      last_refresh_ns = last_auth_token_full_format_refresh_time_ns_;
    }
    auto now_ns = TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
    if (!token.empty() &&
        cache_context.request->audience() == lookup_service_audience_ &&
        now_ns >= last_refresh_ns &&
        (now_ns - last_refresh_ns < threshold_ns_)) {
      auto response = std::make_shared<GetAuthTokenResponse>();
      response->set_token(std::move(token));
      cache_context.response = std::move(response);
      cache_context.status = absl::OkStatus();
      cache_context.Finish();
      return;
    }
    // Fallback: fetch on-demand
    std::string msg = absl::StrCat(
      "Falling back to on-demand full format auth token fetch call for "
      "audience: ",
      cache_context.request->audience(), ". Token empty: ", token.empty(),
      ", time since last refresh (ns): ",
      (now_ns >= last_refresh_ns ? std::to_string(now_ns - last_refresh_ns)
                                 : "N/A"));
  LOG_INFO(*cache_context.logger, msg);
    auto fetch_context =
        cache_context
            .CreateAsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>();
    fetch_context.request = cache_context.request;
    fetch_context.callback = [this, cache_context,
                              target_aud = cache_context.request->audience()](
                                 auto& finished_context) mutable {
      if (finished_context.status.ok() &&
          target_aud == lookup_service_audience_) {
        absl::MutexLock lock(&auth_token_full_format_mutex_);
        active_auth_token_full_format_ = finished_context.response->token();
        last_auth_token_full_format_refresh_time_ns_ =
            TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
      }
      cache_context.response = finished_context.response;
      cache_context.status = finished_context.status;
      cache_context.Finish();
    };
    auth_token_client_->GetAuthTokenFullFormat(fetch_context);
    return;
  }

  absl::StatusOr<GetAuthTokenResponse> cached_response_or =
      GetAuthTokenFromCache(cache_context, kAuthTokenFullFormatKeyPrefix);
  if (cached_response_or.ok()) {
    cache_context.response =
        std::make_shared<GetAuthTokenResponse>(std::move(*cached_response_or));
    cache_context.Finish();
    return;
  }

  // No entry found from the cache. Fetch the auth token.
  LOG_INFO(*cache_context.logger, "Issuing full format auth token fetch call.");
  auto fetch_context =
      cache_context
          .CreateAsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>();
  fetch_context.request = cache_context.request;
  fetch_context.callback =
      absl::bind_front(&CachedAuthTokenClient::OnGetAuthTokenCallback, this,
                       cache_context, kAuthTokenFullFormatKeyPrefix);

  auth_token_client_->GetAuthTokenFullFormat(fetch_context);
}

void CachedAuthTokenClient::OnGetAuthTokenCallback(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& cache_context,
    absl::string_view key_prefix,
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        fetch_context) noexcept {
  if (!fetch_context.status.ok()) {
    std::string msg = "Failed to fetch auth token from underlying client";
    LOG_ERROR(*fetch_context.logger, msg);
    cache_context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, msg);
    cache_context.Finish();
    return;
  }

  const auto& token_response = *fetch_context.response;
  absl::Status cache_result = InsertAuthTokenToCache(
      *cache_context.request, token_response, key_prefix);
  if (!cache_result.ok()) {
    std::string msg = absl::StrCat(
        "Successfully retrieved an auth token, "
        "but unable to cache the response. Error: ",
        cache_result.ToString());
    LOG_WARNING(*fetch_context.logger, msg);
  }

  cache_context.response = fetch_context.response;
  cache_context.Finish();
}

void CachedAuthTokenClient::GetServiceAccountEmail(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        cache_context) noexcept {
  absl::StatusOr<GetServiceAccountEmailResponse> cached_response_or =
      GetEmailFromCache(cache_context);
  if (cached_response_or.ok()) {
    cache_context.response = std::make_shared<GetServiceAccountEmailResponse>(
        std::move(*cached_response_or));
    cache_context.Finish();
    return;
  }

  // No entry found from the cache. Fetch the email.
  auto fetch_context =
      cache_context.CreateAsyncContext<GetServiceAccountEmailRequest,
                                       GetServiceAccountEmailResponse>();
  fetch_context.request = cache_context.request;
  fetch_context.callback =
      absl::bind_front(&CachedAuthTokenClient::OnGetServiceAccountEmailCallback,
                       this, cache_context);

  auth_token_client_->GetServiceAccountEmail(fetch_context);
}

void CachedAuthTokenClient::OnGetServiceAccountEmailCallback(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        cache_context,
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        fetch_context) noexcept {
  if (!fetch_context.status.ok()) {
    std::string msg =
        "Failed to fetch service account email from underlying client";
    LOG_ERROR(*fetch_context.logger, msg);
    cache_context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, msg);
    cache_context.Finish();
    return;
  }

  const auto& email_response = *fetch_context.response;
  absl::Status cache_result =
      InsertEmailToCache(*cache_context.request, email_response);
  if (!cache_result.ok()) {
    std::string msg = absl::StrCat(
        "Successfully retrieved a service account email, "
        "but unable to cache the response. Error: ",
        cache_result.ToString());
    LOG_WARNING(*fetch_context.logger, msg);
  }

  cache_context.response = fetch_context.response;
  cache_context.Finish();
}

absl::Status CachedAuthTokenClient::InsertAuthTokenToCache(
    const GetAuthTokenRequest& request, const GetAuthTokenResponse& response,
    absl::string_view key_prefix) noexcept {
  std::pair<std::string, std::string> key_value_pair(
      GetCacheKey(request, key_prefix), GetCacheValue(response));
  std::string existing_response_value;
  ExecutionResult insert_result =
      cache_.Insert(key_value_pair, existing_response_value);
  if (!insert_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  GetErrorMessage(insert_result.status_code));
  }
  return absl::OkStatus();
}

absl::Status CachedAuthTokenClient::InsertEmailToCache(
    const GetServiceAccountEmailRequest& request,
    const GetServiceAccountEmailResponse& response) noexcept {
  std::pair<std::string, std::string> key_value_pair(GetCacheKey(request),
                                                     GetCacheValue(response));
  std::string existing_response_value;
  ExecutionResult insert_result =
      cache_.Insert(key_value_pair, existing_response_value);
  if (!insert_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  GetErrorMessage(insert_result.status_code));
  }
  return absl::OkStatus();
}

absl::StatusOr<GetAuthTokenResponse>
CachedAuthTokenClient::GetAuthTokenFromCache(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& cache_context,
    absl::string_view key_prefix) noexcept {
  std::string serialized_response;
  ExecutionResult find_result = cache_.Find(
      GetCacheKey(*cache_context.request, key_prefix), serialized_response);
  if (find_result.status_code == SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST) {
    return Status(Error::NOT_FOUND, GetErrorMessage(find_result.status_code));
  } else if (!find_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  GetErrorMessage(find_result.status_code));
  }
  absl::StatusOr<GetAuthTokenResponse> status_or =
      ParseAuthTokenCacheValue(serialized_response);
  if (!status_or.ok()) {
    std::string msg =
        "Unable to deserialize a cached GetAuthTokenResponse object.";
    LOG_ERROR(*cache_context.logger, msg);
  }
  return status_or;
}

absl::StatusOr<GetServiceAccountEmailResponse>
CachedAuthTokenClient::GetEmailFromCache(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>&
        cache_context) noexcept {
  std::string serialized_response;
  ExecutionResult find_result =
      cache_.Find(GetCacheKey(*cache_context.request), serialized_response);
  if (find_result.status_code == SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST) {
    return Status(Error::NOT_FOUND, "GetServiceAccountEmailResponse not found");
  } else if (!find_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  GetErrorMessage(find_result.status_code));
  }
  absl::StatusOr<GetServiceAccountEmailResponse> status_or =
      ParseEmailCacheValue(serialized_response);
  if (!status_or.ok()) {
    std::string msg =
        "Unable to deserialize a cached GetAuthTokenResponse object";
    LOG_ERROR(*cache_context.logger, msg);
  }
  return status_or;
}

void CachedAuthTokenClient::StartBackgroundRefreshLoop() noexcept {
  if (!is_running_) return;
  RefreshAuthToken();
  RefreshAuthTokenFullFormat();
}

void CachedAuthTokenClient::ScheduleNextAuthTokenRefresh(
    bool is_retry) noexcept {
  absl::MutexLock lock(&cancellation_mutex_);
  if (!is_running_) return;

  if (auth_token_cancellation_callback_) {
    auth_token_cancellation_callback_();
  }

  auto delay = is_retry ? std::chrono::seconds(10)
                        : std::chrono::seconds(refresh_interval_seconds_);
  auto schedule_time =
      TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks() +
      std::chrono::duration_cast<std::chrono::nanoseconds>(delay).count();

  async_executor_->ScheduleFor([this]() { RefreshAuthToken(); }, schedule_time,
                               auth_token_cancellation_callback_);
}

void CachedAuthTokenClient::ScheduleNextAuthTokenFullFormatRefresh(
    bool is_retry) noexcept {
  absl::MutexLock lock(&cancellation_mutex_);
  if (!is_running_) return;

  if (auth_token_full_format_cancellation_callback_) {
    auth_token_full_format_cancellation_callback_();
  }

  auto delay = is_retry ? std::chrono::seconds(10)
                        : std::chrono::seconds(refresh_interval_seconds_);
  auto schedule_time =
      TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks() +
      std::chrono::duration_cast<std::chrono::nanoseconds>(delay).count();

  async_executor_->ScheduleFor([this]() { RefreshAuthTokenFullFormat(); },
                               schedule_time,
                               auth_token_full_format_cancellation_callback_);
}

void CachedAuthTokenClient::RefreshAuthToken() noexcept {
  if (!orchestrator_audience_.empty()) {
    auto context = AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>(
        std::make_shared<GetAuthTokenRequest>(),
        absl::bind_front(&CachedAuthTokenClient::OnRefreshAuthTokenCallback,
                         this),
        logger_);
    context.request->set_audience(orchestrator_audience_);
    auth_token_client_->GetAuthToken(context);
  }
}

void CachedAuthTokenClient::RefreshAuthTokenFullFormat() noexcept {
  if (!lookup_service_audience_.empty()) {
    auto context = AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>(
        std::make_shared<GetAuthTokenRequest>(),
        absl::bind_front(
            &CachedAuthTokenClient::OnRefreshAuthTokenFullFormatCallback, this),
        logger_);
    context.request->set_audience(lookup_service_audience_);
    auth_token_client_->GetAuthTokenFullFormat(context);
  }
}

void CachedAuthTokenClient::OnRefreshAuthTokenCallback(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        fetch_context) noexcept {
  if (fetch_context.status.ok()) {
    {
      absl::MutexLock lock(&auth_token_mutex_);
      active_auth_token_ = fetch_context.response->token();
      last_auth_token_refresh_time_ns_ =
          TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
    }
    ScheduleNextAuthTokenRefresh(/*is_retry=*/false);
  } else {
    LOG(WARNING) << "Failed to refresh standard auth token: "
                 << fetch_context.status.ToString();
    ScheduleNextAuthTokenRefresh(/*is_retry=*/true);
  }
}

void CachedAuthTokenClient::OnRefreshAuthTokenFullFormatCallback(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
        fetch_context) noexcept {
  if (fetch_context.status.ok()) {
    {
      absl::MutexLock lock(&auth_token_full_format_mutex_);
      active_auth_token_full_format_ = fetch_context.response->token();
      last_auth_token_full_format_refresh_time_ns_ =
          TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
    }
    ScheduleNextAuthTokenFullFormatRefresh(/*is_retry=*/false);
  } else {
    LOG(WARNING) << "Failed to refresh full format auth token: "
                 << fetch_context.status.ToString();
    ScheduleNextAuthTokenFullFormatRefresh(/*is_retry=*/true);
  }
}
}  // namespace google::confidential_match::match_service
