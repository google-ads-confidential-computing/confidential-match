/*
 * Copyright 2026 Google LLC
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

#include "cc/lookup_server/auth/src/gcp/authorization_proxy_with_cached_public_keys.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "cc/lookup_server/auth/src/error_codes.h"
#include "core/authorization_proxy/src/error_codes.h"
#include "core/common/time_provider/src/time_provider.h"

using google::scp::core::AsyncContext;
using google::scp::core::AsyncExecutorInterface;
using google::scp::core::AsyncPriority;
using google::scp::core::AuthorizationProxyRequest;
using google::scp::core::AuthorizationProxyResponse;
using google::scp::core::ExecutionResult;
using google::scp::core::ExecutionResultOr;
using google::scp::core::FailureExecutionResult;
using google::scp::core::HttpClientInterface;
using google::scp::core::HttpHeaders;
using google::scp::core::HttpRequest;
using google::scp::core::HttpResponse;
using google::scp::core::RetryExecutionResult;
using google::scp::core::SuccessExecutionResult;
using google::scp::core::common::kZeroUuid;
using google::scp::core::common::TimeProvider;
using google::scp::core::errors::GetErrorMessage;
using google::scp::core::errors::SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS;
using google::scp::core::errors::SC_AUTHORIZATION_PROXY_BAD_REQUEST;
using std::bind;
using std::shared_ptr;
using std::string;
using std::transform;
using std::vector;
using std::chrono::duration_cast;
using std::chrono::nanoseconds;
using std::chrono::seconds;
using std::placeholders::_1;

namespace google::confidential_match::lookup_server::auth::gcp {

constexpr char kComponentName[] = "AuthorizationProxyWithCachedPublicKeys";
constexpr char kCacheControlHeader[] = "cache-control";
constexpr char kMaxAgePrefix[] = "max-age=";

// Minimum cooldown period before attempting another fetch if the last attempt
// failed.
constexpr seconds kMinRetryIntervalAfterFailure = seconds(2);

ExecutionResult AuthorizationProxyWithCachedPublicKeys::Init() noexcept {
  return AuthorizationProxyBase::Init();
}

ExecutionResult AuthorizationProxyWithCachedPublicKeys::Run() noexcept {
  auto execution_result = AuthorizationProxyBase::Run();
  if (!execution_result.Successful()) {
    return execution_result;
  }

  is_running_ = true;

  SchedulePublicKeyFetch(/*fetch_immediately=*/true);

  return SuccessExecutionResult();
}

ExecutionResult AuthorizationProxyWithCachedPublicKeys::Stop() noexcept {
  is_running_ = false;
  {
    std::lock_guard<std::mutex> lock(cancellation_mutex_);
    if (key_fetch_cancellation_callback_) {
      key_fetch_cancellation_callback_();
    }
  }
  return AuthorizationProxyBase::Stop();
}

void AuthorizationProxyWithCachedPublicKeys::SchedulePublicKeyFetch(
    bool fetch_immediately) noexcept {
  if (!is_running_) {
    return;
  }

  auto current_steady_time_nanos =
      TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
  auto delay_nanos =
      fetch_immediately
          ? 0
          : duration_cast<nanoseconds>(key_refresh_interval_).count();

  auto schedule_at_timestamp_nanos = current_steady_time_nanos + delay_nanos;

  // Dummy context to schedule TriggerPublicKeyFetch
  AsyncContext<int, int> context(std::make_shared<int>(0),
                                 [](AsyncContext<int, int>&) {});

  auto dispatch_func = [this, schedule_at_timestamp_nanos](
                           AsyncContext<int, int>& context) -> ExecutionResult {
    std::lock_guard<std::mutex> lock(cancellation_mutex_);
    if (!is_running_) {
      return SuccessExecutionResult();
    }
    // Make sure we cancel any pending TriggerPublicKeyFetch calls.
    if (key_fetch_cancellation_callback_) {
      key_fetch_cancellation_callback_();
    }

    auto execution_result = async_executor_->ScheduleFor(
        bind(&AuthorizationProxyWithCachedPublicKeys::TriggerPublicKeyFetch,
             this),
        schedule_at_timestamp_nanos, key_fetch_cancellation_callback_);

    if (!execution_result.Successful()) {
      return RetryExecutionResult(execution_result.status_code);
    }
    return SuccessExecutionResult();
  };

  // This is to account for contention in the async_executor's job queue.
  operation_dispatcher_.Dispatch<AsyncContext<int, int>>(context,
                                                         dispatch_func);
}

// This is a fire and forget operation.
// However, if scheduling this work fails or its result fails, we fall back to
// on-demand fetches, driven by a stale timestamp check.
void AuthorizationProxyWithCachedPublicKeys::TriggerPublicKeyFetch() noexcept {
  if (!is_running_) {
    return;
  }

  auto current_steady_time_nanos =
      TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
  auto last_failed_fetch = last_failed_fetch_timestamp_nanos_.load();
  auto min_retry_delay_nanos =
      duration_cast<nanoseconds>(kMinRetryIntervalAfterFailure).count();

  // Cooldown - skip scheduling if the last failure occurred too recently.
  if (last_failed_fetch > 0 &&
      current_steady_time_nanos < last_failed_fetch + min_retry_delay_nanos) {
    return;
  }

  bool expected = false;
  if (!is_fetch_in_progress_.compare_exchange_strong(expected, true)) {
    return;
  }

  SCP_INFO(kComponentName, kZeroUuid, "Triggering public key fetch.");

  auto execution_result = async_executor_->Schedule(
      bind(&AuthorizationProxyWithCachedPublicKeys::FetchPublicKeys, this),
      AsyncPriority::Urgent);
  if (!execution_result.Successful()) {
    is_fetch_in_progress_.store(false);
    SCP_ERROR(kComponentName, kZeroUuid, execution_result,
              "Failed to schedule public key fetch.");
  }
}

void AuthorizationProxyWithCachedPublicKeys::FetchPublicKeys() noexcept {
  if (!is_running_) {
    is_fetch_in_progress_.store(false);
    return;
  }

  AsyncContext<HttpRequest, HttpResponse> http_context(
      std::make_shared<HttpRequest>(),
      bind(&AuthorizationProxyWithCachedPublicKeys::OnFetchPublicKeysDone, this,
           _1));

  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.request->path =
      std::make_shared<std::string>(public_keys_endpoint_);

  // We don't use operation dispatcher for this call because the http client
  // has a built in operation dispatcher for retries.
  auto execution_result = http_client_->PerformRequest(http_context);

  // Per the current implementation this call can never fail, however we
  // keep this check just in case. If the call fails we retry scheduling the
  // fetch.
  if (!execution_result.Successful()) {
    is_fetch_in_progress_.store(false);
    SCP_WARNING(kComponentName, kZeroUuid,
                "Failed to schedule JWK set fetch http request.");
    last_failed_fetch_timestamp_nanos_.store(
        TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks());
    return SchedulePublicKeyFetch();
  }
}

ExecutionResultOr<seconds>
AuthorizationProxyWithCachedPublicKeys::ExtractMaxAge(
    const shared_ptr<HttpHeaders>& headers) {
  if (!headers) {
    return FailureExecutionResult(AUTH_MISSING_CACHE_CONTROL_MAX_AGE);
  }
  for (const auto& [key, value] : *headers) {
    string lowercase_key = key;
    transform(lowercase_key.begin(), lowercase_key.end(), lowercase_key.begin(),
              ::tolower);
    if (lowercase_key == kCacheControlHeader) {
      vector<string> parts = absl::StrSplit(value, ',');
      for (const string& part : parts) {
        // Trim whitespace
        string directive = part;
        directive.erase(0, directive.find_first_not_of(" \t"));
        directive.erase(directive.find_last_not_of(" \t") + 1);

        string lowercase_directive = directive;
        transform(lowercase_directive.begin(), lowercase_directive.end(),
                  lowercase_directive.begin(), ::tolower);

        if (lowercase_directive.rfind(kMaxAgePrefix, 0) == 0) {
          string val_str = directive.substr(sizeof(kMaxAgePrefix) - 1);
          // Trim quotes if any
          if (val_str.size() >= 2 && val_str.front() == '"' &&
              val_str.back() == '"') {
            val_str = val_str.substr(1, val_str.size() - 2);
          }
          try {
            return seconds(std::stoll(val_str));
          } catch (...) {
            // ignore parsing error, try next directive
          }
        }
      }
    }
  }
  return FailureExecutionResult(AUTH_MISSING_CACHE_CONTROL_MAX_AGE);
}

void AuthorizationProxyWithCachedPublicKeys::OnFetchPublicKeysDone(
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  if (http_context.result.Successful() && http_context.response &&
      http_context.response->body.bytes) {
    string new_keys = http_context.response->body.ToString();

    seconds safe_jwk_duration(-1);
    auto max_age_or = ExtractMaxAge(http_context.response->headers);
    if (max_age_or.Successful() &&
        *max_age_or > key_expiration_safety_period_) {
      safe_jwk_duration = *max_age_or - key_expiration_safety_period_;
    }

    auto validation_result =
        jwt_validator_->ValidateJwkSet(new_keys, safe_jwk_duration);
    if (validation_result.Successful()) {
      SCP_INFO(kComponentName, kZeroUuid, "Fetched JWK set successfully.");

      jwk_cache_.Update(std::move(new_keys));

      auto current_time_nanos =
          TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
      safe_jwk_expiration_nanos_.store(
          current_time_nanos +
          duration_cast<nanoseconds>(safe_jwk_duration).count());
    } else {
      SCP_WARNING(kComponentName, kZeroUuid,
                  "Fetched JWK set is invalid or corrupt. Error: %s. "
                  "Keeping old keys.",
                  GetErrorMessage(validation_result.status_code));
      last_failed_fetch_timestamp_nanos_.store(
          TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks());
    }
  } else {
    int error_code = http_context.response
                         ? static_cast<int>(http_context.response->code)
                         : 0;
    SCP_WARNING(kComponentName, kZeroUuid,
                "Failed to fetch JWK set with http code : %d.", error_code);
    last_failed_fetch_timestamp_nanos_.store(
        TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks());
  }

  is_fetch_in_progress_.store(false);
  SchedulePublicKeyFetch();
}

ExecutionResult AuthorizationProxyWithCachedPublicKeys::AuthorizeInternal(
    AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse>&
        authorization_context) noexcept {
  if (!jwk_cache_.IsInitialized()) {
    if (!is_fetch_in_progress_.load()) {
      SCP_INFO(kComponentName, kZeroUuid,
               "JWK set not initialized. Triggering on-demand fetch.");
    }
    TriggerPublicKeyFetch();
    return RetryExecutionResult(SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS);
  }

  auto current_time_nanos =
      TimeProvider::GetSteadyTimestampInNanosecondsAsClockTicks();
  auto safe_jwk_expiration = safe_jwk_expiration_nanos_.load();

  if (safe_jwk_expiration > 0 && current_time_nanos >= safe_jwk_expiration) {
    if (!is_fetch_in_progress_.load()) {
      SCP_INFO(kComponentName, kZeroUuid,
               "Cached JWK set has expired. Triggering on-demand fetch.");
    }

    TriggerPublicKeyFetch();
    return RetryExecutionResult(SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS);
  }

  auto jwk_set_ptr = jwk_cache_.Get();

  if (!jwk_set_ptr || jwk_set_ptr->empty()) {
    return RetryExecutionResult(SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS);
  }

  if (!authorization_context.request) {
    return FailureExecutionResult(SC_AUTHORIZATION_PROXY_BAD_REQUEST);
  }

  auto execution_result = jwt_validator_->Validate(
      *jwk_set_ptr, authorization_context.request->authorization_metadata
                        .authorization_token);
  if (!execution_result.Successful()) {
    SCP_INFO(kComponentName, kZeroUuid,
             "Incoming request not authorized to call Lookup Server: %s.",
             GetErrorMessage(execution_result.status_code));
    return execution_result;
  }

  authorization_context.response =
      std::make_shared<AuthorizationProxyResponse>();
  authorization_context.response->authorized_metadata.authorized_domain =
      std::make_shared<std::string>();

  authorization_context.result = SuccessExecutionResult();
  authorization_context.Finish();

  return SuccessExecutionResult();
}
}  // namespace google::confidential_match::lookup_server::auth::gcp
