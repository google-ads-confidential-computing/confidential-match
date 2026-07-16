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

#ifndef CC_MATCH_SERVICE_AUTH_TOKEN_CLIENT_CACHED_AUTH_TOKEN_CLIENT_H_
#define CC_MATCH_SERVICE_AUTH_TOKEN_CLIENT_CACHED_AUTH_TOKEN_CLIENT_H_

#include <atomic>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "cc/core/async/async_context.h"
#include "cc/core/common/auto_expiry_concurrent_map/src/auto_expiry_concurrent_map.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/logger_interface.h"
#include "cc/core/interface/type_def.h"
#include "cc/match_service/auth_token_client/auth_token_client_interface.h"
#include "protos/core/auth_token.pb.h"

namespace google::confidential_match::match_service {

// Client that interfaces with the AuthToken provider and caches responses for
// reuse.
class CachedAuthTokenClient : public AuthTokenClientInterface {
 public:
  explicit CachedAuthTokenClient(
      std::shared_ptr<google::scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<AuthTokenClientInterface> auth_token_client,
      int cache_entry_lifetime_seconds,
      std::string orchestrator_audience,
      std::string lookup_service_audience,
      bool enable_background_refresh = false);

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  // Gets the auth token asynchronously, returning a cached token if available.
  void GetAuthToken(AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
                        get_auth_token_context) noexcept override;

  // Gets the auth token with full format asynchronously, returning a cached
  // token if available.
  void GetAuthTokenFullFormat(
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
          get_auth_token_context) noexcept override;

  // Gets the service account email asynchronously, returning a cached email if
  // available.
  void GetServiceAccountEmail(
      AsyncContext<GetServiceAccountEmailRequest,
                   GetServiceAccountEmailResponse>&
          get_service_account_email_context) noexcept override;

 private:
  // Helper to process a callback after fetching an auth token.
  void OnGetAuthTokenCallback(
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& cache_context,
      absl::string_view key_prefix,
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
          fetch_context) noexcept;

  // Helper to process a callback after fetching a service account email.
  void OnGetServiceAccountEmailCallback(
      AsyncContext<GetServiceAccountEmailRequest,
                   GetServiceAccountEmailResponse>& cache_context,
      AsyncContext<GetServiceAccountEmailRequest,
                   GetServiceAccountEmailResponse>& fetch_context) noexcept;

  // Returns a GetAuthTokenResponse from the cache, if it exists.
  absl::StatusOr<GetAuthTokenResponse> GetAuthTokenFromCache(
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& cache_context,
      absl::string_view key_prefix) noexcept;

  // Returns a GetServiceAccountEmailResponse from the cache, if it exists.
  absl::StatusOr<GetServiceAccountEmailResponse> GetEmailFromCache(
      AsyncContext<GetServiceAccountEmailRequest,
                   GetServiceAccountEmailResponse>& cache_context) noexcept;

  // Inserts a GetAuthTokenResponse into the cache.
  absl::Status InsertAuthTokenToCache(const GetAuthTokenRequest& request,
                                      const GetAuthTokenResponse& response,
                                      absl::string_view key_prefix) noexcept;

  // Inserts a GetServiceAccountEmailResponse into the cache.
  absl::Status InsertEmailToCache(
      const GetServiceAccountEmailRequest& request,
      const GetServiceAccountEmailResponse& response) noexcept;

  // Helpers for background refresh behavior
  void StartBackgroundRefreshLoop() noexcept;
  void ScheduleNextAuthTokenRefresh(bool is_retry) noexcept;
  void ScheduleNextAuthTokenFullFormatRefresh(bool is_retry) noexcept;
  void RefreshAuthToken() noexcept;
  void RefreshAuthTokenFullFormat() noexcept;

  void OnRefreshAuthTokenCallback(
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
          fetch_context) noexcept;
  void OnRefreshAuthTokenFullFormatCallback(
      AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>&
          fetch_context) noexcept;

  // The AsyncExecutor used by the concurrent map.
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  // The uncached client used to send requests.
  std::shared_ptr<AuthTokenClientInterface> auth_token_client_;
  // The cache containing successful responses for auth token and email fetches.
  scp::core::common::AutoExpiryConcurrentMap<std::string, std::string> cache_;

  // Background token refresh parameters & state
  std::string orchestrator_audience_;
  std::string lookup_service_audience_;
  bool enable_background_refresh_;
  std::atomic<bool> is_running_{false};
  int refresh_interval_seconds_;
  google::scp::core::Timestamp threshold_ns_;

  // Active cached token strings and email, guarded by absl::Mutex
  absl::Mutex auth_token_mutex_;
  std::string active_auth_token_ ABSL_GUARDED_BY(auth_token_mutex_);
  google::scp::core::Timestamp last_auth_token_refresh_time_ns_
      ABSL_GUARDED_BY(auth_token_mutex_){0};

  absl::Mutex auth_token_full_format_mutex_;
  std::string active_auth_token_full_format_
      ABSL_GUARDED_BY(auth_token_full_format_mutex_);
  google::scp::core::Timestamp last_auth_token_full_format_refresh_time_ns_
      ABSL_GUARDED_BY(auth_token_full_format_mutex_){0};

  // Background timer management
  absl::Mutex cancellation_mutex_;
  google::scp::core::TaskCancellationLambda auth_token_cancellation_callback_
      ABSL_GUARDED_BY(cancellation_mutex_);
  google::scp::core::TaskCancellationLambda
      auth_token_full_format_cancellation_callback_
          ABSL_GUARDED_BY(cancellation_mutex_);

  std::shared_ptr<google::confidential_match::LoggerInterface> logger_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_AUTH_TOKEN_CLIENT_CACHED_AUTH_TOKEN_CLIENT_H_
