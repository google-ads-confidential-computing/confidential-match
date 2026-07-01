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

#ifndef CC_LOOKUP_SERVER_AUTH_SRC_GCP_AUTHORIZATION_PROXY_WITH_CACHED_PUBLIC_KEYS_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_AUTH_SRC_GCP_AUTHORIZATION_PROXY_WITH_CACHED_PUBLIC_KEYS_H_  // NOLINT(whitespace/line_length)

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "cc/lookup_server/auth/src/gcp/jwk_cache.h"
#include "cc/lookup_server/interface/jwt_validator_interface.h"
#include "core/authorization_proxy/src/authorization_proxy_base.h"
#include "core/common/operation_dispatcher/src/operation_dispatcher.h"
#include "core/interface/async_executor_interface.h"
#include "core/interface/http_client_interface.h"

namespace google::confidential_match::lookup_server::auth::gcp {

// These defaults are important and generous on purpose.
// JWKs live for a long time - generally 24 hours.
// We set the default to fetch new JWK sets every hour
// (kDefaultKeyRefreshInterval). We also provide a safety period
// before we start to trigger on-demand JWK fetches with incoming requests.
// We use the max-age from the Cache-Control header to determine the actual
// expiration time. The safety period (kDefaultKeyExpirationSafetyPeriod) is
// subtracted from the max-age value and used to ensure that we have fresh JWK
// sets before they expire.
static constexpr std::chrono::seconds kDefaultKeyRefreshInterval =
    std::chrono::hours(1);
static constexpr std::chrono::seconds kDefaultKeyExpirationSafetyPeriod =
    std::chrono::minutes(10);

static constexpr std::chrono::seconds kDefaultAuthCacheEntryLifetime =
    std::chrono::seconds(150);

class AuthorizationProxyWithCachedPublicKeys
    : public scp::core::AuthorizationProxyBase {
 public:
  AuthorizationProxyWithCachedPublicKeys(
      std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
      std::shared_ptr<scp::core::HttpClientInterface> http_client,
      std::shared_ptr<JwtValidatorInterface> jwt_validator,
      std::string public_keys_endpoint,
      std::chrono::seconds key_refresh_interval = kDefaultKeyRefreshInterval,
      std::chrono::seconds key_expiration_safety_period =
          kDefaultKeyExpirationSafetyPeriod,
      std::chrono::seconds auth_cache_entry_lifetime =
          kDefaultAuthCacheEntryLifetime)
      : AuthorizationProxyBase(async_executor, auth_cache_entry_lifetime),
        async_executor_(std::move(async_executor)),
        http_client_(std::move(http_client)),
        key_refresh_interval_(key_refresh_interval),
        public_keys_endpoint_(std::move(public_keys_endpoint)),
        jwt_validator_(std::move(jwt_validator)),
        operation_dispatcher_(async_executor_,
                              scp::core::common::RetryStrategy(
                                  scp::core::common::RetryStrategyType::Linear,
                                  /*delay_duration_ms=*/1000,
                                  /*maximum_allowed_retry_count=*/3)),
        is_running_(false),
        is_fetch_in_progress_(false),
        last_failed_fetch_timestamp_nanos_(0),
        safe_jwk_expiration_nanos_(0),
        key_expiration_safety_period_(key_expiration_safety_period) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

 protected:
  scp::core::ExecutionResult AuthorizeInternal(
      scp::core::AsyncContext<scp::core::AuthorizationProxyRequest,
                              scp::core::AuthorizationProxyResponse>&
          authorization_context) noexcept override;

  /*
   * Extracts the max-age value from the Cache-Control header in the public key
   * fetch response.
   */
  google::scp::core::ExecutionResultOr<std::chrono::seconds> ExtractMaxAge(
      const std::shared_ptr<google::scp::core::HttpHeaders>& headers);

  /**
   * Schedules a public key fetch operation after a delay.
   */
  void SchedulePublicKeyFetch(bool fetch_immediately = false) noexcept;

  /**
   * Triggers a public key fetch operation by immediately scheduling it.
   */
  void TriggerPublicKeyFetch() noexcept;

  /**
   * Sends an HTTP GET request to the configured public keys endpoint to fetch
   * the JWK set.
   */
  void FetchPublicKeys() noexcept;

  /**
   * Callback that is invoked when the public keys fetch request completes.
   */
  void OnFetchPublicKeysDone(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  std::shared_ptr<scp::core::HttpClientInterface> http_client_;
  std::chrono::seconds key_refresh_interval_;
  std::string public_keys_endpoint_;
  std::shared_ptr<JwtValidatorInterface> jwt_validator_;

  /**
   * Cache for the active set of JWKs used for validating JWTs.
   */
  JwkCache jwk_cache_;

  /**
   * Dispatcher for scheduling asynchronous operations with retries.
   */
  google::scp::core::common::OperationDispatcher operation_dispatcher_;

  /**
   * Flag to indicate if the authorization proxy is running.
   */
  std::atomic<bool> is_running_;

  /**
   * Flag to indicate if a public key fetch operation is in progress.
   */
  std::atomic<bool> is_fetch_in_progress_;

  /**
   * Timestamp of the last failed fetch attempt (in steady clock ticks). Used as
   * a cooldown period when triggering on-demand fetches.
   */
  std::atomic<google::scp::core::Timestamp> last_failed_fetch_timestamp_nanos_;

  /**
   * Timestamp of when we consider the JWK cache to be expired.
   */
  std::atomic<scp::core::Timestamp> safe_jwk_expiration_nanos_;

  /**
   * Period subtracted from the reported max-age value to make sure we expire
   * JWKs early, before their actual expiration.
   */
  std::chrono::seconds key_expiration_safety_period_;

  /**
   * Mutex to protect against concurrent access to cancellation data.
   */
  std::mutex cancellation_mutex_;

  /**
   * Cancellation callback for key fetch operations. This cancels the task that
   * was scheduled for later in AsyncExecutor.
   */
  google::scp::core::TaskCancellationLambda key_fetch_cancellation_callback_;
};

}  // namespace google::confidential_match::lookup_server::auth::gcp

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_AUTH_SRC_GCP_AUTHORIZATION_PROXY_WITH_CACHED_PUBLIC_KEYS_H_
