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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>

#include "cc/lookup_server/auth/mock/mock_jwt_validator.h"
#include "cc/lookup_server/auth/src/error_codes.h"
#include "core/async_executor/mock/mock_async_executor.h"
#include "core/async_executor/src/async_executor.h"
#include "core/authorization_proxy/src/error_codes.h"
#include "core/http2_client/mock/mock_http_client.h"
#include "core/test/utils/conditional_wait.h"
#include "public/core/test/interface/execution_result_matchers.h"

using google::confidential_match::lookup_server::MockJwtValidator;
using google::scp::core::AsyncContext;
using google::scp::core::AuthorizationProxyRequest;
using google::scp::core::AuthorizationProxyResponse;
using google::scp::core::FailureExecutionResult;
using google::scp::core::HttpRequest;
using google::scp::core::HttpResponse;
using google::scp::core::RetryExecutionResult;
using google::scp::core::SuccessExecutionResult;
using google::scp::core::async_executor::mock::MockAsyncExecutor;
using google::scp::core::http2_client::mock::MockHttpClient;
using google::scp::core::test::IsSuccessful;
using google::scp::core::test::ResultIs;
using testing::_;
using testing::Return;

namespace google::confidential_match::lookup_server::auth::gcp::test {

class TestAuthorizationProxyWithCachedPublicKeys
    : public AuthorizationProxyWithCachedPublicKeys {
 public:
  using AuthorizationProxyWithCachedPublicKeys::
      AuthorizationProxyWithCachedPublicKeys;
  using AuthorizationProxyWithCachedPublicKeys::ExtractMaxAge;
  using AuthorizationProxyWithCachedPublicKeys::FetchPublicKeys;
  using AuthorizationProxyWithCachedPublicKeys::TriggerPublicKeyFetch;
};

class AuthorizationProxyWithCachedPublicKeysTest : public ::testing::Test {
 protected:
  void SetUp() override {
    async_executor_ = std::make_shared<MockAsyncExecutor>();

    // Prevent immediate execution to avoid infinite loops from recurring tasks
    async_executor_->schedule_mock =
        [&](const scp::core::AsyncOperation& work) {
          return SuccessExecutionResult();
        };
    async_executor_->schedule_for_mock =
        [&](const scp::core::AsyncOperation& work,
            scp::core::Timestamp timestamp,
            std::function<bool()>& cancellation_callback) {
          return SuccessExecutionResult();
        };

    http_client_ = std::make_shared<MockHttpClient>();
    auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
    ON_CALL(*mock_jwt_validator, ValidateJwkSet(_, _))
        .WillByDefault(Return(SuccessExecutionResult()));
    jwt_validator_ = mock_jwt_validator.get();

    proxy_ = std::make_unique<TestAuthorizationProxyWithCachedPublicKeys>(
        async_executor_, http_client_, mock_jwt_validator,
        "http://test.googleapis.com");

    EXPECT_TRUE(proxy_->Init().Successful());
  }

  std::shared_ptr<MockAsyncExecutor> async_executor_;
  std::shared_ptr<MockHttpClient> http_client_;
  MockJwtValidator* jwt_validator_;
  std::unique_ptr<TestAuthorizationProxyWithCachedPublicKeys> proxy_;
};

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       RunSchedulesFetchImmediately) {
  bool fetch_scheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        fetch_scheduled = true;
        return SuccessExecutionResult();
      };

  EXPECT_TRUE(proxy_->Run().Successful());
  EXPECT_TRUE(fetch_scheduled);
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       AuthorizeFailsIfNoKeysFetched) {
  // Assume Run was called but keys are not fetched yet
  EXPECT_TRUE(proxy_->Run().Successful());

  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";

  // Call Authorize instead of AuthorizeInternal as the base class handles it.
  EXPECT_EQ(
      proxy_->Authorize(context),
      RetryExecutionResult(
          scp::core::errors::SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       AuthorizeSucceedsIfKeysFetched) {
  bool fetch_rescheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        fetch_rescheduled = true;
        return SuccessExecutionResult();
      };

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  EXPECT_TRUE(proxy_->Run().Successful());
  fetch_rescheduled = false;

  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert(
            {"cache-control", "max-age=7200"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // Trigger the fetch directly
  proxy_->TriggerPublicKeyFetch();

  // Assert fetch gets rescheduled so we get fresh keys in the future.
  EXPECT_TRUE(fetch_rescheduled);

  EXPECT_CALL(*jwt_validator_, Validate("keys", "token"))
      .WillOnce(Return(SuccessExecutionResult()));

  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";

  EXPECT_TRUE(proxy_->Authorize(context).Successful());
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       FetchKeysFailureDoesNotCrashAndSchedulesRetry) {
  bool fetch_rescheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        fetch_rescheduled = true;
        return SuccessExecutionResult();
      };

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  EXPECT_TRUE(proxy_->Run().Successful());
  fetch_rescheduled = false;

  // Mock http client returning failure (e.g. 500 Internal Server Error)
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->code =
            scp::core::errors::HttpStatusCode::INTERNAL_SERVER_ERROR;
        http_context.result = FailureExecutionResult(12345);
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // Run the fetch work directly
  proxy_->TriggerPublicKeyFetch();

  // Verify that it rescheduled the fetch and didn't crash
  EXPECT_TRUE(fetch_rescheduled);

  // Verify Authorize still fails because keys are not initialized
  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";
  EXPECT_EQ(
      proxy_->Authorize(context),
      RetryExecutionResult(
          scp::core::errors::SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       OnDemandFetchTriggeredWhenStale) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  ON_CALL(*mock_jwt_validator, ValidateJwkSet(_, _))
      .WillByDefault(Return(SuccessExecutionResult()));
  auto jwt_validator_ptr = mock_jwt_validator.get();

  // Construct a proxy with 1-second refresh and 0-second grace period to force
  // the key fetch to seem stale.
  auto test_proxy =
      std::make_unique<TestAuthorizationProxyWithCachedPublicKeys>(
          async_executor_, http_client_, mock_jwt_validator,
          "http://test.googleapis.com",
          /*key_refresh_interval=*/std::chrono::seconds(1),
          /*key_expiration_safety_period=*/std::chrono::seconds(0));

  EXPECT_TRUE(test_proxy->Init().Successful());

  // 1. Start the proxy.
  EXPECT_TRUE(test_proxy->Run().Successful());

  // Set up mock HTTP response for the initial fetch
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert({"cache-control", "max-age=2"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // We need schedule_mock to execute the work immediately for the fetch to
  // complete
  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  // 2. Trigger the initial fetch directly to initialize the keys and set
  // next_fetch_timestamp
  test_proxy->TriggerPublicKeyFetch();

  // Ensure cache is initialized
  // We can verify this by checking if Authorize succeeds
  EXPECT_CALL(*jwt_validator_ptr, Validate("keys", "token"))
      .WillOnce(Return(SuccessExecutionResult()));

  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";

  // Initial authorize succeeds because keys are fresh (just fetched)
  EXPECT_THAT(test_proxy->Authorize(context), IsSuccessful());

  // 3. Sleep for 2 seconds to guarantee the keys become stale (1s interval
  // exceeded)
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // 4. Set up mock HTTP response for the on-demand fetch to return different
  // keys
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body =
            google::scp::core::BytesBuffer("new_keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert({"cache-control", "max-age=2"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // We want to capture if a new on-demand fetch was scheduled.
  // When Authorize is called, it should see keys are stale, call
  // TriggerPublicKeyFetch(), which schedules FetchPublicKeys via schedule_mock,
  // which runs it synchronously. And it should return retryable error
  // immediately.
  bool on_demand_fetch_triggered = false;
  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    on_demand_fetch_triggered = true;
    work();
    return SuccessExecutionResult();
  };

  // Create context2 with a different token to bypass base class caching
  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context2;
  context2.request = std::make_shared<AuthorizationProxyRequest>();
  context2.request->authorization_metadata.authorization_token = "token2";
  context2.request->authorization_metadata.claimed_identity = "identity";

  // 5. Perform authorization again with context2. Since keys are stale, it
  // should trigger on-demand fetch and return retry error.
  auto auth_result = test_proxy->Authorize(context2);
  EXPECT_EQ(auth_result.status_code,
            scp::core::errors::SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS);
  EXPECT_TRUE(on_demand_fetch_triggered);

  // Now, the on-demand fetch has run synchronously and updated the keys in the
  // cache to "new_keys". The next authorization call should succeed with
  // "new_keys" (since next_fetch_timestamp was updated).
  EXPECT_CALL(*jwt_validator_ptr, Validate("new_keys", "token2"))
      .WillOnce(Return(SuccessExecutionResult()));

  EXPECT_EQ(test_proxy->Authorize(context2), SuccessExecutionResult());
}

TEST(AuthorizationProxyWithCachedPublicKeysRealExecutorTest,
     StopSucceedsWithFutureScheduledTasks) {
  auto async_executor =
      std::make_shared<google::scp::core::AsyncExecutor>(2, 1000);
  EXPECT_TRUE(async_executor->Init().Successful());
  EXPECT_TRUE(async_executor->Run().Successful());

  auto http_client = std::make_shared<MockHttpClient>();
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  ON_CALL(*mock_jwt_validator, ValidateJwkSet(_, _))
      .WillByDefault(Return(SuccessExecutionResult()));

  std::atomic<bool> initial_fetch_completed(false);

  // Mock initial fetch success
  http_client->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert(
            {"cache-control", "max-age=7200"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        initial_fetch_completed.store(true);
        return SuccessExecutionResult();
      };

  auto proxy = std::make_shared<AuthorizationProxyWithCachedPublicKeys>(
      async_executor, http_client, mock_jwt_validator,
      "http://test.googleapis.com",
      /*key_refresh_interval=*/std::chrono::seconds(3600),
      /*key_expiration_safety_period=*/std::chrono::seconds(60));

  EXPECT_TRUE(proxy->Init().Successful());
  EXPECT_TRUE(proxy->Run().Successful());

  // Wait until the initial fetch executes and completes
  google::scp::core::test::WaitUntil(
      [&]() { return initial_fetch_completed.load(); });

  // The first fetch has completed, and the next fetch is scheduled for 1 hour
  // in the future. Stopping the proxy should cancel this future task.
  EXPECT_TRUE(proxy->Stop().Successful());

  // Stopping the executor should not hang.
  EXPECT_TRUE(async_executor->Stop().Successful());
}

TEST(AuthorizationProxyWithCachedPublicKeysRealExecutorTest,
     ConcurrentAuthorizationSucceeds) {
  auto async_executor =
      std::make_shared<google::scp::core::AsyncExecutor>(10, 10000);
  EXPECT_TRUE(async_executor->Init().Successful());
  EXPECT_TRUE(async_executor->Run().Successful());

  auto http_client = std::make_shared<MockHttpClient>();
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();

  std::atomic<bool> initial_fetch_completed(false);

  // Mock initial fetch success
  http_client->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert(
            {"cache-control", "max-age=7200"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        initial_fetch_completed.store(true);
        return SuccessExecutionResult();
      };

  ON_CALL(*mock_jwt_validator, ValidateJwkSet(_, _))
      .WillByDefault(Return(SuccessExecutionResult()));
  ON_CALL(*mock_jwt_validator, Validate(_, _))
      .WillByDefault(Return(SuccessExecutionResult()));

  auto proxy = std::make_shared<AuthorizationProxyWithCachedPublicKeys>(
      async_executor, http_client, mock_jwt_validator,
      "http://test.googleapis.com",
      /*key_refresh_interval=*/std::chrono::seconds(3600),
      /*key_expiration_safety_period=*/std::chrono::seconds(3600));

  EXPECT_TRUE(proxy->Init().Successful());
  EXPECT_TRUE(proxy->Run().Successful());

  // Wait until the initial fetch executes and completes
  google::scp::core::test::WaitUntil(
      [&]() { return initial_fetch_completed.load(); });

  std::vector<std::thread> threads;
  std::atomic<size_t> success_count(0);
  std::atomic<size_t> failure_count(0);
  std::atomic<size_t> completed_count(0);

  for (int i = 0; i < 100; ++i) {
    threads.push_back(std::thread([&, i]() {
      AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse>
          context;
      context.request = std::make_shared<AuthorizationProxyRequest>();
      context.request->authorization_metadata.authorization_token =
          std::string("token_") + std::to_string(i);
      context.request->authorization_metadata.claimed_identity = "identity";
      context.callback = [&](auto& ctx) {
        if (ctx.result.Successful()) {
          success_count++;
        } else {
          failure_count++;
        }
        completed_count++;
      };

      auto result = proxy->Authorize(context);
      if (!result.Successful()) {
        failure_count++;
        completed_count++;
      }
    }));
  }

  for (auto& t : threads) {
    t.join();
  }

  google::scp::core::test::WaitUntil(
      [&]() { return completed_count.load() == 100; });

  EXPECT_EQ(success_count.load(), 100);
  EXPECT_EQ(failure_count.load(), 0);

  EXPECT_TRUE(proxy->Stop().Successful());
  EXPECT_TRUE(async_executor->Stop().Successful());
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       FetchKeysFailureOnValidationReschedulesRetryAndKeepsCachedKeys) {
  bool retry_scheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        retry_scheduled = true;
        return SuccessExecutionResult();
      };

  EXPECT_TRUE(proxy_->Init().Successful());
  EXPECT_TRUE(proxy_->Run().Successful());
  retry_scheduled = false;

  // Set up mock HTTP response for the initial fetch (valid keys)
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert(
            {"cache-control", "max-age=7200"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  EXPECT_CALL(*jwt_validator_,
              ValidateJwkSet("keys", std::chrono::seconds(6600)))
      .WillOnce(Return(SuccessExecutionResult()));

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  // Run the initial fetch directly
  proxy_->TriggerPublicKeyFetch();

  // Cache is initialized with "keys". Validate verify call succeeds
  EXPECT_CALL(*jwt_validator_, Validate("keys", "token"))
      .WillOnce(Return(SuccessExecutionResult()));

  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";
  EXPECT_THAT(proxy_->Authorize(context), IsSuccessful());

  // Now, mock a subsequent refresh that returns corrupted keys
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body =
            google::scp::core::BytesBuffer("corrupted-garbage");
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // ValidateJwkSet returns failure for the corrupted keys
  EXPECT_CALL(*jwt_validator_,
              ValidateJwkSet("corrupted-garbage", std::chrono::seconds(-1)))
      .WillOnce(Return(FailureExecutionResult(
          google::confidential_match::lookup_server::AUTH_INVALID_JWK)));

  // Reset the flag
  retry_scheduled = false;

  // Trigger the periodic fetch directly
  proxy_->TriggerPublicKeyFetch();

  // Verify that the retry was rescheduled
  EXPECT_TRUE(retry_scheduled);

  // Verify that the cached keys were NOT polluted/overwritten and are still
  // "keys"
  EXPECT_CALL(*jwt_validator_, Validate("keys", "token2"))
      .WillOnce(Return(SuccessExecutionResult()));

  context.request->authorization_metadata.authorization_token = "token2";
  EXPECT_THAT(proxy_->Authorize(context), IsSuccessful());
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       FetchFailureEnforcesCooldown) {
  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  EXPECT_TRUE(proxy_->Init().Successful());

  bool perform_request_called = false;
  // Set up HTTP client to fail
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        perform_request_called = true;
        http_context.result = FailureExecutionResult(1234);
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // Run schedules the startup fetch task.
  EXPECT_TRUE(proxy_->Run().Successful());

  // Execute the startup fetch task manually.
  proxy_->TriggerPublicKeyFetch();
  EXPECT_TRUE(perform_request_called);

  // Reset the flag
  perform_request_called = false;

  // Now, call Authorize multiple times.
  // Because of the cooldown, these should be no-ops and not schedule any
  // tasks.
  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";

  proxy_->Authorize(context);
  proxy_->Authorize(context);
  proxy_->Authorize(context);

  // The schedule call count must still be 1 (no new fetches were scheduled)
  EXPECT_FALSE(perform_request_called);
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       RescheduleCancelsPreviousTask) {
  bool previous_task_cancelled = false;

  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  ON_CALL(*mock_jwt_validator, ValidateJwkSet(_, _))
      .WillByDefault(Return(SuccessExecutionResult()));

  // 1s refresh interval, 0s safety period
  auto test_proxy =
      std::make_unique<TestAuthorizationProxyWithCachedPublicKeys>(
          async_executor_, http_client_, mock_jwt_validator,
          "http://test.googleapis.com",
          /*key_refresh_interval=*/std::chrono::seconds(1),
          /*key_expiration_safety_period=*/std::chrono::seconds(0));

  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        cancellation_callback = [&]() {
          previous_task_cancelled = true;
          return true;
        };
        return SuccessExecutionResult();
      };

  EXPECT_TRUE(test_proxy->Init().Successful());
  EXPECT_TRUE(test_proxy->Run().Successful());

  // Set up HTTP client to succeed
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert({"cache-control", "max-age=2"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  EXPECT_CALL(*jwt_validator_, ValidateJwkSet("keys", _))
      .WillRepeatedly(Return(SuccessExecutionResult()));

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  // During startup, Run() scheduled the first fetch. So the cancellation
  // callback for the startup task is registered, but it has not been cancelled
  // yet.
  ASSERT_FALSE(previous_task_cancelled);

  // Sleep for 2s to make keys stale.
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Authorize will trigger on-demand fetch because of staleness
  AsyncContext<AuthorizationProxyRequest, AuthorizationProxyResponse> context;
  context.request = std::make_shared<AuthorizationProxyRequest>();
  context.request->authorization_metadata.authorization_token = "token";
  context.request->authorization_metadata.claimed_identity = "identity";

  EXPECT_EQ(test_proxy->Authorize(context).status_code,
            scp::core::errors::SC_AUTHORIZATION_PROXY_AUTH_REQUEST_INPROGRESS);

  // The on-demand fetch has completed, so it rescheduled a refresh, which must
  // have cancelled the old one.
  EXPECT_TRUE(previous_task_cancelled);
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       MissingCacheControlHeaderTriggersReschedule) {
  bool fetch_rescheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        fetch_rescheduled = true;
        return SuccessExecutionResult();
      };

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  EXPECT_TRUE(proxy_->Run().Successful());
  fetch_rescheduled = false;

  // Mock http client returning response without Cache-Control header
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // We expect ValidateJwkSet to be called with duration = -1 seconds, and we
  // set it to fail
  EXPECT_CALL(*jwt_validator_, ValidateJwkSet("keys", std::chrono::seconds(-1)))
      .WillOnce(Return(
          FailureExecutionResult(google::confidential_match::lookup_server::
                                     AUTH_INVALID_JWK_DURATION)));

  // Trigger the fetch directly
  proxy_->TriggerPublicKeyFetch();

  // Verify that it rescheduled the fetch because validation failed due to
  // missing max-age
  EXPECT_TRUE(fetch_rescheduled);
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       CacheControlMaxAgeUnderflowTriggersReschedule) {
  bool fetch_rescheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        fetch_rescheduled = true;
        return SuccessExecutionResult();
      };

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  EXPECT_TRUE(proxy_->Run().Successful());
  fetch_rescheduled = false;

  // Mock http client returning max-age of 300 seconds (5 minutes)
  // Since proxy's safety period is 10 minutes (600 seconds), this is an
  // underflow
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert(
            {"cache-control", "max-age=300"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // We expect ValidateJwkSet to be called with duration = -1 seconds due to
  // underflow
  EXPECT_CALL(*jwt_validator_, ValidateJwkSet("keys", std::chrono::seconds(-1)))
      .WillOnce(Return(
          FailureExecutionResult(google::confidential_match::lookup_server::
                                     AUTH_INVALID_JWK_DURATION)));

  // Trigger the fetch directly
  proxy_->TriggerPublicKeyFetch();

  // Verify that it rescheduled the fetch
  EXPECT_TRUE(fetch_rescheduled);
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       CacheControlHeaderWithMultipleHttpDirectivesSucceeds) {
  bool fetch_rescheduled = false;
  async_executor_->schedule_for_mock =
      [&](const scp::core::AsyncOperation& work, scp::core::Timestamp timestamp,
          std::function<bool()>& cancellation_callback) {
        fetch_rescheduled = true;
        return SuccessExecutionResult();
      };

  async_executor_->schedule_mock = [&](const scp::core::AsyncOperation& work) {
    work();
    return SuccessExecutionResult();
  };

  EXPECT_TRUE(proxy_->Run().Successful());
  fetch_rescheduled = false;

  // Mock HTTP response with multiple Cache-Control directives:
  // "public, max-age=21524, must-revalidate, no-transform"
  http_client_->perform_request_mock =
      [&](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        http_context.response = std::make_shared<HttpResponse>();
        http_context.response->body = google::scp::core::BytesBuffer("keys");
        http_context.response->headers =
            std::make_shared<scp::core::HttpHeaders>();
        http_context.response->headers->insert(
            {"cache-control",
             "public, max-age=21524, must-revalidate, no-transform"});
        http_context.result = SuccessExecutionResult();
        http_context.Finish();
        return SuccessExecutionResult();
      };

  // 21524 - 600 (safety period) = 20924
  EXPECT_CALL(*jwt_validator_,
              ValidateJwkSet("keys", std::chrono::seconds(20924)))
      .WillOnce(Return(SuccessExecutionResult()));

  // Trigger the fetch directly
  proxy_->TriggerPublicKeyFetch();

  // Verify it rescheduled the fetch successfully (because validation succeeded)
  EXPECT_TRUE(fetch_rescheduled);
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeReturnsFailureIfHeadersIsNull) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");

  auto result = test_proxy.ExtractMaxAge(nullptr);

  EXPECT_THAT(result.result(), ResultIs(FailureExecutionResult(
                                   google::confidential_match::lookup_server::
                                       AUTH_MISSING_CACHE_CONTROL_MAX_AGE)));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeReturnsFailureIfCacheControlHeaderIsMissing) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"content-type", "application/json"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), ResultIs(FailureExecutionResult(
                                   google::confidential_match::lookup_server::
                                       AUTH_MISSING_CACHE_CONTROL_MAX_AGE)));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeReturnsFailureIfCacheControlHeaderHasNoMaxAge) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control", "public, no-transform, must-revalidate"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), ResultIs(FailureExecutionResult(
                                   google::confidential_match::lookup_server::
                                       AUTH_MISSING_CACHE_CONTROL_MAX_AGE)));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeReturnsFailureForInvalidNonNumericMaxAge) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control", "max-age=abc"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), ResultIs(FailureExecutionResult(
                                   google::confidential_match::lookup_server::
                                       AUTH_MISSING_CACHE_CONTROL_MAX_AGE)));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithSimpleMaxAge) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control", "max-age=3600"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(3600));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithMultipleDirectives) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control",
                   "public, max-age=21524, must-revalidate, no-transform"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(21524));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithDifferentKeyCasing) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"Cache-Control", "max-age=1800"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(1800));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithDifferentValueCasing) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control", "public, Max-Age=1800"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(1800));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithQuotedMaxAge) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert(
      {"cache-control", "public, max-age=\"3600\", must-revalidate"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(3600));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithExtraSpaces) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control", "  public  ,   max-age=3600   "});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(3600));
}

TEST_F(AuthorizationProxyWithCachedPublicKeysTest,
       ExtractMaxAgeSucceedsWithMultipleHeadersButOnlyOneValidCacheControl) {
  auto mock_jwt_validator = std::make_shared<MockJwtValidator>();
  TestAuthorizationProxyWithCachedPublicKeys test_proxy(
      async_executor_, http_client_, mock_jwt_validator,
      "http://test.googleapis.com");
  auto headers = std::make_shared<scp::core::HttpHeaders>();
  headers->insert({"cache-control", "public"});
  headers->insert({"cache-control", "max-age=1800"});

  auto result = test_proxy.ExtractMaxAge(headers);

  EXPECT_THAT(result.result(), IsSuccessful());
  EXPECT_EQ(*result, std::chrono::seconds(1800));
}
}  // namespace google::confidential_match::lookup_server::auth::gcp::test
