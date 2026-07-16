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

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "absl/status/status_matchers.h"
#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/interface/type_def.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/match_service/auth_token_client/mock_auth_token_client.h"
#include "cc/match_service/error/error.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "protos/core/auth_token.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::google::confidential_match::GetAuthTokenRequest;
using ::google::confidential_match::GetAuthTokenResponse;
using ::google::confidential_match::GetServiceAccountEmailRequest;
using ::google::confidential_match::GetServiceAccountEmailResponse;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::AsyncOperation;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::Timestamp;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::WaitUntil;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::Eq;
using ::testing::Field;
using ::testing::Pointee;
using ::testing::Property;

constexpr absl::string_view kAudience = "audience";
constexpr absl::string_view kAuthenticationToken = "token";
constexpr absl::string_view kEmail = "test@example.com";

class CachedAuthTokenClientTest : public testing::Test {
 protected:
  CachedAuthTokenClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_async_executor_(std::make_shared<MockAsyncExecutor>()),
        mock_auth_token_client_(std::make_shared<MockAuthTokenClient>()),
        cached_client_(std::make_unique<CachedAuthTokenClient>(
            mock_async_executor_, mock_auth_token_client_,
            /*cache_entry_lifetime_seconds=*/180, std::string(kAudience),
            std::string(kAudience),
            /*enable_background_refresh=*/false)) {
    mock_async_executor_->schedule_mock = [&](const AsyncOperation& work) {
      return SuccessExecutionResult();
    };
    mock_async_executor_->schedule_for_mock =
        [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
          return SuccessExecutionResult();
        };
  }

  void SetUp() override {
    ASSERT_THAT(cached_client_->Init(), IsOk());
    ASSERT_THAT(cached_client_->Run(), IsOk());
  }

  std::shared_ptr<LoggerInterface> logger_;
  std::shared_ptr<MockAsyncExecutor> mock_async_executor_;
  std::shared_ptr<MockAuthTokenClient> mock_auth_token_client_;
  std::unique_ptr<AuthTokenClientInterface> cached_client_;
};

// Helper to simulate a successful response from the uncached client for tokens.
void MockGetAuthTokenResponse(
    const GetAuthTokenResponse& response,
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context) noexcept {
  context.response = std::make_shared<GetAuthTokenResponse>(response);
  context.status = absl::OkStatus();
  context.Finish();
}

// Helper to simulate a failure response from the uncached client.
void MockFailureResponse(
    absl::Status status,
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context) noexcept {
  context.status = status;
  context.Finish();
}

// Helper to simulate a successful response from the uncached client for emails.
void MockGetServiceAccountEmailResponse(
    const GetServiceAccountEmailResponse& response,
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
        context) noexcept {
  context.response = std::make_shared<GetServiceAccountEmailResponse>(response);
  context.status = absl::OkStatus();
  context.Finish();
}

TEST_F(CachedAuthTokenClientTest, GetTokenUsesCache) {
  GetAuthTokenResponse expected_response;
  expected_response.set_token(kAuthenticationToken);
  EXPECT_CALL(*mock_auth_token_client_, GetAuthToken)
      .WillOnce(std::bind(MockGetAuthTokenResponse, expected_response, _1));
  GetAuthTokenRequest request;
  request.set_audience(kAudience);
  std::atomic<bool> is_complete = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context(
      std::make_shared<GetAuthTokenRequest>(request),
      [&expected_response, &is_complete](
          AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected_response));
        is_complete = true;
      },
      logger_);
  std::atomic<bool> is_complete1 = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context1(
      std::make_shared<GetAuthTokenRequest>(request),
      [&expected_response, &is_complete1](
          AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected_response));
        is_complete1 = true;
      },
      logger_);

  cached_client_->GetAuthToken(request_context);
  cached_client_->GetAuthToken(request_context1);

  WaitUntil([&]() { return is_complete.load(); });
  WaitUntil([&]() { return is_complete1.load(); });
}

TEST_F(CachedAuthTokenClientTest, GetAuthTokenFullFormatUsesCache) {
  GetAuthTokenResponse expected_response;
  expected_response.set_token(kAuthenticationToken);
  // Expect the underlying client's FullFormat method to be called
  EXPECT_CALL(*mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(std::bind(MockGetAuthTokenResponse, expected_response, _1));
  GetAuthTokenRequest request;
  request.set_audience(kAudience);
  std::atomic<bool> is_complete = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context(
      std::make_shared<GetAuthTokenRequest>(request),
      [&expected_response, &is_complete](
          AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected_response));
        is_complete = true;
      },
      logger_);
  std::atomic<bool> is_complete1 = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context1(
      std::make_shared<GetAuthTokenRequest>(request),
      [&expected_response, &is_complete1](
          AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected_response));
        is_complete1 = true;
      },
      logger_);

  cached_client_->GetAuthTokenFullFormat(request_context);
  cached_client_->GetAuthTokenFullFormat(request_context1);

  WaitUntil([&]() { return is_complete.load(); });
  WaitUntil([&]() { return is_complete1.load(); });
}

TEST_F(CachedAuthTokenClientTest, GetServiceAccountEmailUsesCache) {
  GetServiceAccountEmailResponse expected_response;
  expected_response.set_email(kEmail);
  EXPECT_CALL(*mock_auth_token_client_, GetServiceAccountEmail)
      .WillOnce(
          std::bind(MockGetServiceAccountEmailResponse, expected_response, _1));
  GetServiceAccountEmailRequest request;
  std::atomic<bool> is_complete = false;
  AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
      request_context(
          std::make_shared<GetServiceAccountEmailRequest>(request),
          [&expected_response, &is_complete](
              AsyncContext<GetServiceAccountEmailRequest,
                           GetServiceAccountEmailResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response));
            is_complete = true;
          },
          logger_);
  std::atomic<bool> is_complete1 = false;
  AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
      request_context1(
          std::make_shared<GetServiceAccountEmailRequest>(request),
          [&expected_response, &is_complete1](
              AsyncContext<GetServiceAccountEmailRequest,
                           GetServiceAccountEmailResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_THAT(*context.response, EqualsProto(expected_response));
            is_complete1 = true;
          },
          logger_);

  cached_client_->GetServiceAccountEmail(request_context);
  cached_client_->GetServiceAccountEmail(request_context1);

  WaitUntil([&]() { return is_complete.load(); });
  WaitUntil([&]() { return is_complete1.load(); });
}

TEST_F(CachedAuthTokenClientTest, GetTokenFailsOnAuthTokenClientError) {
  EXPECT_CALL(*mock_auth_token_client_, GetAuthToken)
      .WillOnce(
          std::bind(MockFailureResponse, absl::InternalError("Error"), _1));
  GetAuthTokenRequest request;
  request.set_audience(kAudience);
  std::atomic<bool> is_complete = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context(
      std::make_shared<GetAuthTokenRequest>(request),
      [&is_complete](
          AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(GetBackendErrorReason(context.status),
                    Eq(Error::AUTH_TOKEN_CLIENT_ERROR));
        is_complete = true;
      },
      logger_);

  cached_client_->GetAuthToken(request_context);

  WaitUntil([&]() { return is_complete.load(); });
}

TEST(CachedAuthTokenClientBackgroundRefreshTest, BackgroundRefreshSuccess) {
  auto logger = std::make_shared<Logger>(GlobalLogger());
  auto mock_async_executor = std::make_shared<MockAsyncExecutor>();
  auto mock_auth_token_client = std::make_shared<MockAuthTokenClient>();

  constexpr absl::string_view kOrchAudience = "orch_aud";
  constexpr absl::string_view kLSAudience = "ls_aud";
  constexpr absl::string_view kMockToken = "token_bg";
  constexpr absl::string_view kMockFullToken = "token_full_bg";
  constexpr absl::string_view kMockEmail = "bg@example.com";

  auto cached_client = std::make_unique<CachedAuthTokenClient>(
      mock_async_executor, mock_auth_token_client,
      /*cache_entry_lifetime_seconds=*/120, std::string(kOrchAudience),
      std::string(kLSAudience),
      /*enable_background_refresh=*/true);

  // Setup mock executor to intercept ScheduleFor to capture scheduling of next
  // refresh
  std::atomic<int> schedule_for_called = 0;
  mock_async_executor->schedule_for_mock =
      [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
        schedule_for_called++;
        return SuccessExecutionResult();
      };

  GetAuthTokenResponse regular_token_response;
  regular_token_response.set_token(std::string(kMockToken));

  GetAuthTokenResponse full_token_response;
  full_token_response.set_token(std::string(kMockFullToken));

  GetServiceAccountEmailResponse email_response;
  email_response.set_email(std::string(kMockEmail));

  // The client constructor is called, now Setup to mock calls
  EXPECT_CALL(*mock_auth_token_client, GetAuthToken)
      .WillOnce(
          std::bind(MockGetAuthTokenResponse, regular_token_response, _1));
  EXPECT_CALL(*mock_auth_token_client, GetAuthTokenFullFormat)
      .WillOnce(std::bind(MockGetAuthTokenResponse, full_token_response, _1));
  EXPECT_CALL(*mock_auth_token_client, GetServiceAccountEmail)
      .WillOnce(
          std::bind(MockGetServiceAccountEmailResponse, email_response, _1));

  ASSERT_THAT(cached_client->Init(), IsOk());
  ASSERT_THAT(cached_client->Run(), IsOk());

  // Wait/Verify regular token load
  GetAuthTokenRequest request;
  request.set_audience(std::string(kOrchAudience));
  std::atomic<bool> is_complete = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context(
      std::make_shared<GetAuthTokenRequest>(request),
      [&](AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_EQ(context.response->token(), kMockToken);
        is_complete = true;
      },
      logger);
  cached_client->GetAuthToken(request_context);
  WaitUntil([&]() { return is_complete.load(); });

  // Verify full format token load
  GetAuthTokenRequest request_full;
  request_full.set_audience(std::string(kLSAudience));
  std::atomic<bool> is_complete_full = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context_full(
      std::make_shared<GetAuthTokenRequest>(request_full),
      [&](AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_EQ(context.response->token(), kMockFullToken);
        is_complete_full = true;
      },
      logger);
  cached_client->GetAuthTokenFullFormat(request_context_full);
  WaitUntil([&]() { return is_complete_full.load(); });

  // Verify email load
  GetServiceAccountEmailRequest request_email;
  std::atomic<bool> is_complete_email = false;
  AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
      request_context_email(
          std::make_shared<GetServiceAccountEmailRequest>(request_email),
          [&](AsyncContext<GetServiceAccountEmailRequest,
                           GetServiceAccountEmailResponse>& context) {
            EXPECT_THAT(context.status, IsOk());
            EXPECT_EQ(context.response->email(), kMockEmail);
            is_complete_email = true;
          },
          logger);
  cached_client->GetServiceAccountEmail(request_context_email);
  WaitUntil([&]() { return is_complete_email.load(); });

  // Stop client to verify cancellation
  ASSERT_THAT(cached_client->Stop(), IsOk());
}

TEST(CachedAuthTokenClientBackgroundRefreshTest, FallbackOnStaleToken) {
  auto logger = std::make_shared<Logger>(GlobalLogger());
  auto mock_async_executor = std::make_shared<MockAsyncExecutor>();
  auto mock_auth_token_client = std::make_shared<MockAuthTokenClient>();

  constexpr absl::string_view kOrchAudience = "orch_aud";
  constexpr absl::string_view kInitialToken = "token_initial";
  constexpr absl::string_view kFallbackToken = "token_fallback";

  // Set cache_entry_lifetime_seconds to 1 second so 2x interval is 2 seconds.
  auto cached_client = std::make_unique<CachedAuthTokenClient>(
      mock_async_executor, mock_auth_token_client,
      /*cache_entry_lifetime_seconds=*/1, std::string(kOrchAudience),
      /*lookup_service_audience=*/"",
      /*enable_background_refresh=*/true);

  mock_async_executor->schedule_for_mock =
      [&](const AsyncOperation& work, Timestamp, std::function<bool()>&) {
        return SuccessExecutionResult();
      };

  GetAuthTokenResponse initial_token_response;
  initial_token_response.set_token(std::string(kInitialToken));

  EXPECT_CALL(*mock_auth_token_client, GetAuthToken)
      .WillOnce(
          std::bind(MockGetAuthTokenResponse, initial_token_response, _1));
  EXPECT_CALL(*mock_auth_token_client, GetServiceAccountEmail)
      .WillRepeatedly(std::bind(MockGetServiceAccountEmailResponse,
                                GetServiceAccountEmailResponse(), _1));

  ASSERT_THAT(cached_client->Init(), IsOk());
  ASSERT_THAT(cached_client->Run(), IsOk());

  // 1. Verify immediate load uses initial token
  GetAuthTokenRequest request;
  request.set_audience(std::string(kOrchAudience));
  std::atomic<bool> is_complete = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> request_context(
      std::make_shared<GetAuthTokenRequest>(request),
      [&](AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_EQ(context.response->token(), kInitialToken);
        is_complete = true;
      },
      logger);
  cached_client->GetAuthToken(request_context);
  WaitUntil([&]() { return is_complete.load(); });

  // 2. Sleep for 2.1 seconds so that time since last refresh exceeds 2 *
  // refresh_interval_seconds_ (2 seconds)
  std::this_thread::sleep_for(std::chrono::milliseconds(2100));

  // 3. Verify that GetAuthToken now falls back to retrieving a new token
  GetAuthTokenResponse fallback_token_response;
  fallback_token_response.set_token(std::string(kFallbackToken));
  EXPECT_CALL(*mock_auth_token_client, GetAuthToken)
      .WillOnce(
          std::bind(MockGetAuthTokenResponse, fallback_token_response, _1));

  std::atomic<bool> is_fallback_complete = false;
  AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> fallback_context(
      std::make_shared<GetAuthTokenRequest>(request),
      [&](AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_EQ(context.response->token(), kFallbackToken);
        is_fallback_complete = true;
      },
      logger);
  cached_client->GetAuthToken(fallback_context);
  WaitUntil([&]() { return is_fallback_complete.load(); });

  ASSERT_THAT(cached_client->Stop(), IsOk());
}

}  // namespace
}  // namespace google::confidential_match::match_service
