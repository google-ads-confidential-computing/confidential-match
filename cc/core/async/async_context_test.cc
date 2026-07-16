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

#include "cc/core/async/async_context.h"

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "absl/synchronization/notification.h"
#include "cc/core/interface/async_executor_interface.h"
#include "gtest/gtest.h"

#include "cc/core/async/error_codes.h"
#include "cc/core/logger/logger.h"

namespace google::confidential_match {
namespace {

using ::google::scp::core::AsyncExecutorAffinitySetting;
using ::google::scp::core::AsyncExecutorStats;
using ::google::scp::core::AsyncPriority;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::TaskCancellationLambda;
using ::google::scp::core::Timestamp;

class AsyncContextTest : public testing::Test {
 protected:
  AsyncContextTest() : logger_(std::make_shared<Logger>(GlobalLogger())) {}

  std::shared_ptr<LoggerInterface> logger_;
};

struct TestRequest {
  int id = 0;
};

struct TestResponse {
  explicit TestResponse(absl::string_view value) : value(value) {}

  std::string value;
};

TEST_F(AsyncContextTest, FinishSuccess) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  AsyncContext<TestRequest, TestResponse> context(
      request,
      [&](AsyncContext<TestRequest, TestResponse>& context) {
        EXPECT_EQ(context.request->id, 1);
        ASSERT_TRUE(context.status.ok());
        EXPECT_EQ(context.response->value, "Success");
        finished.Notify();
      },
      logger_);
  context.response = std::make_shared<TestResponse>("Success");
  context.status = absl::OkStatus();

  context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, FinishWithError) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  AsyncContext<TestRequest, TestResponse> context(
      request,
      [&](AsyncContext<TestRequest, TestResponse>& context) {
        EXPECT_EQ(context.request->id, 1);
        ASSERT_FALSE(context.status.ok());
        EXPECT_EQ(context.status.code(), absl::StatusCode::kInternal);
        EXPECT_EQ(context.status.message(), "Error");
        EXPECT_EQ(context.response, nullptr);
        finished.Notify();
      },
      logger_);
  context.status = absl::InternalError("Error");

  context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, FinishWithUnsetResponse) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  AsyncContext<TestRequest, TestResponse> context(
      request,
      [&](AsyncContext<TestRequest, TestResponse>& context) {
        ASSERT_FALSE(context.status.ok());
        EXPECT_EQ(context.status.code(), absl::StatusCode::kInternal);
        EXPECT_EQ(context.response, nullptr);
        finished.Notify();
      },
      logger_);

  context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, CreateContextPreservesLogger) {
  auto parent_context = AsyncContext<TestRequest, TestResponse>(logger_);
  auto child_context =
      parent_context.CreateAsyncContext<TestRequest, TestResponse>();
  EXPECT_EQ(child_context.logger, parent_context.logger);
}

TEST_F(AsyncContextTest, CreateExecutionResultChildContextPreservesLogger) {
  auto parent_context = AsyncContext<TestRequest, TestResponse>(logger_);
  auto child_context =
      parent_context
          .CreateExecutionResultAsyncContext<TestRequest, TestResponse>();
  EXPECT_EQ(child_context.logger, parent_context.logger);
}

TEST_F(AsyncContextTest, CreateContextWithRequestAndCallback) {
  absl::Notification finished;
  auto parent_context = AsyncContext<TestRequest, TestResponse>(
      std::make_shared<TestRequest>(),
      [&](AsyncContext<TestRequest, TestResponse>& context) {
        FAIL() << "Parent context should not have been called.";
      },
      logger_);
  parent_context.request->id = 123;
  auto child_request = std::make_shared<TestRequest>();
  child_request->id = 456;
  auto child_context =
      parent_context.CreateAsyncContext<TestRequest, TestResponse>(
          child_request, [&](AsyncContext<TestRequest, TestResponse>& context) {
            EXPECT_EQ(context.request->id, 456);
            ASSERT_FALSE(context.status.ok());
            EXPECT_EQ(context.status.code(),
                      absl::StatusCode::kInvalidArgument);
            finished.Notify();
          });
  child_context.status = absl::InvalidArgumentError("Invalid argument");

  child_context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
  EXPECT_EQ(child_context.logger, parent_context.logger);
  EXPECT_EQ(parent_context.request->id, 123);
  EXPECT_EQ(child_context.request->id, 456);
  EXPECT_FALSE(child_context.status.ok());
}

TEST_F(AsyncContextTest,
       CreateExecutionResultChildContextWithRequestAndCallback) {
  absl::Notification finished;
  auto parent_context = AsyncContext<TestRequest, TestResponse>(
      std::make_shared<TestRequest>(),
      [&](AsyncContext<TestRequest, TestResponse>& context) {
        FAIL() << "Parent context should not have been called.";
      },
      logger_);
  parent_context.request->id = 123;
  auto child_request = std::make_shared<TestRequest>();
  child_request->id = 456;
  auto child_context =
      parent_context.CreateExecutionResultAsyncContext<TestRequest,
                                                       TestResponse>(
          child_request,
          [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
            EXPECT_EQ(context.request->id, 456);
            EXPECT_FALSE(context.result.Successful());
            EXPECT_EQ(context.result.status_code, 1);
            finished.Notify();
          });
  child_context.result = FailureExecutionResult(1);

  child_context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
  EXPECT_EQ(child_context.logger, parent_context.logger);
  EXPECT_EQ(parent_context.request->id, 123);
  EXPECT_EQ(child_context.request->id, 456);
  EXPECT_FALSE(child_context.result.Successful());
}

TEST_F(AsyncContextTest, ExecutionResultAsyncContextFinishSuccess) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  ExecutionResultAsyncContext<TestRequest, TestResponse> context(
      request,
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        EXPECT_EQ(context.request->id, 1);
        ASSERT_TRUE(context.result.Successful());
        EXPECT_EQ(context.response->value, "Success");
        finished.Notify();
      },
      logger_);
  context.response = std::make_shared<TestResponse>("Success");

  context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, ExecutionResultAsyncContextFinishWithError) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  ExecutionResultAsyncContext<TestRequest, TestResponse> context(
      request,
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        EXPECT_EQ(context.request->id, 1);
        ASSERT_FALSE(context.result.Successful());
        EXPECT_EQ(context.result.status_code, 123);
        EXPECT_EQ(context.response, nullptr);
        finished.Notify();
      },
      logger_);
  context.result = scp::core::FailureExecutionResult(123);

  context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, ExecutionResultAsyncContextFinishWithUnsetResponse) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  ExecutionResultAsyncContext<TestRequest, TestResponse> context(
      request,
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        ASSERT_FALSE(context.result.Successful());
        EXPECT_EQ(context.result.status_code, ASYNC_INTERNAL_ERROR);
        EXPECT_EQ(context.response, nullptr);
        finished.Notify();
      },
      logger_);

  context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, ExecutionResultCreateContextPreservesLogger) {
  auto parent_context =
      ExecutionResultAsyncContext<TestRequest, TestResponse>(logger_);
  auto child_context =
      parent_context.CreateAsyncContext<TestRequest, TestResponse>();
  EXPECT_EQ(child_context.logger, parent_context.logger);
}

TEST_F(AsyncContextTest,
       ExecutionResultCreateExecutionResultChildContextPreservesLogger) {
  auto parent_context =
      ExecutionResultAsyncContext<TestRequest, TestResponse>(logger_);
  auto child_context =
      parent_context
          .CreateExecutionResultAsyncContext<TestRequest, TestResponse>();
  EXPECT_EQ(child_context.logger, parent_context.logger);
}

TEST_F(AsyncContextTest, ExecutionResultCreateContextWithRequestAndCallback) {
  absl::Notification finished;
  auto parent_context = ExecutionResultAsyncContext<TestRequest, TestResponse>(
      std::make_shared<TestRequest>(),
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        FAIL() << "Parent context should not have been called.";
      },
      logger_);
  parent_context.request->id = 123;
  auto child_request = std::make_shared<TestRequest>();
  child_request->id = 456;
  auto child_context =
      parent_context.CreateAsyncContext<TestRequest, TestResponse>(
          child_request, [&](AsyncContext<TestRequest, TestResponse>& context) {
            EXPECT_EQ(context.request->id, 456);
            EXPECT_EQ(context.response->value, "response");
            EXPECT_TRUE(context.status.ok());
            finished.Notify();
          });
  child_context.response = std::make_shared<TestResponse>("response");

  child_context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
  EXPECT_EQ(child_context.logger, parent_context.logger);
  EXPECT_EQ(parent_context.request->id, 123);
  EXPECT_EQ(child_context.request->id, 456);
  EXPECT_EQ(child_context.response->value, "response");
  EXPECT_TRUE(child_context.status.ok());
}

TEST_F(AsyncContextTest,
       ExecutionResultCreateExecutionResultChildContextWithRequestAndCallback) {
  absl::Notification finished;
  auto parent_context = ExecutionResultAsyncContext<TestRequest, TestResponse>(
      std::make_shared<TestRequest>(),
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        FAIL() << "Parent context should not have been called.";
      },
      logger_);
  parent_context.request->id = 123;
  auto child_request = std::make_shared<TestRequest>();
  child_request->id = 456;
  auto child_context =
      parent_context.CreateExecutionResultAsyncContext<TestRequest,
                                                       TestResponse>(
          child_request,
          [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
            EXPECT_EQ(context.request->id, 456);
            EXPECT_EQ(context.response->value, "response");
            EXPECT_TRUE(context.result.Successful());
            finished.Notify();
          });
  child_context.response = std::make_shared<TestResponse>("response");

  child_context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
  EXPECT_EQ(child_context.logger, parent_context.logger);
  EXPECT_EQ(parent_context.request->id, 123);
  EXPECT_EQ(child_context.request->id, 456);
  EXPECT_EQ(child_context.response->value, "response");
  EXPECT_TRUE(child_context.result.Successful());
}

TEST_F(AsyncContextTest,
       ExecutionResultAsyncContextFinishWithScpContextSuccess) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  ExecutionResultAsyncContext<TestRequest, TestResponse> context(
      request,
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        EXPECT_EQ(context.request->id, 1);
        ASSERT_TRUE(context.result.Successful());
        EXPECT_EQ(context.response->value, "Success");
        finished.Notify();
      },
      logger_);
  auto scp_context = context.CreateScpAsyncContext();
  scp_context.result = SuccessExecutionResult();
  scp_context.response = std::make_shared<TestResponse>("Success");

  scp_context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

TEST_F(AsyncContextTest, ExecutionResultAsyncContextFinishWithScpContextError) {
  absl::Notification finished;
  auto request = std::make_shared<TestRequest>();
  request->id = 1;
  ExecutionResultAsyncContext<TestRequest, TestResponse> context(
      request,
      [&](ExecutionResultAsyncContext<TestRequest, TestResponse>& context) {
        EXPECT_EQ(context.request->id, 1);
        ASSERT_FALSE(context.result.Successful());
        EXPECT_EQ(context.result.status_code, 123);
        EXPECT_EQ(context.response, nullptr);
        finished.Notify();
      },
      logger_);
  auto scp_context = context.CreateScpAsyncContext();
  scp_context.result = scp::core::FailureExecutionResult(123);

  scp_context.Finish();

  EXPECT_TRUE(finished.HasBeenNotified());
}

}  // namespace
}  // namespace google::confidential_match
