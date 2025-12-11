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

#include "cc/core/logger/log.h"

#include <memory>

#include "absl/base/log_severity.h"
#include "absl/log/scoped_mock_log.h"
#include "gtest/gtest.h"

#include "cc/core/logger/logger_interface.h"

namespace google::confidential_match {
namespace {

using ::testing::_;
using ::testing::AllOf;
using ::testing::HasSubstr;

class LogTest : public ::testing::Test {
 public:
  LogTest() : logger_(std::make_unique<Logger>()) {}

 protected:
  std::unique_ptr<LoggerInterface> logger_;
};

TEST_F(LogTest, LogInfoSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kInfo, HasSubstr("log_test.cc"),
                       HasSubstr("Test info")));
  log.StartCapturingLogs();

  LOG_INFO(*logger_, "Test info");
}

TEST_F(LogTest, LogWarningSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kWarning, HasSubstr("log_test.cc"),
                       HasSubstr("Test warning")));
  log.StartCapturingLogs();

  LOG_WARNING(*logger_, "Test warning");
}

TEST_F(LogTest, LogErrorSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kError, HasSubstr("log_test.cc"),
                       HasSubstr("Test error")));
  log.StartCapturingLogs();

  LOG_ERROR(*logger_, "Test error");
}

TEST_F(LogTest, LogInfoWithRequestIdSuccess) {
  Logger logger;
  logger.SetRequestId("test-request-id");
  absl::ScopedMockLog log;
  EXPECT_CALL(log,
              Log(absl::LogSeverity::kInfo, HasSubstr("log_test.cc"),
                  AllOf(HasSubstr("test-request-id"), HasSubstr("Test info"))));
  log.StartCapturingLogs();

  LOG_INFO(logger, "Test info");
}

TEST_F(LogTest, LogInfoWithFormatArgsSuccess) {
  Logger logger("test-request-id");
  absl::ScopedMockLog log;
  EXPECT_CALL(log,
              Log(absl::LogSeverity::kInfo, HasSubstr("log_test.cc"),
                  AllOf(HasSubstr("test-request-id"),
                        HasSubstr("Test info with args: string, 1, 2.0"))));
  log.StartCapturingLogs();

  LOG_INFO(logger, "Test info with args: %s, %d, %f", "string", 1, 2.0);
}

TEST_F(LogTest, LogInfoWithGlobalLoggerSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kInfo, HasSubstr("log_test.cc"),
                       HasSubstr("Test global logger")));
  log.StartCapturingLogs();

  LOG_INFO(GlobalLogger(), "Test global logger");

  // Verify that the global logger is a singleton.
  const Logger& logger1 = GlobalLogger();
  const Logger& logger2 = GlobalLogger();
  EXPECT_EQ(&logger1, &logger2);
}

}  // namespace
}  // namespace google::confidential_match
