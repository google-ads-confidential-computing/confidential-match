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

#include "cc/core/logger/logger.h"

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

class LoggerTest : public ::testing::Test {
 public:
  LoggerTest() : logger_(std::make_unique<Logger>()) {}

 protected:
  std::unique_ptr<LoggerInterface> logger_;
};

TEST_F(LoggerTest, LogInfoSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kInfo, HasSubstr("logger_test.cc"),
                       HasSubstr("Test info")));
  log.StartCapturingLogs();

  logger_->Log(absl::LogSeverity::kInfo, "logger_test.cc", 123, "Test info");
}

TEST_F(LoggerTest, LogWarningSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kWarning, HasSubstr("logger_test.cc"),
                       HasSubstr("Test warning")));
  log.StartCapturingLogs();

  logger_->Log(absl::LogSeverity::kWarning, "logger_test.cc", 123,
               "Test warning");
}

TEST_F(LoggerTest, LogErrorSuccess) {
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kError, HasSubstr("logger_test.cc"),
                       HasSubstr("Test error")));
  log.StartCapturingLogs();

  logger_->Log(absl::LogSeverity::kError, "logger_test.cc", 123, "Test error");
}

TEST_F(LoggerTest, LogInfoWithRequestIdSuccess) {
  Logger logger;
  logger.SetRequestId("test-request-id");
  absl::ScopedMockLog log;
  EXPECT_CALL(log,
              Log(absl::LogSeverity::kInfo, HasSubstr("logger_test.cc"),
                  AllOf(HasSubstr("test-request-id"), HasSubstr("Test info"))));
  log.StartCapturingLogs();

  logger.Log(absl::LogSeverity::kInfo, "logger_test.cc", 123, "Test info");
}

TEST_F(LoggerTest, LogInfoWithFormatArgsSuccess) {
  Logger logger("test-request-id");
  absl::ScopedMockLog log;
  EXPECT_CALL(log,
              Log(absl::LogSeverity::kInfo, HasSubstr("logger_test.cc"),
                  AllOf(HasSubstr("test-request-id"),
                        HasSubstr("Test info with args: string, 1, 2.0"))));
  log.StartCapturingLogs();

  logger.Log(absl::LogSeverity::kInfo, "logger_test.cc", 123,
             "Test info with args: %s, %d, %f", "string", 1, 2.0);
}

TEST_F(LoggerTest, LogGlobalLoggerSuccess) {
  const Logger& logger = GlobalLogger();
  absl::ScopedMockLog log;
  EXPECT_CALL(log, Log(absl::LogSeverity::kInfo, HasSubstr("logger_test.cc"),
                       HasSubstr("Test global logger")));
  log.StartCapturingLogs();

  logger.Log(absl::LogSeverity::kInfo, "logger_test.cc", 123,
             "Test global logger");

  // Verify that the global logger is a singleton.
  const Logger& logger2 = GlobalLogger();
  EXPECT_EQ(&logger, &logger2);
}

}  // namespace
}  // namespace google::confidential_match
