/*
 * Copyright 2025 Google LLC
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

#ifndef CC_CORE_LOGGER_LOGGER_H_
#define CC_CORE_LOGGER_LOGGER_H_

#include <string>

#include "absl/base/log_severity.h"
#include "absl/strings/string_view.h"
#include "cc/core/logger/logger_interface.h"

namespace google::confidential_match {

// A logger used to send log messages.
//
// Logs should be used with the log macros in `log.h` instead of directly.
// Example usage:
//
//   // Defined once per request.
//   Logger logger("unique_request_id");
//   // ...
//   LOG_INFO(logger, "Log with params: %s, %d, %f", "string", 1, 2.0);
//
//
// If no logger is available, you can fall back to the GlobalLogger:
//
//   LOG_INFO(GlobalLogger(), "Log with params: %s, %d, %f", "string", 1, 2.0);
//
class Logger : public LoggerInterface {
 public:
  // Constructs a logger object.
  Logger() : Logger("") {}

  // Constructs a logger object.
  //
  // request_id: (Optional) The request identifier to attach to all logs.
  explicit Logger(absl::string_view request_id) : request_id_(request_id) {}

  // Logs a message at the provided log severity.
  void Log(absl::LogSeverity severity, const char* file, int line,
           absl::string_view message) const override;

  // Sets the request ID to be attached to log statements.
  void SetRequestId(const std::string& request_id);

  using LoggerInterface::Log;

 private:
  // The request ID to be attached to all logs.
  std::string request_id_;
};

// Returns a global logger object with no associated request ID.
const Logger& GlobalLogger();

}  // namespace google::confidential_match

#endif  // CC_CORE_LOGGER_LOGGER_H_
