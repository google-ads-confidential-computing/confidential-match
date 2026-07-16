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

#ifndef CC_CORE_LOGGER_LOGGER_INTERFACE_H_
#define CC_CORE_LOGGER_LOGGER_INTERFACE_H_

#include "absl/base/log_severity.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match {

// Interface for a logger object.
class LoggerInterface {
 public:
  virtual ~LoggerInterface() = default;

  // Logs a message at the provided log severity.
  virtual void Log(absl::LogSeverity severity, const char* file, int line,
                   absl::string_view message) const = 0;

  template <typename... Args>
  void Log(absl::LogSeverity severity, const char* file, int line,
           absl::FormatSpec<Args...> format, Args... args) const {
    return Log(severity, file, line, absl::StrFormat(format, args...));
  }
};
}  // namespace google::confidential_match

#endif  // CC_CORE_LOGGER_LOGGER_INTERFACE_H_
