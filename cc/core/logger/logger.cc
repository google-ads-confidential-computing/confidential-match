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

#include <string>

#include "absl/base/log_severity.h"
#include "absl/log/log.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace google::confidential_match {

void Logger::Log(absl::LogSeverity severity, const char* file, int line,
                 absl::string_view message) const {
  std::string log_message;
  if (!request_id_.empty()) {
    log_message = absl::StrCat("[Req. ID: ", request_id_, "] ", message);
  } else {
    log_message = std::string(message);
  }

  switch (severity) {
    case absl::LogSeverity::kInfo:
      LOG(INFO).AtLocation(file, line) << log_message;
      break;
    case absl::LogSeverity::kWarning:
      LOG(WARNING).AtLocation(file, line) << log_message;
      break;
    case absl::LogSeverity::kError:
      LOG(ERROR).AtLocation(file, line) << log_message;
      break;
    case absl::LogSeverity::kFatal:
      LOG(FATAL).AtLocation(file, line) << log_message;
      break;
    default:
      // Unknown severity, log as info.
      LOG(ERROR).AtLocation(file, line)
          << "Error: Got unsupported log severity '" << severity
          << "', falling back to INFO level.";
      LOG(INFO).AtLocation(file, line) << log_message;
  }
}

void Logger::SetRequestId(const std::string& request_id) {
  request_id_ = request_id;
}

const Logger& GlobalLogger() {
  static Logger* logger = new Logger();
  return *logger;
}

}  // namespace google::confidential_match
