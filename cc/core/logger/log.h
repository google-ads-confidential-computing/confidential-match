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

#ifndef CC_CORE_LOGGER_LOG_H_
#define CC_CORE_LOGGER_LOG_H_

#include "absl/base/log_severity.h"
#include "cc/core/logger/logger.h"
#include "cc/core/logger/logger_interface.h"

// Logs a message at the "info" severity level.
#define LOG_INFO(logger, format, ...)                                \
  (logger).Log(absl::LogSeverity::kInfo, __FILE__, __LINE__, format, \
               ##__VA_ARGS__)

// Logs a message at the "warning" severity level.
#define LOG_WARNING(logger, format, ...)                                \
  (logger).Log(absl::LogSeverity::kWarning, __FILE__, __LINE__, format, \
               ##__VA_ARGS__)

// Logs a message at the "error" severity level.
#define LOG_ERROR(logger, format, ...)                                \
  (logger).Log(absl::LogSeverity::kError, __FILE__, __LINE__, format, \
               ##__VA_ARGS__)

// Logs a message at the "fatal" severity level.
// This will terminate the program and should be used sparingly.
#define LOG_FATAL(logger, format, ...)                                \
  (logger).Log(absl::LogSeverity::kFatal, __FILE__, __LINE__, format, \
               ##__VA_ARGS__)

#endif  // CC_CORE_LOGGER_LOG_H_
