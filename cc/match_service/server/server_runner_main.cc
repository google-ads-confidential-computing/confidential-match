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

#include <csignal>
#include <list>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/log/check.h"
#include "absl/log/globals.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/config_provider/src/env_config_provider.h"
#include "cc/core/interface/service_interface.h"
#include "cc/core/logger/src/log_providers/console_log_provider.h"
#include "cc/core/logger/src/log_utils.h"
#include "cc/core/logger/src/logger.h"

#include "cc/match_service/server/match_server.h"

using ::google::confidential_match::match_service::MatchServer;
using ::google::scp::core::EnvConfigProvider;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::LoggerInterface;
using ::google::scp::core::LogLevel;
using ::google::scp::core::ServiceInterface;
using ::google::scp::core::common::GlobalLogger;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::logger::ConsoleLogProvider;
using ::google::scp::core::logger::FromString;
using ::google::scp::core::logger::Logger;

// Configuration key for enabled log level types.
constexpr char kEnabledLogLevels[] = "enabled_log_levels";

int main(int argc, char* argv[]) {
  // Initialize signal handling
  // Use symbolizer for human-readable stack traces
  absl::InitializeSymbolizer(argv[0]);
  constexpr absl::FailureSignalHandlerOptions options;
  absl::InstallFailureSignalHandler(options);
  std::signal(SIGINT, SIG_DFL);
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGCHLD, SIG_IGN);
  std::signal(SIGHUP, SIG_IGN);

  // TODO: Read log level from environment
  // Initialize logging
  absl::SetStderrThreshold(absl::LogSeverityAtLeast::kInfo);
  absl::InitializeLog();

  EnvConfigProvider config_provider;
  std::unordered_set<LogLevel> log_levels;
  if (std::list<std::string> enabled_log_levels;
      config_provider.Get(kEnabledLogLevels, enabled_log_levels).Successful()) {
    for (const auto& enabled_log_level : enabled_log_levels) {
      log_levels.emplace(FromString(enabled_log_level));
    }
    GlobalLogger::SetGlobalLogLevels(log_levels);
  }

  std::unique_ptr<LoggerInterface> logger_ptr =
      std::make_unique<Logger>(std::make_unique<ConsoleLogProvider>());
  CHECK(logger_ptr->Init().Successful()) << "Cannot initialize logger.";
  CHECK(logger_ptr->Run().Successful()) << "Cannot run logger.";
  GlobalLogger::SetGlobalLogger(std::move(logger_ptr));

  MatchServer match_server(std::move(log_levels));
  if (const absl::Status init_status = match_server.Init(); !init_status.ok()) {
    LOG(FATAL) << "Match Server init failed: " << init_status.message();
  }
  if (const absl::Status run_status = match_server.Run(); !run_status.ok()) {
    LOG(FATAL) << "Match Server run failed: " << run_status.message();
  }

  return 0;
}
