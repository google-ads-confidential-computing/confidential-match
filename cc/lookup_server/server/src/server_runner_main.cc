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

#include <execinfo.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <list>
#include <string>


#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/symbolize.h"
#include "absl/log/absl_log.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/config_provider/src/env_config_provider.h"
#include "cc/core/interface/errors.h"
#include "cc/core/logger/src/log_providers/console_log_provider.h"
#include "cc/core/logger/src/log_providers/syslog/syslog_log_provider.h"
#include "cc/core/logger/src/log_utils.h"
#include "cc/core/logger/src/logger.h"

#include "cc/lookup_server/interface/configuration_keys.h"
#include "cc/lookup_server/server/src/lookup_server.h"

using ::google::confidential_match::lookup_server::kEnabledLogLevels;
using ::google::confidential_match::lookup_server::LookupServer;
using ::google::scp::core::ConfigProviderInterface;
using ::google::scp::core::EnvConfigProvider;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::LoggerInterface;
using ::google::scp::core::LogLevel;
using ::google::scp::core::ServiceInterface;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::GlobalLogger;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::logger::ConsoleLogProvider;
using ::google::scp::core::logger::FromString;
using ::google::scp::core::logger::Logger;
using ::google::scp::core::logger::log_providers::SyslogLogProvider;

std::shared_ptr<ConfigProviderInterface> config_provider;
std::shared_ptr<LookupServer> lookup_server;

constexpr char kComponentName[] = "LookupServerRunner";

absl::Status Init(std::shared_ptr<ServiceInterface> service,
                  std::string service_name) {
  ExecutionResult result = service->Init();
  if (!result.Successful()) {
    std::string err_message =
        absl::StrFormat("Service %s failed to initialize.", service_name);
    SCP_ERROR(kComponentName, kZeroUuid, result, "%s", err_message.c_str());
    return absl::InternalError(err_message);
  }

  SCP_INFO(
      kComponentName, kZeroUuid,
      absl::StrFormat("Service %s initialized successfully.", service_name));
  return absl::OkStatus();
}

absl::Status Run(std::shared_ptr<ServiceInterface> service,
                 std::string service_name) {
  ExecutionResult result = service->Run();
  if (!result.Successful()) {
    std::string err_message =
        absl::StrFormat("Service %s failed to run.", service_name);
    SCP_ERROR(kComponentName, kZeroUuid, result, "%s", err_message.c_str());
    return absl::InternalError(err_message);
  }

  SCP_INFO(kComponentName, kZeroUuid,
           absl::StrFormat("Service %s run successfully.", service_name));
  return absl::OkStatus();
}

int main(int argc, char** argv) {
  absl::InitializeSymbolizer(argv[0]);
  absl::InstallFailureSignalHandler(absl::FailureSignalHandlerOptions());
  std::signal(SIGINT, SIG_DFL);
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGCHLD, SIG_IGN);
  std::signal(SIGHUP, SIG_IGN);

  config_provider = std::make_shared<EnvConfigProvider>();
  config_provider->Init();

  std::list<std::string> enabled_log_levels;
  std::unordered_set<LogLevel> log_levels;

  if (config_provider->Get(kEnabledLogLevels, enabled_log_levels)
          .Successful()) {
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

  lookup_server = std::make_shared<LookupServer>(config_provider);

  CHECK_OK(Init(lookup_server, "LookupServer")) << "Initialization failed.";
  CHECK_OK(Run(lookup_server, "LookupServer")) << "Run failed.";

  while (true) {
    std::this_thread::sleep_for(std::chrono::minutes(1));
  }
  return 0;
}
