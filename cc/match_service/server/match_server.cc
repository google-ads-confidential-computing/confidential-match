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

#include "cc/match_service/server/match_server.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "cc/core/async_executor/src/async_executor.h"
#include "cc/core/curl_client/src/http1_curl_client.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/hash/sha256_hasher.h"
#include "cc/core/http2_client/src/http2_client.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/errors.h"
#include "cc/cpio/client_providers/cloud_initializer/src/aws/aws_initializer.h"
#include "cc/match_service/auth_token_client/auth_token_client.h"
#include "cc/match_service/auth_token_client/cached_auth_token_client.h"
#include "cc/match_service/crypto_client/aead_crypto_client.h"
#include "cc/match_service/crypto_client/hybrid_crypto_client.h"
#include "cc/match_service/kms_client/cached_kms_client.h"
#include "cc/match_service/kms_client/kms_client.h"
#include "cc/match_service/lookup_service_client/lookup_service_client.h"
#include "cc/match_service/lookup_service_shard_client/lookup_service_shard_client.h"
#include "cc/match_service/orchestrator_client/cached_orchestrator_client.h"
#include "cc/match_service/orchestrator_client/orchestrator_client.h"
#include "cc/match_service/service/match_service.h"
#include "cc/match_service/tasks/hashed_match_task.h"
#include "cc/match_service/tasks/kms_encrypted_match_task.h"
#include "cc/match_service/tasks/match_task.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/adapters/metric_client/src/metric_client.h"
#include "cc/public/cpio/adapters/private_key_client/src/private_key_client.h"
#include "cc/public/cpio/interface/cpio.h"
#include "cc/public/cpio/interface/kms_client/aws/aws_kms_client_factory.h"
#include "cc/public/cpio/interface/type_def.h"
#include "cc/public/cpio/utils/configuration_fetcher/src/configuration_fetcher.h"
#include "cc/public/cpio/utils/dual_writing_metric_client/noop/noop_dual_writing_metric_client.h"
#include "cc/public/cpio/utils/dual_writing_metric_client/src/dual_writing_metric_client.h"
#include "cc/public/cpio/utils/key_fetching/src/ondemand_key_fetcher_with_cache.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/time_util.h"
#include "grpcpp/ext/proto_server_reflection_plugin.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/health_check_service_interface.h"
#include "protos/match_service/backend/configuration/configuration_keys.pb.h"
#include "protos/match_service/backend/key_fetcher_options.pb.h"

namespace google::confidential_match::match_service {
namespace {

using backend::ConfigurationKeys;
using backend::ConfigurationKeys_Name;
using ::google::cmrt::sdk::v1::KeyCoordinatorConfiguration;
using ::google::protobuf::TextFormat;
using ::google::scp::core::AsyncExecutor;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::AsyncExecutorStats;
using ::google::scp::core::AsyncExecutorTaskLoadBalancingScheme;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::Http1CurlClient;
using ::google::scp::core::HttpClient;
using ::google::scp::core::ServiceInterface;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::cpio::AwsKmsClientFactory;
using ::google::scp::cpio::AwsKmsClientOptions;
using ::google::scp::cpio::ConfigurationFetcher;
using ::google::scp::cpio::ConfigurationFetcherInterface;
using ::google::scp::cpio::Cpio;
using ::google::scp::cpio::CpioOptions;
using ::google::scp::cpio::DualWritingMetricClient;
using ::google::scp::cpio::KmsClientFactory;
using ::google::scp::cpio::KmsClientOptions;
using ::google::scp::cpio::LogOption;
using ::google::scp::cpio::MetricClientFactory;
using ::google::scp::cpio::MetricClientOptions;
using ::google::scp::cpio::NoopDualWritingMetricClient;
using ::google::scp::cpio::OndemandKeyFetcherWithCache;
using ::google::scp::cpio::PrivateKeyClientFactory;
using ::google::scp::cpio::PrivateKeyClientOptions;
using ::google::scp::cpio::client_providers::AwsInitializer;
using ::grpc::experimental::AltsServerCredentials;
using ::grpc::experimental::AltsServerCredentialsOptions;

// Controls how often the background health check is run.
constexpr uint32_t kBackgroundHealthCheckIntervalSec = 60;

constexpr absl::string_view kParameterNamePrefix = "ms-";

// Configuration key for the max queue size of the CPU async executor.
constexpr char kCpuAsyncExecutorQueueSize[] = "cpu_async_executor_queue_size";
// Configuration key for the number of CPU async executor threads.
constexpr char kCpuAsyncExecutorThreadsCount[] =
    "cpu_async_executor_threads_count";
// Configuration key for the max queue size of the IO async executor.
constexpr char kIoAsyncExecutorQueueSize[] = "io_async_executor_queue_size";
// Configuration key for the number of CPIO IO async executor threads.
constexpr char kIoAsyncExecutorThreadsCount[] =
    "io_async_executor_threads_count";
constexpr char kMatchServiceAsyncExecutorQueueSize[] =
    "match_service_queue_size";
// Configuration key for the number of Match Service async executor threads.
constexpr char kMatchServiceAsyncExecutorThreadsCount[] =
    "match_service_thread_count";
// Configuration key for the GCP project id.
constexpr char kGcpProjectId[] = "gcp_project_id";
// Default audience for the AWS KMS client.
constexpr char kAwsKmsDefaultAudience[] = "aws_kms_default_audience";
// Default signatures for the AWS KMS client.
constexpr char kAwsKmsDefaultSignatures[] = "aws_kms_default_signatures";
// Configuration key for the Match Server host address.
constexpr char kHostAddress[] = "host_address";
// Configuration key for the Match Server host port.
constexpr char kHostPort[] = "host_port";
// Configuration key for the Orchestrator host address.
constexpr char kOrchestratorHostAddress[] = "orchestrator_host_address";
// Configuration key for the JWT audience used for requests to Lookup Service.
constexpr char kLookupServiceAuthAudience[] = "lookup_service_auth_audience";
// Configuration key for the Lookup Service cluster group ID.
constexpr char kLookupServiceClusterGroupId[] =
    "lookup_service_cluster_group_id";
// Configuration key to provide a comma-separated list of allowed client
// accounts.
constexpr char kAllowedClientAccounts[] = "allowed_client_accounts";
// Configures the maximum number of records to send to Lookup Service.
constexpr int kLookupServiceMaxRecordsPerRequest = 5000;
// The default region at creation time for the AwsKmsClient
constexpr absl::string_view kAwsDefaultRegion = "us-east-2";
// Config for CPIO GCP KMS client
constexpr std::chrono::milliseconds kGcpKmsClientRetryInitialInterval =
    std::chrono::milliseconds(200);
constexpr size_t kGcpKmsClientRetryTotalRetries = 5;
// Config for Metric client
constexpr std::chrono::milliseconds kMetricExporterInterval =
    std::chrono::milliseconds(60000);
// Configuration key for enabling MetricClient.
constexpr char kEnableMetricClient[] = "enable_metric_client";
// Configuration key for metric_namespace.
constexpr char kMetricNamespace[] = "metric_namespace";
// The address of the Otel collector.
constexpr char kCollectorAddress[] = "collector_address";

// Name of SCP services created by the server.
constexpr absl::string_view kConfigurationFetcherName = "ConfigurationFetcher";
constexpr absl::string_view kMetricClientName = "MetricClient";
constexpr absl::string_view kCpuAsyncExecutorName = "CpuAsyncExecutor";
constexpr absl::string_view kIoAsyncExecutorName = "IoAsyncExecutor";
constexpr absl::string_view kHttp1ClientServiceName = "HTTP1Client";
constexpr absl::string_view kHttp2ClientServiceName = "HTTP2Client";
constexpr absl::string_view kMatchServiceAsyncExecutorName =
    "MatchServiceAsyncExecutor";
constexpr absl::string_view kPrivateKeyClientServiceName = "PrivateKeyClient";
constexpr absl::string_view kOndemandKeyFetcherServiceName =
    "OndemandKeyFetcher";

// Read an environment variable as a std::string.
absl::StatusOr<std::string> GetEnvString(const char* name) noexcept {
  const char* value = getenv(name);
  if (value == nullptr) {
    return absl::NotFoundError(absl::StrCat("Env var ", name, " is not set"));
  }
  return std::string(value);
}

// read an environment variable as a bool.
absl::StatusOr<bool> GetEnvBool(const char* name) {
  std::string str;
  ASSIGN_OR_RETURN(str, GetEnvString(name));
  std::transform(str.begin(), str.end(), str.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  if (str == "true") {
    return true;
  } else if (str == "false") {
    return false;
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Bool env variable %s could not be parsed: %s", name, str));
  }
}

// read an environment variable as a comma-separated list of strings.
absl::StatusOr<std::vector<std::string>> GetEnvList(const char* name) noexcept {
  absl::StatusOr<std::string> value = GetEnvString(name);
  if (!value.ok()) {
    return std::move(value).status();
  }
  return absl::StrSplit(*std::move(value), ',');
}

// Read an environment variable as a std::size_t.
absl::StatusOr<std::size_t> GetEnvSize(const char* name) noexcept {
  absl::StatusOr<std::string> value = GetEnvString(name);
  if (!value.ok()) {
    return std::move(value).status();
  }
  std::size_t result;
  if (!absl::SimpleAtoi(*std::move(value), &result)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Env var ", name, " could not be parsed"));
  }
  return result;
}

// Helper to initialize CPIO.
absl::Status InitCpio(
    const std::shared_ptr<AsyncExecutorInterface>& cpu_async_executor,
    const std::shared_ptr<AsyncExecutorInterface>& io_async_executor,
    const std::unordered_set<scp::core::LogLevel>& enabled_log_levels) {
  CpioOptions cpio_options;
  cpio_options.log_option = LogOption::kConsoleLog;
  cpio_options.enabled_log_levels = enabled_log_levels;
  cpio_options.cpu_async_executor = cpu_async_executor;
  cpio_options.io_async_executor = io_async_executor;

  ExecutionResult result = Cpio::InitCpio(cpio_options);
  if (!result.Successful()) {
    auto message = absl::StrCat("Failed to initialize CPIO: ",
                                GetErrorMessage(result.status_code));
    LOG(ERROR) << message;
    return absl::InternalError(message);
  }
  LOG(INFO) << absl::StrCat("Successfully initialized CPIO");
  return absl::OkStatus();
}

// Helper to initialize an SCP service and log information.
absl::Status InitScpService(ServiceInterface& service,
                            absl::string_view service_name) {
  ExecutionResult result = service.Init();
  if (!result.Successful()) {
    auto message = absl::StrCat("Failed to initialize service ", service_name,
                                ": ", GetErrorMessage(result.status_code));
    LOG(ERROR) << message;
    return absl::InternalError(message);
  }
  LOG(INFO) << absl::StrCat("Successfully initialized service: ", service_name);
  return absl::OkStatus();
}

// Helper to run an SCP service and log information.
absl::Status RunScpService(ServiceInterface& service,
                           absl::string_view service_name) {
  ExecutionResult result = service.Run();
  if (!result.Successful()) {
    auto message = absl::StrCat("Failed to run service ", service_name, ": ",
                                GetErrorMessage(result.status_code));
    LOG(ERROR) << message;
    return absl::InternalError(message);
  }
  LOG(INFO) << absl::StrCat("Successfully ran service: ", service_name);
  return absl::OkStatus();
}

// Helper to stop an SCP service and log information.
absl::Status StopScpService(ServiceInterface& service,
                            absl::string_view service_name) {
  ExecutionResult result = service.Stop();
  if (!result.Successful()) {
    auto message = absl::StrCat("Failed to stop service ", service_name, ": ",
                                GetErrorMessage(result.status_code));
    LOG(ERROR) << message;
    return absl::InternalError(message);
  }
  LOG(INFO) << absl::StrCat("Successfully stopped service: ", service_name);
  return absl::OkStatus();
}

// TODO(b/492229483): Add validation logic for KeyFetcherOptions.
::google::scp::cpio::KeyFetcherOptions ConvertKeyFetcherOptions(
    const backend::KeyFetcherOptions& proto) {
  ::google::scp::cpio::KeyFetcherOptions options;
  options.prefetch_keys = proto.prefetch_keys();
  options.prefetch_retry = proto.prefetch_retry();

  options.max_prefetch_wait_time_millis = proto.max_prefetch_wait_time_millis();

  if (proto.has_prefetch_keys_max_age()) {
    options.prefetch_keys_max_age =
        std::chrono::seconds(proto.prefetch_keys_max_age().seconds());
  }
  if (proto.has_key_cache_lifetime()) {
    options.key_cache_lifetime =
        std::chrono::seconds(proto.key_cache_lifetime().seconds());
  }
  if (proto.has_fetching_failure_cache_lifetime()) {
    options.fetching_failure_cache_lifetime =
        std::chrono::seconds(proto.fetching_failure_cache_lifetime().seconds());
  }
  options.enable_key_fetching_metrics = proto.enable_key_fetching_metrics();
  options.enable_active_keys_api_for_encryption_keys =
      proto.enable_active_keys_api_for_encryption_keys();

  if (proto.has_auto_refresh_time_duration()) {
    options.auto_refresh_time_duration =
        std::chrono::seconds(proto.auto_refresh_time_duration().seconds());
  }
  options.enable_on_demand_fetching_for_hmac_key =
      proto.enable_on_demand_fetching_for_hmac_key();
  options.enable_on_demand_fetching_lock_for_encryption_key =
      proto.enable_on_demand_fetching_lock_for_encryption_key();

  if (proto.has_on_demand_fetching_waiting_timeout()) {
    options.on_demand_fetching_waiting_timeout = std::chrono::milliseconds(
        ::google::protobuf::util::TimeUtil::DurationToMilliseconds(
            proto.on_demand_fetching_waiting_timeout()));
  }
  options.use_read_lock_for_cache_read = proto.use_read_lock_for_cache_read();
  options.auto_refresh_key_cache_valid_in_days =
      proto.auto_refresh_key_cache_valid_in_days();
  options.enable_key_selection_timestamp_validation =
      proto.enable_key_selection_timestamp_validation();

  for (const auto& config :
       proto.encryption_key_prefetch_config().keyset_prefetch_configs()) {
    options.encryption_key_prefetch_config_map[config.key_namespace()] = config;
  }

  return options;
}

KeyCoordinatorConfiguration ConvertEndpoints(
    const backend::PrivateKeyEndpoints& endpoints) {
  KeyCoordinatorConfiguration config;
  config.mutable_private_key_endpoints()->Add(endpoints.endpoints().begin(),
                                              endpoints.endpoints().end());
  config.mutable_key_namespace()->Add(endpoints.key_namespaces().begin(),
                                      endpoints.key_namespaces().end());
  return config;
}

}  // namespace

absl::Status MatchServer::SetConfiguration() noexcept {
  ASSIGN_OR_RETURN(config_.enable_metric_client,
                   GetEnvBool(kEnableMetricClient));
  if (config_.enable_metric_client) {
    ASSIGN_OR_RETURN(config_.metric_namespace, GetEnvString(kMetricNamespace));
    ASSIGN_OR_RETURN(config_.otel_collector_address,
                     GetEnvString(kCollectorAddress));
  }
  ASSIGN_OR_RETURN(config_.cpu_async_executor_queue_size,
                   GetEnvSize(kCpuAsyncExecutorQueueSize));
  ASSIGN_OR_RETURN(config_.cpu_async_executor_threads_count,
                   GetEnvSize(kCpuAsyncExecutorThreadsCount));
  ASSIGN_OR_RETURN(config_.io_async_executor_queue_size,
                   GetEnvSize(kIoAsyncExecutorQueueSize));
  ASSIGN_OR_RETURN(config_.io_async_executor_threads_count,
                   GetEnvSize(kIoAsyncExecutorThreadsCount));
  ASSIGN_OR_RETURN(config_.match_service_async_executor_queue_size,
                   GetEnvSize(kMatchServiceAsyncExecutorQueueSize));
  ASSIGN_OR_RETURN(config_.match_service_async_executor_threads_count,
                   GetEnvSize(kMatchServiceAsyncExecutorThreadsCount));
  ASSIGN_OR_RETURN(config_.gcp_project_id, GetEnvString(kGcpProjectId));
  ASSIGN_OR_RETURN(config_.aws_kms_default_audience,
                   GetEnvString(kAwsKmsDefaultAudience));
  ASSIGN_OR_RETURN(config_.aws_kms_default_signatures,
                   GetEnvList(kAwsKmsDefaultSignatures));
  ASSIGN_OR_RETURN(config_.host_address, GetEnvString(kHostAddress));
  ASSIGN_OR_RETURN(config_.host_port, GetEnvString(kHostPort));
  ASSIGN_OR_RETURN(config_.orchestrator_host_address,
                   GetEnvString(kOrchestratorHostAddress));
  ASSIGN_OR_RETURN(config_.lookup_service_auth_audience,
                   GetEnvString(kLookupServiceAuthAudience));
  ASSIGN_OR_RETURN(config_.lookup_service_cluster_group_id,
                   GetEnvString(kLookupServiceClusterGroupId));
  ASSIGN_OR_RETURN(config_.allowed_client_accounts,
                   GetEnvList(kAllowedClientAccounts));

  return absl::OkStatus();
}

absl::Status MatchServer::SetConfigFromParameters() noexcept {
  DLOG(INFO) << "Fetching PrivateKeyEndpoints";
  auto private_key_endpoints_or =
      config_fetcher_->GetParameterByNameSync(ConfigurationKeys_Name(
          ConfigurationKeys::CONFIGURATION_KEYS_PRIVATE_KEY_ENDPOINTS));
  if (!private_key_endpoints_or.Successful()) {
    auto message = absl::StrCat(
        "Failed to get PrivateKeyEndpoints : ",
        GetErrorMessage(private_key_endpoints_or.result().status_code));
    LOG(ERROR) << message;
    // TODO(b/492229483) return error once coordinator key logic is on criticial
    // path.
    return absl::OkStatus();
  }
  if (!TextFormat::ParseFromString(private_key_endpoints_or.value(),
                                   &config_.private_key_endpoints)) {
    auto message = absl::StrCat("Failed to parse PrivateKeyEndpoints");
    LOG(ERROR) << message;
    // TODO(b/492229483) return error once coordinator key logic is on criticial
    // path.
    return absl::OkStatus();
  }

  DLOG(INFO) << "Fetching KeyFetcherOptions";
  auto key_fetcher_options_or =
      config_fetcher_->GetParameterByNameSync(ConfigurationKeys_Name(
          ConfigurationKeys::CONFIGURATION_KEYS_KEY_FETCHER_OPTIONS));
  if (!key_fetcher_options_or.Successful()) {
    auto message = absl::StrCat(
        "Failed to get KeyFetcherOptions: ",
        GetErrorMessage(key_fetcher_options_or.result().status_code));
    LOG(ERROR) << message;
    // TODO(b/492229483) return error once coordinator key logic is on criticial
    // path.
    return absl::OkStatus();
  }
  if (!TextFormat::ParseFromString(key_fetcher_options_or.value(),
                                   &config_.key_fetcher_options)) {
    auto message = absl::StrCat("Failed to parse KeyFetcherOptions");
    LOG(ERROR) << message;
    // TODO(b/492229483) return error once coordinator key logic is on criticial
    // path.
    return absl::OkStatus();
  }

  DLOG(INFO) << "Fetching AuthTokenCacheEntryLifetimeSeconds";
  auto auth_token_cache_entry_lifetime_seconds_or =
      config_fetcher_->GetParameterByNameSync(ConfigurationKeys_Name(
          ConfigurationKeys::
              CONFIGURATION_KEYS_AUTH_TOKEN_CACHE_ENTRY_LIFETIME_SECONDS));
  if (auth_token_cache_entry_lifetime_seconds_or.Successful()) {
    int value = 0;
    if (absl::SimpleAtoi(auth_token_cache_entry_lifetime_seconds_or.value(),
                         &value) &&
        value > 0) {
      config_.auth_token_cache_entry_lifetime_seconds = value;
      LOG(INFO) << "AuthTokenCacheEntryLifetimeSeconds is configured as: "
                << value << " seconds.";
    } else {
      LOG(WARNING)
          << "Failed to parse "
             "CONFIGURATION_KEYS_AUTH_TOKEN_CACHE_ENTRY_LIFETIME_SECONDS: "
          << auth_token_cache_entry_lifetime_seconds_or.value();
    }
  } else {
    LOG(INFO) << "CONFIGURATION_KEYS_AUTH_TOKEN_CACHE_ENTRY_LIFETIME_SECONDS "
                 "not found, using default: "
              << config_.auth_token_cache_entry_lifetime_seconds << " seconds.";
  }

  DLOG(INFO) << "Fetching DisableServiceAccountEmailRetrieval";
  const auto disable_retrieval_config_key = ConfigurationKeys::
      CONFIGURATION_KEYS_DISABLE_SERVICE_ACCOUNT_EMAIL_RETRIEVAL;
  auto disable_sa_email_retrieval_or = config_fetcher_->GetBoolByNameSync(
      ConfigurationKeys_Name(disable_retrieval_config_key));
  if (disable_sa_email_retrieval_or.Successful()) {
    config_.disable_service_account_email_retrieval =
        disable_sa_email_retrieval_or.value();
  } else {
    LOG(WARNING) << "Failed to fetch "
                    "CONFIGURATION_KEYS_DISABLE_SERVICE_ACCOUNT_EMAIL_"
                    "RETRIEVAL: "
                 << GetErrorMessage(
                        disable_sa_email_retrieval_or.result().status_code);
  }

  LOG(INFO) << "DisableServiceAccountEmailRetrieval: "
            << (config_.disable_service_account_email_retrieval ? "true"
                                                                : "false");

  if (config_.disable_service_account_email_retrieval) {
    DLOG(INFO) << "Fetching ServiceAccountEmail";
    auto sa_email_or =
        config_fetcher_->GetParameterByNameSync(ConfigurationKeys_Name(
            ConfigurationKeys::CONFIGURATION_KEYS_SERVICE_ACCOUNT_EMAIL));
    if (sa_email_or.Successful()) {
      config_.service_account_email = sa_email_or.value();
      LOG(INFO) << "ServiceAccountEmail: " << config_.service_account_email;
    } else {
      LOG(ERROR) << "Failed to get ServiceAccountEmail: "
                 << GetErrorMessage(sa_email_or.result().status_code);
      return absl::InternalError("Failed to get ServiceAccountEmail.");
    }
  }

  DLOG(INFO) << "Fetching EnableBackgroundAuthTokenRefresh";
  const auto enable_refresh_config_key = ConfigurationKeys::
      CONFIGURATION_KEYS_ENABLE_BACKGROUND_AUTH_TOKEN_REFRESH;
  auto enable_refresh_or = config_fetcher_->GetBoolByNameSync(
      ConfigurationKeys_Name(enable_refresh_config_key));
  if (enable_refresh_or.Successful()) {
    config_.enable_background_auth_token_refresh = enable_refresh_or.value();
  } else {
    LOG(INFO) << "CONFIGURATION_KEYS_ENABLE_BACKGROUND_AUTH_TOKEN_REFRESH "
              << "not found, using default: "
              << (config_.enable_background_auth_token_refresh ? "true"
                                                               : "false");
  }

  LOG(INFO) << "EnableBackgroundAuthTokenRefresh: "
            << (config_.enable_background_auth_token_refresh ? "true"
                                                            : "false");

  return absl::OkStatus();
}

absl::Status MatchServer::CreateComponents() noexcept {
  // Secrets will be of the format "ms-<env>-<secret_name>".
  // "ms-" is static for all envs, the <env> will be acquired by the instance
  // label, so we supply std::nullopt for the environment_name_label.
  config_fetcher_ = std::make_shared<ConfigurationFetcher>(
      std::string(kParameterNamePrefix), std::nullopt);
  RETURN_IF_ERROR(InitScpService(*config_fetcher_, kConfigurationFetcherName));
  RETURN_IF_ERROR(RunScpService(*config_fetcher_, kConfigurationFetcherName));
  RETURN_IF_ERROR(SetConfigFromParameters());
  if (config_.enable_metric_client) {
    MetricClientOptions metric_client_options;
    metric_client_options.enable_remote_metric_aggregation = true;
    metric_client_options.metric_exporter_interval = kMetricExporterInterval;
    metric_client_options.namespace_for_batch_recording =
        config_.metric_namespace;
    metric_client_options.remote_metric_collector_address =
        config_.otel_collector_address;
    metric_client_ = MetricClientFactory::Create(metric_client_options);
    dual_writing_metric_client_ = std::make_unique<DualWritingMetricClient>(
        metric_client_.get(), nullptr, config_.metric_namespace);
  } else {
    dual_writing_metric_client_ =
        std::make_unique<NoopDualWritingMetricClient>();
  }
  cpu_async_executor_ = std::make_shared<AsyncExecutor>(
      config_.cpu_async_executor_threads_count,
      config_.cpu_async_executor_queue_size,
      /* drop_tasks_on_stop= */ false,
      AsyncExecutorTaskLoadBalancingScheme::RoundRobinGlobal,
      /* enable_stats_keeping= */ true);

  io_async_executor_ = std::make_shared<AsyncExecutor>(
      config_.io_async_executor_threads_count,
      config_.io_async_executor_queue_size,
      /* drop_tasks_on_stop= */ false,
      AsyncExecutorTaskLoadBalancingScheme::RoundRobinGlobal,
      /* enable_stats_keeping= */ true);

  match_service_async_executor_ = std::make_shared<AsyncExecutor>(
      config_.match_service_async_executor_threads_count,
      config_.match_service_async_executor_queue_size,
      /* drop_tasks_on_stop= */ false,
      AsyncExecutorTaskLoadBalancingScheme::RoundRobinGlobal,
      /* enable_stats_keeping= */ true);

  MatchServiceOptions service_options = {
      .allowed_client_accounts = config_.allowed_client_accounts,
  };

  http1_client_ = std::make_unique<Http1CurlClient>(cpu_async_executor_,
                                                    io_async_executor_);
  http2_client_ = std::make_unique<HttpClient>(cpu_async_executor_);
  auth_token_client_ = std::make_shared<AuthTokenClient>(http1_client_.get());
  cached_auth_token_client_ = std::make_unique<CachedAuthTokenClient>(
      cpu_async_executor_, auth_token_client_,
      config_.auth_token_cache_entry_lifetime_seconds,
      config_.orchestrator_host_address,
      config_.lookup_service_auth_audience,
      config_.enable_background_auth_token_refresh);
  orchestrator_client_ = std::make_shared<OrchestratorClient>(
      cached_auth_token_client_.get(), http2_client_.get(),
      config_.orchestrator_host_address);
  cached_orchestrator_client_ = std::make_unique<CachedOrchestratorClient>(
      cpu_async_executor_, orchestrator_client_);
  KmsClientOptions gcp_kms_client_options;
  gcp_kms_client_options.enable_gcp_kms_client_retries = true;
  gcp_kms_client_options.gcp_kms_client_retry_initial_interval =
      kGcpKmsClientRetryInitialInterval;
  gcp_kms_client_options.gcp_kms_client_retry_total_retries =
      kGcpKmsClientRetryTotalRetries;
  gcp_kms_client_options.enable_new_gcp_error_code_converter = true;
  gcp_kms_client_ = std::make_unique<KmsClient>(
      KmsClientFactory::Create(gcp_kms_client_options));
  cached_gcp_kms_client_ = std::make_unique<CachedKmsClient>(
      cpu_async_executor_, gcp_kms_client_.get());
  AwsKmsClientOptions aws_kms_client_options;
  aws_kms_client_options.region = kAwsDefaultRegion;
  aws_kms_client_ = std::make_unique<KmsClient>(
      AwsKmsClientFactory::Create(std::move(aws_kms_client_options)));
  cached_aws_kms_client_ = std::make_unique<CachedKmsClient>(
      cpu_async_executor_, aws_kms_client_.get());
  aead_crypto_client_ = std::make_unique<AeadCryptoClient>(
      cached_aws_kms_client_.get(), cached_gcp_kms_client_.get(),
      config_.aws_kms_default_signatures, config_.aws_kms_default_audience);
  lookup_service_shard_client_ = std::make_unique<LookupServiceShardClient>(
      cached_auth_token_client_.get(), http2_client_.get(),
      config_.lookup_service_auth_audience,
      config_.disable_service_account_email_retrieval,
      config_.service_account_email);
  lookup_service_client_ = std::make_unique<LookupServiceClient>(
      cached_orchestrator_client_.get(), lookup_service_shard_client_.get(),
      kLookupServiceMaxRecordsPerRequest,
      config_.lookup_service_cluster_group_id, metric_client_.get(),
      config_.metric_namespace);
  sha256_hasher_ = std::make_unique<Sha256Hasher>();

  PrivateKeyClientOptions private_key_client_options;
  private_key_client_options.enable_gcp_kms_client_retries = true;
  private_key_client_options.enable_new_gcp_error_code_converter = true;
  private_key_client_options.enable_gcp_kms_client_cache = true;
  private_key_client_options.gcp_kms_client_retry_initial_interval =
      kGcpKmsClientRetryInitialInterval;
  private_key_client_options.gcp_kms_client_retry_total_retries =
      kGcpKmsClientRetryTotalRetries;
  private_key_client_ =
      PrivateKeyClientFactory::Create(private_key_client_options);

  key_fetcher_ = std::make_unique<OndemandKeyFetcherWithCache>(
      cpu_async_executor_, *private_key_client_, *dual_writing_metric_client_,
      ConvertEndpoints(config_.private_key_endpoints),
      ConvertKeyFetcherOptions(config_.key_fetcher_options));

  hybrid_crypto_client_ =
      std::make_unique<HybridCryptoClient>(key_fetcher_.get());

  hashed_match_task_ = std::make_unique<HashedMatchTask>(
      lookup_service_client_.get(), sha256_hasher_.get());
  kms_encrypted_match_task_ = std::make_unique<KmsEncryptedMatchTask>(
      lookup_service_client_.get(), aead_crypto_client_.get(),
      sha256_hasher_.get(), hybrid_crypto_client_.get(),
      config_.private_key_endpoints, metric_client_.get(),
      config_.metric_namespace);
  match_task_ = std::make_unique<MatchTask>(hashed_match_task_.get(),
                                            kms_encrypted_match_task_.get());
  match_service_ = std::make_unique<MatchService>(
      match_service_async_executor_.get(), match_task_.get(),
      metric_client_.get(), config_.metric_namespace,
      std::move(service_options));
  return absl::OkStatus();
}

void MatchServer::RunGrpcServer() noexcept {
  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // TODO: Set ports from constants
  const auto server_address =
      absl::StrFormat("%s:%s", config_.host_address, config_.host_port);
  const std::string healthcheck_address = "[::]:50052";
  grpc::ServerBuilder builder;
  builder.RegisterService(match_service_.get());
  builder.AddListeningPort(
      server_address, AltsServerCredentials(AltsServerCredentialsOptions()));
  builder.AddListeningPort(healthcheck_address,
                           grpc::InsecureServerCredentials());
  grpc_server_ = builder.BuildAndStart();
  LOG(INFO) << "Server listening on ALTS port " << server_address;
  LOG(INFO) << "Healthcheck service also listening on unauthenticated port "
            << healthcheck_address;
  // Note that another thread must be responsible for shutting down the server
  // for this call to ever return.
  grpc_server_->Wait();
}

absl::Status MatchServer::Init() noexcept {
  RETURN_IF_ERROR(SetConfiguration());
  RETURN_IF_ERROR(CreateComponents());

  LOG(INFO) << "Initializing Match Server components...";
  aws_cloud_init_.InitCloud();
  RETURN_IF_ERROR(
      InitCpio(cpu_async_executor_, io_async_executor_, enabled_log_levels_));
  if (metric_client_ != nullptr) {
    RETURN_IF_ERROR(InitScpService(*metric_client_, kMetricClientName));
  }
  RETURN_IF_ERROR(InitScpService(*cpu_async_executor_, kCpuAsyncExecutorName));
  RETURN_IF_ERROR(InitScpService(*io_async_executor_, kIoAsyncExecutorName));
  RETURN_IF_ERROR(InitScpService(*match_service_async_executor_,
                                 kMatchServiceAsyncExecutorName));
  RETURN_IF_ERROR(InitScpService(*http1_client_, kHttp1ClientServiceName));
  RETURN_IF_ERROR(InitScpService(*http2_client_, kHttp2ClientServiceName));
  RETURN_IF_ERROR(
      InitScpService(*private_key_client_, kPrivateKeyClientServiceName));
  RETURN_IF_ERROR(
      InitScpService(*key_fetcher_, kOndemandKeyFetcherServiceName));
  RETURN_IF_ERROR(gcp_kms_client_->Init());
  RETURN_IF_ERROR(cached_gcp_kms_client_->Init());
  RETURN_IF_ERROR(aws_kms_client_->Init());
  RETURN_IF_ERROR(cached_aws_kms_client_->Init());
  RETURN_IF_ERROR(aead_crypto_client_->Init());
  RETURN_IF_ERROR(hybrid_crypto_client_->Init());
  RETURN_IF_ERROR(auth_token_client_->Init());
  RETURN_IF_ERROR(cached_auth_token_client_->Init());
  RETURN_IF_ERROR(orchestrator_client_->Init());
  RETURN_IF_ERROR(cached_orchestrator_client_->Init());
  RETURN_IF_ERROR(lookup_service_shard_client_->Init());
  RETURN_IF_ERROR(lookup_service_client_->Init());
  LOG(INFO) << "Match Server initialized";
  return absl::OkStatus();
}

absl::Status MatchServer::Run() noexcept {
  if (is_running_) {
    LOG(WARNING) << "Match Server already running";
    return absl::OkStatus();
  }
  LOG(INFO) << "Running Match Server components...";
  if (metric_client_ != nullptr) {
    RETURN_IF_ERROR(RunScpService(*metric_client_, kMetricClientName));
  }
  RETURN_IF_ERROR(RunScpService(*cpu_async_executor_, kCpuAsyncExecutorName));
  RETURN_IF_ERROR(RunScpService(*io_async_executor_, kIoAsyncExecutorName));
  RETURN_IF_ERROR(RunScpService(*match_service_async_executor_,
                                kMatchServiceAsyncExecutorName));
  RETURN_IF_ERROR(RunScpService(*http1_client_, kHttp1ClientServiceName));
  RETURN_IF_ERROR(RunScpService(*http2_client_, kHttp2ClientServiceName));
  RETURN_IF_ERROR(
      RunScpService(*private_key_client_, kPrivateKeyClientServiceName));
  RETURN_IF_ERROR(RunScpService(*key_fetcher_, kOndemandKeyFetcherServiceName));
  RETURN_IF_ERROR(gcp_kms_client_->Run());
  RETURN_IF_ERROR(cached_gcp_kms_client_->Run());
  RETURN_IF_ERROR(aws_kms_client_->Run());
  RETURN_IF_ERROR(cached_aws_kms_client_->Run());
  RETURN_IF_ERROR(aead_crypto_client_->Run());
  RETURN_IF_ERROR(hybrid_crypto_client_->Run());
  RETURN_IF_ERROR(auth_token_client_->Run());
  RETURN_IF_ERROR(cached_auth_token_client_->Run());
  RETURN_IF_ERROR(orchestrator_client_->Run());
  RETURN_IF_ERROR(cached_orchestrator_client_->Run());
  RETURN_IF_ERROR(lookup_service_shard_client_->Run());
  RETURN_IF_ERROR(lookup_service_client_->Run());
  is_running_ = true;

  // Start a thread to monitor the health of the service.
  std::promise<void> thread_started_promise;
  health_checking_thread_ =
      std::make_unique<std::thread>([this, &thread_started_promise] {
        thread_started_promise.set_value();
        BackgroundHealthCheckLoop();
      });
  thread_started_promise.get_future().wait();

  RunGrpcServer();  // Blocking call.
  return absl::OkStatus();
}

absl::Status MatchServer::Stop() noexcept {
  if (!is_running_) {
    LOG(INFO) << "Match Server not running";
    return absl::OkStatus();
  }
  LOG(INFO) << "Stopping Match Server components...";
  if (grpc_server_) {
    grpc_server_->Shutdown();
    LOG(INFO) << "Stopped gRPC server";
  }
  RETURN_IF_ERROR(lookup_service_client_->Stop());
  RETURN_IF_ERROR(lookup_service_shard_client_->Stop());
  RETURN_IF_ERROR(cached_orchestrator_client_->Stop());
  RETURN_IF_ERROR(orchestrator_client_->Stop());
  RETURN_IF_ERROR(cached_auth_token_client_->Stop());
  RETURN_IF_ERROR(auth_token_client_->Stop());
  RETURN_IF_ERROR(aead_crypto_client_->Stop());
  RETURN_IF_ERROR(hybrid_crypto_client_->Stop());
  RETURN_IF_ERROR(cached_aws_kms_client_->Stop());
  RETURN_IF_ERROR(aws_kms_client_->Stop());
  RETURN_IF_ERROR(cached_gcp_kms_client_->Stop());
  RETURN_IF_ERROR(gcp_kms_client_->Stop());
  RETURN_IF_ERROR(
      StopScpService(*key_fetcher_, kOndemandKeyFetcherServiceName));
  RETURN_IF_ERROR(
      StopScpService(*private_key_client_, kPrivateKeyClientServiceName));
  RETURN_IF_ERROR(StopScpService(*http2_client_, kHttp2ClientServiceName));
  RETURN_IF_ERROR(StopScpService(*http1_client_, kHttp1ClientServiceName));
  RETURN_IF_ERROR(StopScpService(*match_service_async_executor_,
                                 kMatchServiceAsyncExecutorName));
  RETURN_IF_ERROR(StopScpService(*io_async_executor_, kIoAsyncExecutorName));
  RETURN_IF_ERROR(StopScpService(*cpu_async_executor_, kCpuAsyncExecutorName));
  if (metric_client_ != nullptr) {
    RETURN_IF_ERROR(StopScpService(*metric_client_, kMetricClientName));
  }
  RETURN_IF_ERROR(StopScpService(*config_fetcher_, kConfigurationFetcherName));
  aws_cloud_init_.ShutdownCloud();
  is_running_ = false;
  LOG(INFO) << "Stopped Match Server";
  return absl::OkStatus();
}

void MatchServer::BackgroundHealthCheckLoop() noexcept {
  std::chrono::time_point next_check_time = std::chrono::steady_clock::now();

  while (is_running_) {
    if (std::chrono::steady_clock::now() < next_check_time) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    BackgroundHealthCheck();
    next_check_time = std::chrono::steady_clock::now() +
                      std::chrono::seconds(kBackgroundHealthCheckIntervalSec);
  }
}

void MatchServer::BackgroundHealthCheck() noexcept {
  if (cpu_async_executor_ != nullptr) {
    AsyncExecutorStats previous_stats = cpu_async_executor_stats_;
    cpu_async_executor_stats_ = cpu_async_executor_->GetStatistics();
    LogAsyncExecutorMetrics("CPUAsyncExecutor", cpu_async_executor_stats_,
                            previous_stats);
  }
  if (io_async_executor_ != nullptr) {
    AsyncExecutorStats previous_stats = io_async_executor_stats_;
    io_async_executor_stats_ = io_async_executor_->GetStatistics();
    LogAsyncExecutorMetrics("IOAsyncExecutor", io_async_executor_stats_,
                            previous_stats);
  }
  if (match_service_async_executor_ != nullptr) {
    AsyncExecutorStats previous_stats = match_service_async_executor_stats_;
    match_service_async_executor_stats_ =
        match_service_async_executor_->GetStatistics();
    LogAsyncExecutorMetrics("MatchServiceAsyncExecutor",
                            match_service_async_executor_stats_,
                            previous_stats);
  }
}

void MatchServer::LogAsyncExecutorMetrics(
    absl::string_view name, const AsyncExecutorStats& stats,
    const AsyncExecutorStats& previous_stats) noexcept {
  size_t normal_tasks_executed_diff = stats.num_normal_tasks_executed -
                                      previous_stats.num_normal_tasks_executed;
  size_t high_tasks_executed_diff =
      stats.num_high_tasks_executed - previous_stats.num_high_tasks_executed;
  size_t urgent_tasks_executed_diff = stats.num_urgent_tasks_executed -
                                      previous_stats.num_urgent_tasks_executed;

  LOG(INFO) << absl::StrFormat(
      "BackgroundHealthCheck: %s tasks executed, last %ds: normal: "
      "%d, high: %d, urgent: %d. "
      "(All time: normal: %d, high: %d, urgent: %d)",
      name, kBackgroundHealthCheckIntervalSec, normal_tasks_executed_diff,
      high_tasks_executed_diff, urgent_tasks_executed_diff,
      stats.num_normal_tasks_executed, stats.num_high_tasks_executed,
      stats.num_urgent_tasks_executed);
  LOG(INFO) << absl::StrFormat(
      "BackgroundHealthCheck: %s current queue sizes: "
      "normal: %d, high: %d, urgent: %d",
      name, stats.normal_task_queue_size, stats.high_task_queue_size,
      stats.urgent_task_queue_size);
}

}  // namespace google::confidential_match::match_service
