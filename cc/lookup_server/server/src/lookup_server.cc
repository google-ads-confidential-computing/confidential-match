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

#include "cc/lookup_server/server/src/lookup_server.h"

#include <list>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/async_executor/src/async_executor.h"
#include "cc/core/authorization_proxy/src/pass_thru_authorization_proxy.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/config_provider/src/config_provider.h"
#include "cc/core/config_provider/src/env_config_provider.h"
#include "cc/core/curl_client/src/http1_curl_client.h"
#include "cc/core/http2_client/src/http2_client.h"
#include "cc/core/http2_server/src/http2_server.h"
#include "cc/core/interface/type_def.h"
#include "cc/core/logger/src/log_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"
#include "cc/public/cpio/interface/cloud_initializer/cloud_initializer_factory.h"
#include "cc/public/cpio/interface/cpio.h"
#include "cc/public/cpio/interface/kms_client/aws/aws_kms_client_factory.h"
#include "cc/public/cpio/interface/kms_client/aws/type_def.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "cc/public/cpio/interface/parameter_client/parameter_client_interface.h"
#include "cc/public/cpio/utils/metric_instance/interface/metric_instance_factory_interface.h"
#include "cc/public/cpio/utils/metric_instance/src/metric_instance_factory.h"

#include "cc/lookup_server/auth/src/jwt_validator.h"
#include "cc/lookup_server/coordinator_client/src/cached_coordinator_client.h"
#include "cc/lookup_server/coordinator_client/src/coordinator_client.h"
#include "cc/lookup_server/crypto_client/src/aead_crypto_client.h"
#include "cc/lookup_server/crypto_client/src/hpke_crypto_client.h"
#include "cc/lookup_server/interface/cloud_platform_dependency_factory_interface.h"
#include "cc/lookup_server/interface/configuration_keys.h"
#include "cc/lookup_server/kms_client/src/cached_kms_client.h"
#include "cc/lookup_server/kms_client/src/kms_client.h"
#include "cc/lookup_server/match_data_loader/src/match_data_loader.h"
#include "cc/lookup_server/match_data_provider/src/blob_storage_data_provider.h"
#include "cc/lookup_server/match_data_provider/src/blob_storage_match_data_provider.h"
#include "cc/lookup_server/match_data_storage/src/in_memory_match_data_storage.h"
#include "cc/lookup_server/metric_client/src/metric_client.h"
#include "cc/lookup_server/orchestrator_client/src/orchestrator_client.h"
#include "cc/lookup_server/parameter_client/src/parameter_client.h"
#include "cc/lookup_server/server/src/error_codes.h"
#include "cc/lookup_server/service/src/lookup_service.h"

#if defined(CLOUD_PLATFORM_GCP)
#include "cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp/gcp_dependency_factory.h"
#elif defined(CLOUD_PLATFORM_GCP_WITH_TEST_CPIO)
#include "cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp/gcp_dependency_factory.h"
#include "public/cpio/test/global_cpio/test_lib_cpio.h"
#elif defined(CLOUD_PLATFORM_GCP_INTEGRATION_TEST)
#include "cc/lookup_server/server/src/cloud_platform_dependency_factory/gcp/gcp_dependency_factory.h"
#elif defined(CLOUD_PLATFORM_LOCAL)
#include "cc/lookup_server/server/src/cloud_platform_dependency_factory/local/local_dependency_factory.h"
#endif

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::kEnabledLogLevels;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutor;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::AuthorizationProxyInterface;
using ::google::scp::core::ConfigProvider;
using ::google::scp::core::ConfigProviderInterface;
using ::google::scp::core::EnvConfigProvider;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::Http1CurlClient;
using ::google::scp::core::Http2Server;
using ::google::scp::core::Http2ServerOptions;
using ::google::scp::core::HttpClient;
using ::google::scp::core::HttpClientInterface;
using ::google::scp::core::HttpServerInterface;
using ::google::scp::core::LogLevel;
using ::google::scp::core::PassThruAuthorizationProxy;
using ::google::scp::core::ServiceInterface;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::TimeDuration;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::common::RetryStrategyOptions;
using ::google::scp::core::common::RetryStrategyType;
using ::google::scp::cpio::AwsKmsClientFactory;
using ::google::scp::cpio::AwsKmsClientOptions;
using ::google::scp::cpio::BlobStorageClientFactory;
using ::google::scp::cpio::BlobStorageClientInterface;
using ::google::scp::cpio::BlobStorageClientOptions;
using ::google::scp::cpio::CloudInitializerFactory;
using ::google::scp::cpio::Cpio;
using ::google::scp::cpio::CpioOptions;
using ::google::scp::cpio::KmsClientFactory;
using ::google::scp::cpio::KmsClientOptions;
using ::google::scp::cpio::LogOption;
using ::google::scp::cpio::MetricClientFactory;
using ::google::scp::cpio::MetricClientOptions;
using ::google::scp::cpio::MetricInstanceFactory;
using ::google::scp::cpio::ParameterClientFactory;
using ::google::scp::cpio::ParameterClientOptions;
using ::google::scp::cpio::PrivateKeyClientFactory;
using ::google::scp::cpio::PrivateKeyClientOptions;

using CfmMetricClient =
    ::google::confidential_match::lookup_server::MetricClient;
using CpioMetricClientInterface = ::google::scp::cpio::MetricClientInterface;
using CfmParameterClient =
    ::google::confidential_match::lookup_server::ParameterClient;
using CpioParameterClientInterface =
    ::google::scp::cpio::ParameterClientInterface;

constexpr absl::string_view kComponentName = "LookupServer";
constexpr absl::string_view kHttp1ClientServiceName = "HTTP1Client";
constexpr absl::string_view kHttp2ClientServiceName = "HTTP2Client";
constexpr absl::string_view kHttpServerServiceName = "HTTPServer";
constexpr absl::string_view kHealthHttpServerName = "HealthHTTPServer";
constexpr absl::string_view kLookupServiceName = "LookupService";
constexpr absl::string_view kHealthServiceName = "HealthService";
constexpr absl::string_view kAsyncExecutorServiceName = "AsyncExecutor";
constexpr absl::string_view kIoAsyncExecutorServiceName = "IoAsyncExecutor";
constexpr absl::string_view kOrchestratorClientName = "OrchestratorClient";
constexpr absl::string_view kAeadCryptoClientName = "AeadCryptoClient";
constexpr absl::string_view kHpkeCryptoClientName = "HpkeCryptoClient";
constexpr absl::string_view kMatchDataLoaderServiceName = "MatchDataLoader";
constexpr absl::string_view kDataProviderServiceName = "DataProvider";
constexpr absl::string_view kStreamedMatchDataProviderServiceName =
    "StreamedMatchDataProvider";
constexpr absl::string_view kMatchDataStorageServiceName = "MatchDataStorage";
constexpr absl::string_view kAwsKmsClientName = "AwsKmsClient";
constexpr absl::string_view kAwsCachedKmsClientName = "AwsCachedKmsClient";
constexpr absl::string_view kGcpKmsClientName = "GcpKmsClient";
constexpr absl::string_view kGcpCachedKmsClientName = "GcpCachedKmsClient";
constexpr absl::string_view kPrivateKeyClientName = "PrivateKeyClient";
constexpr absl::string_view kCoordinatorClientName = "CoordinatorClient";
constexpr absl::string_view kCachedCoordinatorClientName =
    "CachedCoordinatorClient";
constexpr absl::string_view kAuthorizationProxyServiceName =
    "AuthorizationProxy";
constexpr absl::string_view kPassThruAuthorizationProxyServiceName =
    "PassThruAuthorizationProxy";
constexpr absl::string_view kBlobStorageClientServiceName = "BlobStorageClient";
constexpr absl::string_view kMetricClientName = "MetricClient";
constexpr absl::string_view kParameterClientName = "ParameterClient";

// Configures the namespace used by Metric Client.
constexpr absl::string_view kMetricClientNamespace = "gce_instance";
constexpr absl::string_view kMetricClientClusterIdLabelKey = "cluster_id";
constexpr absl::string_view kMetricClientClusterGroupIdLabelKey =
    "cluster_group_id";

// Default configuration values when running with test CPIO.
constexpr absl::string_view kTestCpioDefaultProjectId =
    "admcloud-cm-data-staging";
constexpr absl::string_view kTestCpioDefaultRegion = "us-central1";
constexpr absl::string_view kTestCpioDefaultInstanceId = "1";

// Configures how long the CPIO blob storage client waits before timing out.
constexpr std::chrono::seconds kBlobStorageClientTransferStallTimeout =
    std::chrono::seconds(60 * 15);
// Configures the maximum number of retries for the CPIO blob storage client.
constexpr size_t kBlobStorageClientRetryLimit = 10;
// Configures the SCP Http2Server retry delay.
static constexpr TimeDuration kHttpServerRetryStrategyDelayInMs = 31;
// Configures the metric namespace used by Http2Server when recording metrics.
constexpr char kHttp2ServerMetricNamespace[] = "gce_instance";
// Configures the metric name used by Lookup Service Http2Server.
constexpr char kHttp2ServerLookupServiceMetricName[] = "HttpRequest";
// Configures the metric name used by Healthcheck Service Http2Server.
constexpr char kHttp2ServerHealthServiceMetricName[] =
    "HealthServiceHttpRequest";

// The issuer to use for authentication of incoming request JWTs.
constexpr absl::string_view kJwtIssuer = "https://accounts.google.com";

// The default region at creation time for the AwsKmsClient
constexpr absl::string_view kAwsDefaultRegion = "us-east-2";

// Helper to initialize a service and log information.
ExecutionResult InitService(ServiceInterface& service,
                            absl::string_view service_name) {
  ExecutionResult result = service.Init();
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 absl::StrCat("Failed to initialize service: ", service_name));
  } else {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrCat("Successfully initialized service: ", service_name));
  }
  return result;
}

// Helper to run a service and log information.
ExecutionResult RunService(ServiceInterface& service,
                           absl::string_view service_name) {
  ExecutionResult result = service.Run();
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 absl::StrCat("Failed to run service: ", service_name));
  } else {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrCat("Successfully ran service: ", service_name));
  }
  return result;
}

// Helper to stop a service and log information.
ExecutionResult StopService(ServiceInterface& service,
                            absl::string_view service_name) {
  ExecutionResult result = service.Stop();
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 absl::StrCat("Failed to stop service: ", service_name));
  } else {
    SCP_INFO(kComponentName, kZeroUuid,
             absl::StrCat("Successfully stopped service: ", service_name));
  }
  return result;
}

// Returns the proper dependency factory (GCP, local, ...) based on
// compiler flags that are applied during preprocessing.
std::unique_ptr<CloudPlatformDependencyFactoryInterface>
GetPlatformDependencyFactoryForCurrentEnv(
    std::shared_ptr<ConfigProviderInterface> config_provider) {
#if defined(CLOUD_PLATFORM_GCP)
  SCP_DEBUG(kComponentName, kZeroUuid,
            "Running Lookup Server with GCP dependencies.");
  auto platform_dependency_factory =
      std::make_unique<GcpDependencyFactory>(config_provider);
#elif defined(CLOUD_PLATFORM_GCP_WITH_TEST_CPIO)
  SCP_DEBUG(kComponentName, kZeroUuid,
            "Running Lookup Server with GCP dependencies and test CPIO.");
  auto platform_dependency_factory =
      std::make_unique<GcpDependencyFactory>(config_provider);
#elif defined(CLOUD_PLATFORM_GCP_INTEGRATION_TEST)
  SCP_DEBUG(kComponentName, kZeroUuid,
            "Running Lookup Server with GCP integration test dependencies.");
  auto platform_dependency_factory =
      std::make_unique<GcpDependencyFactory>(config_provider);
#elif defined(CLOUD_PLATFORM_LOCAL)
  SCP_DEBUG(kComponentName, kZeroUuid,
            "Running Lookup Server with local dependencies.");
  auto platform_dependency_factory =
      std::make_unique<LocalDependencyFactory>(config_provider);
#endif
  return platform_dependency_factory;
}

ExecutionResultOr<std::unique_ptr<CpioOptions>> InitCpio(
    std::shared_ptr<AsyncExecutorInterface> async_executor,
    std::shared_ptr<AsyncExecutorInterface> io_async_executor,
    std::unordered_set<LogLevel> log_levels) {
#if defined(CLOUD_PLATFORM_GCP_WITH_TEST_CPIO)
  auto test_cpio_options =
      std::make_shared<google::scp::cpio::TestCpioOptions>();
  test_cpio_options->owner_id = kTestCpioDefaultProjectId;
  test_cpio_options->zone = kTestCpioDefaultRegion;
  test_cpio_options->region = kTestCpioDefaultRegion;
  test_cpio_options->instance_id = kTestCpioDefaultInstanceId;
  test_cpio_options->log_option = LogOption::kConsoleLog;
  test_cpio_options->enabled_log_levels = log_levels;
  test_cpio_options->cpu_async_executor = async_executor;
  test_cpio_options->io_async_executor = io_async_executor;

  RETURN_IF_FAILURE(
      google::scp::cpio::TestLibCpio::InitCpio(*test_cpio_options));

  return std::make_unique<CpioOptions>(test_cpio_options->ToCpioOptions());
#else
  auto cpio_options = std::make_unique<CpioOptions>();
  cpio_options->log_option = LogOption::kConsoleLog;
  cpio_options->cpu_async_executor = async_executor;
  cpio_options->io_async_executor = io_async_executor;
  cpio_options->enabled_log_levels = log_levels;

  RETURN_IF_FAILURE(Cpio::InitCpio(*cpio_options));

  return cpio_options;
#endif
}

}  // namespace

ExecutionResult LookupServer::CreateAndInitCpioOptions() noexcept {
  ExecutionResult result =
      config_provider_->Get(std::string(kAsyncExecutorQueueSize),
                            cpio_options_config_.async_executor_queue_size);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read async executor queue size.");
    return result;
  }

  result = config_provider_->Get(
      std::string(kAsyncExecutorThreadsCount),
      cpio_options_config_.async_executor_thread_pool_size);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read async executor thread pool size.");
    return result;
  }

  result =
      config_provider_->Get(std::string(kIoAsyncExecutorQueueSize),
                            cpio_options_config_.io_async_executor_queue_size);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read io async executor queue size.");
    return result;
  }

  result = config_provider_->Get(
      std::string(kIoAsyncExecutorThreadsCount),
      cpio_options_config_.io_async_executor_thread_pool_size);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read io async executor thread pool size.");
    return result;
  }

  async_executor_ = std::make_shared<AsyncExecutor>(
      cpio_options_config_.async_executor_thread_pool_size,
      cpio_options_config_.async_executor_queue_size);
  io_async_executor_ = std::make_shared<AsyncExecutor>(
      cpio_options_config_.io_async_executor_thread_pool_size,
      cpio_options_config_.io_async_executor_queue_size);

  std::list<std::string> enabled_log_levels;
  std::unordered_set<LogLevel> log_levels;
  if (config_provider_->Get(kEnabledLogLevels, enabled_log_levels)
          .Successful()) {
    for (const auto& enabled_log_level : enabled_log_levels) {
      log_levels.emplace(
          google::scp::core::logger::FromString(enabled_log_level));
    }
  }

  ASSIGN_OR_RETURN(cpio_options_,
                   InitCpio(async_executor_, io_async_executor_, log_levels));

  RETURN_IF_FAILURE(InitService(*async_executor_, kAsyncExecutorServiceName));
  RETURN_IF_FAILURE(
      InitService(*io_async_executor_, kIoAsyncExecutorServiceName));
  RETURN_IF_FAILURE(RunService(*async_executor_, kAsyncExecutorServiceName));
  RETURN_IF_FAILURE(
      RunService(*io_async_executor_, kIoAsyncExecutorServiceName));

  return SuccessExecutionResult();
}

ExecutionResult LookupServer::LoadTeeConfigs() noexcept {
  tee_options_config_.cluster_group_id = std::make_shared<std::string>();
  ExecutionResult result = config_provider_->Get(
      std::string(kClusterGroupId), *tee_options_config_.cluster_group_id);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read the cluster group ID from TEE metadata.");
    return result;
  }

  tee_options_config_.cluster_id = std::make_shared<std::string>();
  result = config_provider_->Get(std::string(kClusterId),
                                 *tee_options_config_.cluster_id);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read the cluster ID from TEE metadata.");
    return result;
  }

  tee_options_config_.environment_name = std::make_shared<std::string>();
  result = config_provider_->Get(std::string(kEnvironment),
                                 *tee_options_config_.environment_name);
  if (!result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, result,
                 "Failed to read the environment name from TEE metadata.");
    return result;
  }
  return SuccessExecutionResult();
}

ExecutionResult LookupServer::LoadParameters() noexcept {
  if (!parameter_client_
           ->GetSizeT(kTotalHttp2ServerThreadsCount,
                      parameters_.http2server_thread_pool_size)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kTotalHttp2ServerThreadsCount));

    ExecutionResult result =
        config_provider_->Get(std::string(kTotalHttp2ServerThreadsCount),
                              parameters_.http2server_thread_pool_size);

    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read http2 server thread pool size.");
      return result;
    }
  }

  parameters_.host_address = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kLookupServiceHostAddress, *parameters_.host_address)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kLookupServiceHostAddress));

    ExecutionResult result = config_provider_->Get(
        std::string(kLookupServiceHostAddress), *parameters_.host_address);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read host address.");
      return result;
    }
  }

  parameters_.host_port = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kLookupServiceHostPort, *parameters_.host_port)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kLookupServiceHostPort));
    ExecutionResult result = config_provider_->Get(
        std::string(kLookupServiceHostPort), *parameters_.host_port);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read host port.");
      return result;
    }
  }

  parameters_.health_service_port = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kHealthServicePort, *parameters_.health_service_port)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kLookupServiceHostPort));
    ExecutionResult result = config_provider_->Get(
        std::string(kHealthServicePort), *parameters_.health_service_port);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read health service port.");
      return result;
    }
  }

  parameters_.orchestrator_host_address = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kOrchestratorHostAddress,
                       *parameters_.orchestrator_host_address)
           .Successful()) {
    ExecutionResult result =
        config_provider_->Get(std::string(kOrchestratorHostAddress),
                              *parameters_.orchestrator_host_address);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read orchestrator host address.");
      return result;
    }
  }

  parameters_.kms_resource_name = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kKmsResourceName, *parameters_.kms_resource_name)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kKmsResourceName));
    ExecutionResult result = config_provider_->Get(
        std::string(kKmsResourceName), *parameters_.kms_resource_name);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the kms resource name.");
      return result;
    }
  }

  parameters_.kms_region = std::make_shared<std::string>();
  if (!parameter_client_->GetString(kKmsRegion, *parameters_.kms_region)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kKmsRegion));
    ExecutionResult result =
        config_provider_->Get(std::string(kKmsRegion), *parameters_.kms_region);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the kms region.");
      return result;
    }
  }

  parameters_.kms_wip_provider = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kKmsWipProvider, *parameters_.kms_wip_provider)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kKmsWipProvider));
    ExecutionResult result = config_provider_->Get(
        std::string(kKmsWipProvider), *parameters_.kms_wip_provider);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the kms wip provider.");
      return result;
    }
  }

  std::list<std::string> kms_default_signatures;
  if (!parameter_client_
           ->GetStringList(kKmsDefaultSignatures, kms_default_signatures)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kKmsDefaultSignatures));
    ExecutionResult result = config_provider_->Get(
        std::string(kKmsDefaultSignatures), kms_default_signatures);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the kms default signatures.");
      return result;
    }
  }
  parameters_.kms_default_signatures =
      std::make_shared<std::vector<std::string>>();
  for (const auto& signature : kms_default_signatures) {
    if (!signature.empty()) {
      parameters_.kms_default_signatures->push_back(signature);
    }
  }

  parameters_.aws_kms_default_audience = std::make_shared<std::string>();
  if (!parameter_client_
           ->GetString(kAwsKmsDefaulAudience,
                       *parameters_.aws_kms_default_audience)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kAwsKmsDefaulAudience));
    ExecutionResult result =
        config_provider_->Get(std::string(kAwsKmsDefaulAudience),
                              *parameters_.aws_kms_default_audience);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the AWS default KMS audience.");
      return result;
    }
  }

  parameters_.jwt_audience = std::make_shared<std::string>();
  if (!parameter_client_->GetString(kJwtAudience, *parameters_.jwt_audience)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kJwtAudience));
    ExecutionResult result = config_provider_->Get(std::string(kJwtAudience),
                                                   *parameters_.jwt_audience);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the JWT audience.");
      return result;
    }
  }

  std::list<std::string> jwt_allowed_emails;
  if (!parameter_client_->GetStringList(kJwtAllowedEmails, jwt_allowed_emails)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kJwtAllowedEmails));
    ExecutionResult result = config_provider_->Get(
        std::string(kJwtAllowedEmails), jwt_allowed_emails);
    if (!result.Successful()) {
      SCP_CRITICAL(
          kComponentName, kZeroUuid, result,
          "Failed to read the JWT emails. Trying to fetch JWT subjects.");
      return result;
    }
  }
  parameters_.jwt_allowed_emails = std::make_shared<std::vector<std::string>>();
  for (const auto& email : jwt_allowed_emails) {
    if (!email.empty()) {
      parameters_.jwt_allowed_emails->push_back(email);
    }
  }

  std::list<std::string> jwt_allowed_subjects;
  if (!parameter_client_
           ->GetStringList(kJwtAllowedSubjects, jwt_allowed_subjects)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kJwtAllowedSubjects));
    ExecutionResult result = config_provider_->Get(
        std::string(kJwtAllowedSubjects), jwt_allowed_subjects);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the JWT subjects and JWT Emails.");
      return result;
    }
  }
  parameters_.jwt_allowed_subjects =
      std::make_shared<std::vector<std::string>>();
  for (const auto& subject : jwt_allowed_subjects) {
    if (!subject.empty()) {
      parameters_.jwt_allowed_subjects->push_back(subject);
    }
  }

  if (!parameter_client_
           ->GetSizeT(kDataRefreshIntervalMins,
                      parameters_.data_refresh_interval_mins_)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kDataRefreshIntervalMins));
    ExecutionResult result =
        config_provider_->Get(std::string(kDataRefreshIntervalMins),
                              parameters_.data_refresh_interval_mins_);
    if (!result.Successful()) {
      SCP_CRITICAL(kComponentName, kZeroUuid, result,
                   "Failed to read the match data refresh interval.");
      return result;
    }
  }

  uint64_t storage_hash_bucket_count;
  if (!parameter_client_
           ->GetSizeT(kStorageHashBucketCount, storage_hash_bucket_count)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kStorageHashBucketCount));
    ExecutionResult result = config_provider_->Get(
        std::string(kStorageHashBucketCount), storage_hash_bucket_count);
    if (!result.Successful()) {
      SCP_WARNING(kComponentName, kZeroUuid,
                  absl::StrFormat("No storage hash bucket count config value "
                                  "given, using default of %d.",
                                  parameters_.storage_hash_bucket_count_));
    } else {
      parameters_.storage_hash_bucket_count_ = storage_hash_bucket_count;
    }
  } else {
    parameters_.storage_hash_bucket_count_ = storage_hash_bucket_count;
  }

  uint64_t max_concurrent_streamed_file_reads;
  if (!parameter_client_
           ->GetSizeT(kMaxConcurrentStreamedFileReads,
                      max_concurrent_streamed_file_reads)
           .Successful()) {
    SCP_WARNING(kComponentName, kZeroUuid,
                absl::StrFormat(
                    "Failed to fetch %s from parameter client. Trying env vars",
                    kMaxConcurrentStreamedFileReads));
    ExecutionResult result =
        config_provider_->Get(std::string(kMaxConcurrentStreamedFileReads),
                              max_concurrent_streamed_file_reads);
    if (!result.Successful()) {
      SCP_WARNING(
          kComponentName, kZeroUuid,
          absl::StrFormat("Failed to read the maximum number of concurrent "
                          "streamed file reads, using default of %d.",
                          parameters_.max_concurrent_streamed_file_reads));
    } else {
      parameters_.max_concurrent_streamed_file_reads =
          max_concurrent_streamed_file_reads;
    }
  } else {
    parameters_.max_concurrent_streamed_file_reads =
        max_concurrent_streamed_file_reads;
  }

  parameters_.http2_server_private_key_file_path =
      std::make_shared<std::string>("");
  parameters_.http2_server_certificate_file_path =
      std::make_shared<std::string>("");

  // If the "use tls" key exists, then the path to the private key and
  // certificate must be valid, non-empty strings. Otherwise, we just assume
  // the default of false for "use tls"
  if ((parameter_client_
           ->GetBool(kHttp2ServerUseTls, parameters_.http2_server_use_tls)
           .Successful() ||
       config_provider_
           ->Get(kHttp2ServerUseTls, parameters_.http2_server_use_tls)
           .Successful()) &&
      parameters_.http2_server_use_tls) {
    if ((!parameter_client_
              ->GetString(kHttp2ServerPrivateKeyFilePath,
                          *parameters_.http2_server_private_key_file_path)
              .Successful() &&
         !config_provider_
              ->Get(kHttp2ServerPrivateKeyFilePath,
                    *parameters_.http2_server_private_key_file_path)
              .Successful()) ||
        parameters_.http2_server_private_key_file_path->empty()) {
      return FailureExecutionResult(INVALID_HTTP2_SERVER_PRIVATE_KEY_FILE_PATH);
    }

    if ((!parameter_client_
              ->GetString(kHttp2ServerCertificateFilePath,
                          *parameters_.http2_server_certificate_file_path)
              .Successful() &&
         !config_provider_
              ->Get(kHttp2ServerCertificateFilePath,
                    *parameters_.http2_server_certificate_file_path)
              .Successful()) ||
        parameters_.http2_server_certificate_file_path->empty()) {
      return FailureExecutionResult(INVALID_HTTP2_SERVER_CERT_FILE_PATH);
    }
  }
  return SuccessExecutionResult();
}

ExecutionResult LookupServer::CreateComponents() noexcept {
  auto aws_cloud_initializer = CloudInitializerFactory().CreateAwsInitializer();
  aws_cloud_initializer->InitCloud();

  std::unique_ptr<CloudPlatformDependencyFactoryInterface>
      platform_dependency_factory =
          GetPlatformDependencyFactoryForCurrentEnv(config_provider_);

  // The factory should be initialized before other components are
  // constructed.
  ExecutionResult factory_result = platform_dependency_factory->Init();
  if (!factory_result.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, factory_result,
                 "Failed to initialize the platform dependency factory.");
    return factory_result;
  }
  RETURN_IF_FAILURE(CreateAndInitCpioOptions());
  RETURN_IF_FAILURE(LoadTeeConfigs());

  std::shared_ptr<CpioParameterClientInterface> cpio_parameter_client =
      ParameterClientFactory::Create(ParameterClientOptions());
  parameter_client_ = std::make_shared<CfmParameterClient>(
      cpio_parameter_client, *tee_options_config_.environment_name);

  RETURN_IF_FAILURE(InitService(*parameter_client_, kParameterClientName));
  RETURN_IF_FAILURE(RunService(*parameter_client_, kParameterClientName));
  RETURN_IF_FAILURE(LoadParameters());

  http1_client_ =
      std::make_shared<Http1CurlClient>(async_executor_, io_async_executor_);
  http2_client_ = std::make_shared<HttpClient>(async_executor_);

  std::shared_ptr<CpioMetricClientInterface> cpio_metric_client =
      MetricClientFactory::Create(MetricClientOptions());
  absl::flat_hash_map<std::string, std::string> metric_client_base_labels;
  metric_client_base_labels[kMetricClientClusterIdLabelKey] =
      *tee_options_config_.cluster_id;
  metric_client_base_labels[kMetricClientClusterGroupIdLabelKey] =
      *tee_options_config_.cluster_group_id;
  metric_client_ = std::make_shared<CfmMetricClient>(
      cpio_metric_client, kMetricClientNamespace, metric_client_base_labels);

  ExecutionResultOr<std::unique_ptr<JwtValidator>> jwt_validator_or =
      JwtValidator::Create(kJwtIssuer, *parameters_.jwt_audience,
                           *parameters_.jwt_allowed_emails,
                           *parameters_.jwt_allowed_subjects);
  if (!jwt_validator_or.Successful()) {
    SCP_CRITICAL(kComponentName, kZeroUuid, factory_result,
                 "Failed to initialize the JWT validator.");
    return jwt_validator_or.result();
  }
  jwt_validator_ = std::move(*jwt_validator_or);

  authorization_proxy_ =
      platform_dependency_factory->ConstructAuthorizationProxyClient(
          async_executor_, http2_client_, jwt_validator_);
  pass_thru_authorization_proxy_ =
      std::make_shared<PassThruAuthorizationProxy>();

  BlobStorageClientOptions blob_storage_client_options;
  blob_storage_client_options.transfer_stall_timeout =
      kBlobStorageClientTransferStallTimeout;
  blob_storage_client_options.retry_limit = kBlobStorageClientRetryLimit;
  blob_storage_client_ =
      BlobStorageClientFactory::Create(blob_storage_client_options);

  gcp_kms_client_ = std::make_shared<KmsClient>();
  gcp_cached_kms_client_ =
      std::make_shared<CachedKmsClient>(async_executor_, gcp_kms_client_);

  AwsKmsClientOptions options;
  // TODO(b/398112417): Remove when no longer necessary
  options.region = kAwsDefaultRegion;
  aws_kms_client_ =
      std::make_shared<KmsClient>(AwsKmsClientFactory::Create(options));
  aws_cached_kms_client_ =
      std::make_shared<CachedKmsClient>(async_executor_, aws_kms_client_);

  PrivateKeyClientOptions private_key_client_options;
  private_key_client_options.enable_gcp_kms_client_retries = true;
  private_key_client_ =
      PrivateKeyClientFactory::Create(private_key_client_options);
  coordinator_client_ =
      std::make_shared<CoordinatorClient>(private_key_client_);
  cached_coordinator_client_ = std::make_shared<CachedCoordinatorClient>(
      async_executor_, coordinator_client_);

  aead_crypto_client_ = std::make_shared<AeadCryptoClient>(
      aws_cached_kms_client_, gcp_cached_kms_client_,
      *parameters_.kms_default_signatures,
      *parameters_.aws_kms_default_audience);
  hpke_crypto_client_ = HpkeCryptoClient::Create(cached_coordinator_client_);

  orchestrator_client_ = std::make_shared<OrchestratorClient>(
      http1_client_, http2_client_, *parameters_.orchestrator_host_address);

  data_provider_ =
      std::make_shared<BlobStorageDataProvider>(blob_storage_client_);
  streamed_match_data_provider_ =
      std::make_shared<BlobStorageMatchDataProvider>(
          blob_storage_client_, data_provider_,
          parameters_.max_concurrent_streamed_file_reads);
  if (parameters_.storage_hash_bucket_count_ == 0) {
    match_data_storage_ = std::make_shared<InMemoryMatchDataStorage>();
  } else {
    match_data_storage_ = std::make_shared<InMemoryMatchDataStorage>(
        parameters_.storage_hash_bucket_count_);
  }
  match_data_loader_ = std::make_shared<MatchDataLoader>(
      data_provider_, streamed_match_data_provider_, match_data_storage_,
      metric_client_, orchestrator_client_, aead_crypto_client_,
      *tee_options_config_.cluster_group_id, *tee_options_config_.cluster_id,
      *parameters_.kms_resource_name, *parameters_.kms_region,
      *parameters_.kms_wip_provider, parameters_.data_refresh_interval_mins_);

  metric_instance_factory_ = std::make_shared<MetricInstanceFactory>(
      async_executor_.get(), cpio_metric_client.get());

  http_server_ = std::make_shared<Http2Server>(
      *parameters_.host_address, *parameters_.host_port,
      parameters_.http2server_thread_pool_size, async_executor_,
      authorization_proxy_, metric_instance_factory_, config_provider_,
      Http2ServerOptions(
          parameters_.http2_server_use_tls,
          parameters_.http2_server_private_key_file_path,
          parameters_.http2_server_certificate_file_path,
          RetryStrategyOptions(
              RetryStrategyType::Exponential, kHttpServerRetryStrategyDelayInMs,
              google::scp::core::kDefaultRetryStrategyMaxRetries),
          kHttp2ServerMetricNamespace, kHttp2ServerLookupServiceMetricName));

  health_http_server_ = std::make_shared<Http2Server>(
      *parameters_.host_address, *parameters_.health_service_port, 1,
      async_executor_, pass_thru_authorization_proxy_, metric_instance_factory_,
      config_provider_,
      Http2ServerOptions(
          parameters_.http2_server_use_tls,
          parameters_.http2_server_private_key_file_path,
          parameters_.http2_server_certificate_file_path,
          RetryStrategyOptions(
              RetryStrategyType::Exponential, kHttpServerRetryStrategyDelayInMs,
              google::scp::core::kDefaultRetryStrategyMaxRetries),
          kHttp2ServerMetricNamespace, kHttp2ServerHealthServiceMetricName));

  absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
      status_providers;
  status_providers[kMatchDataStorageServiceName] = match_data_storage_;

  lookup_service_ = std::make_shared<LookupService>(
      match_data_storage_, http_server_, aead_crypto_client_,
      hpke_crypto_client_, metric_client_, metric_instance_factory_,
      status_providers);
  health_service_ =
      std::make_shared<HealthService>(health_http_server_, status_providers);

  return SuccessExecutionResult();
}

ExecutionResult LookupServer::Init() noexcept {
  RETURN_IF_FAILURE(CreateComponents());

  SCP_INFO(kComponentName, kZeroUuid, "Initializing Lookup Server...");

  RETURN_IF_FAILURE(InitService(*http1_client_, kHttp1ClientServiceName));
  RETURN_IF_FAILURE(InitService(*http2_client_, kHttp2ClientServiceName));
  RETURN_IF_FAILURE(InitService(*metric_client_, kMetricClientName));
  RETURN_IF_FAILURE(
      InitService(*authorization_proxy_, kAuthorizationProxyServiceName));
  RETURN_IF_FAILURE(InitService(*pass_thru_authorization_proxy_,
                                kPassThruAuthorizationProxyServiceName));
  RETURN_IF_FAILURE(
      InitService(*blob_storage_client_, kBlobStorageClientServiceName));
  RETURN_IF_FAILURE(InitService(*gcp_kms_client_, kGcpKmsClientName));
  RETURN_IF_FAILURE(
      InitService(*gcp_cached_kms_client_, kGcpCachedKmsClientName));
  RETURN_IF_FAILURE(InitService(*aws_kms_client_, kAwsKmsClientName));
  RETURN_IF_FAILURE(
      InitService(*aws_cached_kms_client_, kAwsCachedKmsClientName));
  RETURN_IF_FAILURE(InitService(*private_key_client_, kPrivateKeyClientName));
  RETURN_IF_FAILURE(InitService(*coordinator_client_, kCoordinatorClientName));
  RETURN_IF_FAILURE(
      InitService(*cached_coordinator_client_, kCachedCoordinatorClientName));
  RETURN_IF_FAILURE(InitService(*aead_crypto_client_, kAeadCryptoClientName));
  RETURN_IF_FAILURE(InitService(*hpke_crypto_client_, kHpkeCryptoClientName));
  RETURN_IF_FAILURE(
      InitService(*orchestrator_client_, kOrchestratorClientName));
  RETURN_IF_FAILURE(InitService(*data_provider_, kDataProviderServiceName));
  RETURN_IF_FAILURE(InitService(*streamed_match_data_provider_,
                                kStreamedMatchDataProviderServiceName));
  RETURN_IF_FAILURE(
      InitService(*match_data_storage_, kMatchDataStorageServiceName));
  RETURN_IF_FAILURE(
      InitService(*match_data_loader_, kMatchDataLoaderServiceName));
  RETURN_IF_FAILURE(InitService(*http_server_, kHttpServerServiceName));
  RETURN_IF_FAILURE(InitService(*health_http_server_, kHealthHttpServerName));
  RETURN_IF_FAILURE(InitService(*lookup_service_, kLookupServiceName));
  RETURN_IF_FAILURE(InitService(*health_service_, kHealthServiceName));

  SCP_INFO(kComponentName, kZeroUuid, "Lookup Server initialized.");
  return SuccessExecutionResult();
}

ExecutionResult LookupServer::Run() noexcept {
  if (is_running_) {
    return FailureExecutionResult(LOOKUP_SERVER_ALREADY_RUNNING);
  }

  SCP_INFO(kComponentName, kZeroUuid, "Running Lookup Server components...");

  RETURN_IF_FAILURE(RunService(*http1_client_, kHttp1ClientServiceName));
  RETURN_IF_FAILURE(RunService(*http2_client_, kHttp2ClientServiceName));
  RETURN_IF_FAILURE(RunService(*metric_client_, kMetricClientName));
  RETURN_IF_FAILURE(
      RunService(*authorization_proxy_, kAuthorizationProxyServiceName));
  RETURN_IF_FAILURE(RunService(*pass_thru_authorization_proxy_,
                               kPassThruAuthorizationProxyServiceName));
  RETURN_IF_FAILURE(
      RunService(*blob_storage_client_, kBlobStorageClientServiceName));
  RETURN_IF_FAILURE(RunService(*gcp_kms_client_, kGcpKmsClientName));
  RETURN_IF_FAILURE(
      RunService(*gcp_cached_kms_client_, kGcpCachedKmsClientName));
  RETURN_IF_FAILURE(RunService(*aws_kms_client_, kAwsKmsClientName));
  RETURN_IF_FAILURE(
      RunService(*aws_cached_kms_client_, kAwsCachedKmsClientName));
  RETURN_IF_FAILURE(RunService(*private_key_client_, kPrivateKeyClientName));
  RETURN_IF_FAILURE(RunService(*coordinator_client_, kCoordinatorClientName));
  RETURN_IF_FAILURE(
      RunService(*cached_coordinator_client_, kCachedCoordinatorClientName));
  RETURN_IF_FAILURE(RunService(*aead_crypto_client_, kAeadCryptoClientName));
  RETURN_IF_FAILURE(RunService(*hpke_crypto_client_, kHpkeCryptoClientName));
  RETURN_IF_FAILURE(RunService(*orchestrator_client_, kOrchestratorClientName));
  RETURN_IF_FAILURE(RunService(*data_provider_, kDataProviderServiceName));
  RETURN_IF_FAILURE(RunService(*streamed_match_data_provider_,
                               kStreamedMatchDataProviderServiceName));
  RETURN_IF_FAILURE(
      RunService(*match_data_storage_, kMatchDataStorageServiceName));
  RETURN_IF_FAILURE(
      RunService(*match_data_loader_, kMatchDataLoaderServiceName));
  RETURN_IF_FAILURE(RunService(*http_server_, kHttpServerServiceName));
  RETURN_IF_FAILURE(RunService(*health_http_server_, kHealthHttpServerName));
  RETURN_IF_FAILURE(RunService(*lookup_service_, kLookupServiceName));
  RETURN_IF_FAILURE(RunService(*health_service_, kHealthServiceName));

  is_running_ = true;
  SCP_INFO(kComponentName, kZeroUuid, "Lookup Server running.");
  return SuccessExecutionResult();
}

ExecutionResult LookupServer::Stop() noexcept {
  if (!is_running_) {
    return FailureExecutionResult(LOOKUP_SERVER_NOT_RUNNING);
  }

  SCP_INFO(kComponentName, kZeroUuid, "Stopping Lookup Server components...");

  RETURN_IF_FAILURE(StopService(*health_service_, kHealthServiceName));
  RETURN_IF_FAILURE(StopService(*lookup_service_, kLookupServiceName));
  RETURN_IF_FAILURE(StopService(*health_http_server_, kHealthHttpServerName));
  RETURN_IF_FAILURE(StopService(*http_server_, kHttpServerServiceName));
  RETURN_IF_FAILURE(
      StopService(*match_data_loader_, kMatchDataLoaderServiceName));
  RETURN_IF_FAILURE(
      StopService(*match_data_storage_, kMatchDataStorageServiceName));
  RETURN_IF_FAILURE(StopService(*streamed_match_data_provider_,
                                kStreamedMatchDataProviderServiceName));
  RETURN_IF_FAILURE(StopService(*data_provider_, kDataProviderServiceName));
  RETURN_IF_FAILURE(
      StopService(*orchestrator_client_, kOrchestratorClientName));
  RETURN_IF_FAILURE(StopService(*hpke_crypto_client_, kHpkeCryptoClientName));
  RETURN_IF_FAILURE(StopService(*aead_crypto_client_, kAeadCryptoClientName));
  RETURN_IF_FAILURE(
      StopService(*cached_coordinator_client_, kCachedCoordinatorClientName));
  RETURN_IF_FAILURE(StopService(*coordinator_client_, kCoordinatorClientName));
  RETURN_IF_FAILURE(StopService(*private_key_client_, kPrivateKeyClientName));
  RETURN_IF_FAILURE(
      StopService(*aws_cached_kms_client_, kGcpCachedKmsClientName));
  RETURN_IF_FAILURE(StopService(*aws_kms_client_, kAwsKmsClientName));
  RETURN_IF_FAILURE(
      StopService(*gcp_cached_kms_client_, kAwsCachedKmsClientName));
  RETURN_IF_FAILURE(StopService(*gcp_kms_client_, kAwsKmsClientName));
  RETURN_IF_FAILURE(
      StopService(*blob_storage_client_, kBlobStorageClientServiceName));
  RETURN_IF_FAILURE(StopService(*pass_thru_authorization_proxy_,
                                kPassThruAuthorizationProxyServiceName));
  RETURN_IF_FAILURE(
      StopService(*authorization_proxy_, kAuthorizationProxyServiceName));
  RETURN_IF_FAILURE(StopService(*metric_client_, kMetricClientName));
  RETURN_IF_FAILURE(StopService(*http2_client_, kHttp2ClientServiceName));
  RETURN_IF_FAILURE(StopService(*http1_client_, kHttp1ClientServiceName));
  RETURN_IF_FAILURE(StopService(*parameter_client_, kParameterClientName));
  RETURN_IF_FAILURE(
      StopService(*io_async_executor_, kIoAsyncExecutorServiceName));
  RETURN_IF_FAILURE(StopService(*async_executor_, kAsyncExecutorServiceName));

  is_running_ = false;
  SCP_INFO(kComponentName, kZeroUuid, "Lookup Server stopped.");
  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
