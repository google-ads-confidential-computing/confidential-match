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

#ifndef CC_LOOKUP_SERVER_SERVER_SRC_LOOKUP_SERVER_H_
#define CC_LOOKUP_SERVER_SERVER_SRC_LOOKUP_SERVER_H_

#include <memory>
#include <string>
#include <vector>

#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/authorization_proxy_interface.h"
#include "cc/core/interface/blob_storage_provider_interface.h"
#include "cc/core/interface/config_provider_interface.h"
#include "cc/core/interface/http_client_interface.h"
#include "cc/core/interface/http_server_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"
#include "cc/public/cpio/interface/private_key_client/private_key_client_interface.h"
#include "cc/public/cpio/utils/metric_instance/interface/metric_instance_factory_interface.h"

#include "cc/lookup_server/health_service/src/health_service.h"
#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/data_provider_interface.h"
#include "cc/lookup_server/interface/jwt_validator_interface.h"
#include "cc/lookup_server/interface/kms_client_interface.h"
#include "cc/lookup_server/interface/match_data_loader_interface.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/lookup_server/interface/orchestrator_client_interface.h"
#include "cc/lookup_server/interface/parameter_client_interface.h"
#include "cc/lookup_server/interface/streamed_match_data_provider_interface.h"
#include "cc/lookup_server/service/src/lookup_service.h"

namespace google::confidential_match::lookup_server {

// Stores configuration values to init CPIO.
struct CpioOptionsConfig {
  size_t async_executor_queue_size = 10000;
  size_t async_executor_thread_pool_size = 8;  // Should be same as no. of cores
  size_t io_async_executor_queue_size = 10000;
  size_t io_async_executor_thread_pool_size = 256;
};

// Stores values for variables passed in from tee metadata
struct TeeOptionsConfig {
  // IDs for where this Lookup Server instance is running
  std::shared_ptr<std::string> cluster_group_id;
  std::shared_ptr<std::string> cluster_id;

  std::shared_ptr<std::string> environment_name;
};

// Stores parameters for Lookup Server
struct LookupServerParameters {
  size_t http2server_thread_pool_size = 256;
  // The addresses and ports that Lookup Server runs as
  std::shared_ptr<std::string> host_address;
  std::shared_ptr<std::string> host_port;
  std::shared_ptr<std::string> health_service_port;

  // The host address to use for the Orchestrator
  std::shared_ptr<std::string> orchestrator_host_address;

  // GCP KMS parameters
  std::shared_ptr<std::string> kms_resource_name;
  std::shared_ptr<std::string> kms_region;
  std::shared_ptr<std::string> kms_wip_provider;

  // AWS KMS parameters
  std::shared_ptr<std::vector<std::string>> kms_default_signatures;
  std::shared_ptr<std::string> aws_kms_default_audience;

  // Parameters used for JWT authentication of incoming requests
  std::shared_ptr<std::string> jwt_audience;
  std::shared_ptr<std::vector<std::string>> jwt_allowed_emails;
  std::shared_ptr<std::vector<std::string>> jwt_allowed_subjects;

  // Configures the amount of time to wait between data refreshes
  uint64_t data_refresh_interval_mins_;
  // Configures the size for internal hash map initialization, or 0 for default.
  uint64_t storage_hash_bucket_count_ = 0;
  // Configures the maximum number of match data files to read concurrently.
  uint64_t max_concurrent_streamed_file_reads = 100;

  // TLS context for HTTP2 Server
  bool http2_server_use_tls = false;
  std::shared_ptr<std::string> http2_server_private_key_file_path;
  std::shared_ptr<std::string> http2_server_certificate_file_path;
};

// The main server responsible for running and managing all Lookup Server
// services.
class LookupServer : public scp::core::ServiceInterface {
 public:
  explicit LookupServer(
      std::shared_ptr<scp::core::ConfigProviderInterface> config_provider)
      : config_provider_(config_provider) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

 protected:
  virtual scp::core::ExecutionResult CreateAndInitCpioOptions() noexcept;
  virtual scp::core::ExecutionResult LoadTeeConfigs() noexcept;
  virtual scp::core::ExecutionResult LoadParameters() noexcept;
  virtual scp::core::ExecutionResult CreateComponents() noexcept;

  std::shared_ptr<scp::core::ConfigProviderInterface> config_provider_;
  std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor_;
  std::shared_ptr<scp::core::AsyncExecutorInterface> io_async_executor_;
  std::shared_ptr<scp::cpio::CpioOptions> cpio_options_;
  std::shared_ptr<ParameterClientInterface> parameter_client_;
  std::shared_ptr<scp::core::HttpClientInterface> http1_client_;
  std::shared_ptr<scp::core::HttpClientInterface> http2_client_;
  std::shared_ptr<MetricClientInterface> metric_client_;
  std::shared_ptr<JwtValidatorInterface> jwt_validator_;
  std::shared_ptr<scp::core::AuthorizationProxyInterface> authorization_proxy_;
  std::shared_ptr<scp::core::AuthorizationProxyInterface>
      pass_thru_authorization_proxy_;
  std::shared_ptr<scp::cpio::BlobStorageClientInterface> blob_storage_client_;
  std::shared_ptr<KmsClientInterface> aws_kms_client_;
  std::shared_ptr<KmsClientInterface> aws_cached_kms_client_;
  std::shared_ptr<KmsClientInterface> gcp_kms_client_;
  std::shared_ptr<KmsClientInterface> gcp_cached_kms_client_;
  std::shared_ptr<scp::cpio::PrivateKeyClientInterface> private_key_client_;
  std::shared_ptr<CoordinatorClientInterface> coordinator_client_;
  std::shared_ptr<CoordinatorClientInterface> cached_coordinator_client_;
  std::shared_ptr<CryptoClientInterface> aead_crypto_client_;
  std::shared_ptr<CryptoClientInterface> hpke_crypto_client_;
  std::shared_ptr<OrchestratorClientInterface> orchestrator_client_;
  std::shared_ptr<DataProviderInterface> data_provider_;
  std::shared_ptr<StreamedMatchDataProviderInterface>
      streamed_match_data_provider_;
  std::shared_ptr<MatchDataStorageInterface> match_data_storage_;
  std::shared_ptr<MatchDataLoaderInterface> match_data_loader_;
  std::shared_ptr<scp::core::HttpServerInterface> http_server_;
  std::shared_ptr<scp::core::HttpServerInterface> health_http_server_;
  std::shared_ptr<scp::cpio::MetricInstanceFactoryInterface>
      metric_instance_factory_;

  std::shared_ptr<LookupService> lookup_service_;
  std::shared_ptr<HealthService> health_service_;
  bool is_running_ = false;
  CpioOptionsConfig cpio_options_config_;
  TeeOptionsConfig tee_options_config_;
  LookupServerParameters parameters_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVER_SRC_LOOKUP_SERVER_H_
