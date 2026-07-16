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

#ifndef CC_MATCH_SERVICE_SERVER_MATCH_SERVER_H_
#define CC_MATCH_SERVICE_SERVER_MATCH_SERVER_H_

#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cc/core/hash/hasher_interface.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/http_client_interface.h"
#include "cc/cpio/client_providers/cloud_initializer/src/aws/aws_initializer.h"
#include "cc/match_service/auth_token_client/auth_token_client_interface.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "cc/match_service/lookup_service_client/lookup_service_client_interface.h"
#include "cc/match_service/lookup_service_shard_client/lookup_service_shard_client_interface.h"
#include "cc/match_service/orchestrator_client/orchestrator_client_interface.h"
#include "cc/match_service/service/match_service.h"
#include "cc/match_service/tasks/match_task_interface.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "cc/public/cpio/interface/private_key_client/private_key_client_interface.h"
#include "cc/public/cpio/utils/configuration_fetcher/interface/configuration_fetcher_interface.h"
#include "cc/public/cpio/utils/dual_writing_metric_client/interface/dual_writing_metric_client_interface.h"
#include "cc/public/cpio/utils/key_fetching/interface/key_fetcher_with_cache_interface.h"
#include "protos/match_service/backend/key_fetcher_options.pb.h"
#include "protos/match_service/backend/private_key_endpoints.pb.h"

namespace google::confidential_match::match_service {

struct ConfigOptions {
  // Metric client config.
  bool enable_metric_client = false;
  std::string metric_namespace;
  std::string otel_collector_address;

  // Async executor config.
  std::size_t cpu_async_executor_queue_size = 0;
  std::size_t cpu_async_executor_threads_count = 0;
  std::size_t io_async_executor_queue_size = 0;
  std::size_t io_async_executor_threads_count = 0;
  std::size_t match_service_async_executor_queue_size = 0;
  std::size_t match_service_async_executor_threads_count = 0;

  // GCP config.
  std::string gcp_project_id;

  // AWS config.
  std::string aws_kms_default_audience;
  std::vector<std::string> aws_kms_default_signatures;

  // gRPC server host config.
  std::string host_address;
  std::string host_port;

  // The host address of the Orchestrator.
  std::string orchestrator_host_address;
  // The JWT audience used for requests to Lookup Service.
  std::string lookup_service_auth_audience;
  // The Lookup Service cluster group ID.
  std::string lookup_service_cluster_group_id;

  // Service config.
  std::vector<std::string> allowed_client_accounts;

  // Coordinator config.
  backend::PrivateKeyEndpoints private_key_endpoints;
  // Key fetcher config.
  backend::KeyFetcherOptions key_fetcher_options;
  // Lifetime of the auth token cache entry in seconds.
  // Note that identity tokens are valid for 1 hour.
  // See
  // https://docs.cloud.google.com/docs/authentication/token-types#identity-tokens
  int auth_token_cache_entry_lifetime_seconds = 3 * 60;
  // Gating flag to disable service account email retrieval.
  bool disable_service_account_email_retrieval = false;
  // Injected service account email.
  std::string service_account_email;
  // A boolean to enable background refresh for CachedAuthTokenClient.
  bool enable_background_auth_token_refresh = false;
};

// The main server responsible for running Match Service.
class MatchServer {
 public:
  explicit MatchServer(
      std::unordered_set<scp::core::LogLevel> enabled_log_levels)
      : enabled_log_levels_(std::move(enabled_log_levels)) {}

  absl::Status Init() noexcept;
  absl::Status Run() noexcept;
  absl::Status Stop() noexcept;

 private:
  // Sets configuration values.
  absl::Status SetConfiguration() noexcept;

  // Creates components used by the Data Producer Server.
  absl::Status CreateComponents() noexcept;

  // Sets configuration values that are fetched using ConfigurationFetcher.
  absl::Status SetConfigFromParameters() noexcept;

  // Runs gRPC server for the Data Producer Service.
  void RunGrpcServer() noexcept;

  // Periodically checks on the status of the instance and logs health
  // related metrics.
  void BackgroundHealthCheckLoop() noexcept;

  // Runs a background health check operation once, checking on the
  // instance status and logging health-related metrics.
  void BackgroundHealthCheck() noexcept;

  // Fetches and logs metrics for the provided async executor.
  //
  // name: the name associated with the async executor
  // stats: the current stats for the async executor
  // previous_stats: the previous stats recorded for the executor
  void LogAsyncExecutorMetrics(
      absl::string_view name, const scp::core::AsyncExecutorStats& stats,
      const scp::core::AsyncExecutorStats& previous_stats) noexcept;

  std::unordered_set<scp::core::LogLevel> enabled_log_levels_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<scp::cpio::ConfigurationFetcherInterface> config_fetcher_;
  std::unique_ptr<scp::cpio::MetricClientInterface> metric_client_;
  std::shared_ptr<scp::core::AsyncExecutorInterface> cpu_async_executor_;
  std::shared_ptr<scp::core::AsyncExecutorInterface> io_async_executor_;
  std::shared_ptr<scp::core::AsyncExecutorInterface>
      match_service_async_executor_;
  // Thread used to run tasks that periodically check on the service health.
  std::unique_ptr<std::thread> health_checking_thread_;
  // The most recent metrics recording from the CPU async executor.
  scp::core::AsyncExecutorStats cpu_async_executor_stats_;
  // The most recent metrics recording from the IO async executor.
  scp::core::AsyncExecutorStats io_async_executor_stats_;
  // The most recent metrics recording from the Match Service async executor.
  scp::core::AsyncExecutorStats match_service_async_executor_stats_;
  // Client used to make HTTP1 requests, eg. to perform authentication.
  std::unique_ptr<scp::core::HttpClientInterface> http1_client_;
  // Client used to make HTTP2 requests, eg. to call the Orchestrator and LS.
  std::unique_ptr<scp::core::HttpClientInterface> http2_client_;
  // Required to initialize and shutdown the AWS SDK
  scp::cpio::client_providers::AwsInitializer aws_cloud_init_;
  // Client used to make requests to GCP KMS
  std::unique_ptr<KmsClientInterface> gcp_kms_client_;
  // Client with caching used to make requests to GCP KMS
  std::unique_ptr<KmsClientInterface> cached_gcp_kms_client_;
  // Client used to make requests to AWS KMS
  std::unique_ptr<KmsClientInterface> aws_kms_client_;
  // Client with caching used to make requests to AWS KMS
  std::unique_ptr<KmsClientInterface> cached_aws_kms_client_;
  // Client used for wrapped key decryption/encryption
  std::unique_ptr<CryptoClientInterface> aead_crypto_client_;
  // Client for retrieving authentication tokens.
  std::shared_ptr<AuthTokenClientInterface> auth_token_client_;
  // Client for retrieving cached authentication tokens.
  std::unique_ptr<AuthTokenClientInterface> cached_auth_token_client_;
  // The client used to call the Orchestrator.
  std::shared_ptr<OrchestratorClientInterface> orchestrator_client_;
  // The client used to call the Orchestrator and caches responses.
  std::unique_ptr<OrchestratorClientInterface> cached_orchestrator_client_;
  // The client used to determine the appropriate Lookup Service shard.
  std::unique_ptr<LookupServiceShardClientInterface>
      lookup_service_shard_client_;
  // The client used to send requests to Lookup Service.
  std::unique_ptr<LookupServiceClientInterface> lookup_service_client_;
  // Used to compute SHA-256 hashes.
  std::unique_ptr<HasherInterface> sha256_hasher_;
  std::unique_ptr<scp::cpio::PrivateKeyClientInterface> private_key_client_;
  std::unique_ptr<scp::cpio::DualWritingMetricClientInterface>
      dual_writing_metric_client_;
  std::unique_ptr<scp::cpio::KeyFetcherWithCacheInterface> key_fetcher_;
  // HybridCryptoClients for each application.
  std::unique_ptr<CryptoClientInterface> hybrid_crypto_client_;
  // The task used for matching hashed requests.
  std::unique_ptr<MatchTaskInterface> hashed_match_task_;
  // The task used for matching kms encrypted requests.
  std::unique_ptr<MatchTaskInterface> kms_encrypted_match_task_;
  // The task used to matching all requests.
  std::unique_ptr<MatchTaskInterface> match_task_;
  // The service used to handle matching requests.
  std::unique_ptr<MatchService> match_service_;

  bool is_running_ = false;
  ConfigOptions config_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_SERVER_MATCH_SERVER_H_
