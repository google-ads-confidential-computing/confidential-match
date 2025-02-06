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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_LOOKUP_SERVICE_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_LOOKUP_SERVICE_H_

#include <memory>
#include <string>
#include <public/cpio/utils/metric_instance/interface/metric_instance_factory_interface.h>

#include "absl/container/flat_hash_map.h"
#include "absl/time/clock.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/config_provider_interface.h"
#include "cc/core/interface/http_server_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "cc/public/cpio/utils/metric_instance/interface/aggregate_metric_interface.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/lookup_server_service_interface.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/lookup_server/interface/status_provider_interface.h"
#include "cc/lookup_server/service/src/coordinator_encrypted_lookup_task.h"
#include "cc/lookup_server/service/src/hashed_lookup_task.h"
#include "cc/lookup_server/service/src/kms_encrypted_lookup_task.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief The main service responsible for handling requests to Lookup Service.
 */
class LookupService : public LookupServerServiceInterface {
 public:
  LookupService(
      std::shared_ptr<MatchDataStorageInterface> match_data_storage,
      std::shared_ptr<scp::core::HttpServerInterface> http_server,
      std::shared_ptr<CryptoClientInterface> aead_crypto_client,
      std::shared_ptr<CryptoClientInterface> hpke_crypto_client,
      std::shared_ptr<lookup_server::MetricClientInterface> metric_client,
      std::shared_ptr<scp::cpio::MetricInstanceFactoryInterface>
          metric_instance_factory,
      absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
          service_status_providers)
      : match_data_storage_(match_data_storage),
        http_server_(http_server),
        aead_crypto_client_(aead_crypto_client),
        hpke_crypto_client_(hpke_crypto_client),
        metric_client_(metric_client),
        metric_instance_factory_(metric_instance_factory),
        service_status_providers_(service_status_providers),
        last_request_time_ms_(0),
        hashed_lookup_task_(match_data_storage_),
        kms_encrypted_lookup_task_(match_data_storage_, aead_crypto_client_),
        coordinator_encrypted_lookup_task_(match_data_storage_,
                                           hpke_crypto_client_) {}

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

 protected:
  /**
   * @brief Request handler for the healthcheck API.
   *
   * @param http_context provides the request information. Will be mutated
   * with the response after handling the request
   * @return an ExecutionResult indicating whether the operation was successful
   */
  scp::core::ExecutionResult GetHealthcheck(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  /**
   * @brief Request handler for the match lookup API.
   *
   * @param http_context provides the request information. Will be mutated
   * with the response after handling the request.
   * @return an ExecutionResult indicating whether the operation was successful
   */
  scp::core::ExecutionResult PostLookup(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept;

  /**
   * @brief Handles a PostLookup request asynchronously, returning whether or
   * not the operation was successful.
   *
   * @param lookup_context the context containing the lookup request
   * @return an ExecutionResult indicating whether the request was handled
   * successfully
   */
  void PostLookupHandler(scp::core::AsyncContext<proto_api::LookupRequest,
                                                 proto_api::LookupResponse>
                             context) noexcept;

  /**
   * Handles the response after performing the lookup request.
   *
   * @param lookup_context the context containing the result of the lookup
   * request
   * @param http_context the HTTP context to write the response to
   * @param request_start_time the time at which the request first started
   */
  void OnPostLookupHandlerCallback(
      scp::core::AsyncContext<proto_api::LookupRequest,
                              proto_api::LookupResponse>& lookup_context,
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>
          http_context,
      absl::Time request_start_time) noexcept;

  /// Builds Aggregate Metrics to use to record custom metrics
  virtual scp::core::ExecutionResult BuildAggregateMetrics() noexcept;

  // The AggregateMetric instance for lookup request counts.
  std::shared_ptr<scp::cpio::AggregateMetricInterface> request_count_metrics_;

  // The AggregateMetric instance for lookup request latency.
  std::shared_ptr<scp::cpio::AggregateMetricInterface> request_error_metrics_;

  // The AggregateMetric instance for lookup requests with invalid sharding
  // schemes.
  std::shared_ptr<scp::cpio::AggregateMetricInterface>
      request_invalid_scheme_metrics_;

 private:
  /** @brief Initializes components used to record request metrics. */
  scp::core::ExecutionResult InitMetrics() noexcept;

  /** @brief Runs components used to record request metrics. */
  scp::core::ExecutionResult RunMetrics() noexcept;

  /** @brief Stops components used to record request metrics. */
  scp::core::ExecutionResult StopMetrics() noexcept;

  ///  Uses MetricClient to put metrics to cloud
  scp::core::ExecutionResult PutLatencyMetrics(
      absl::Time request_start_time) noexcept;

  // A storage service containing match data.
  std::shared_ptr<MatchDataStorageInterface> match_data_storage_;
  // An instance of the HTTP server.
  std::shared_ptr<scp::core::HttpServerInterface> http_server_;
  // An instance of the AEAD crypto client.
  std::shared_ptr<CryptoClientInterface> aead_crypto_client_;
  // An instance of the hybrid public-key encryption crypto client.
  std::shared_ptr<CryptoClientInterface> hpke_crypto_client_;
  // An instance of the CFM metric client.
  std::shared_ptr<lookup_server::MetricClientInterface> metric_client_;
  // An instance of MetricInstanceFactory.
  std::shared_ptr<scp::cpio::MetricInstanceFactoryInterface>
      metric_instance_factory_;
  // Mapping of service_name -> service_status_provider, which provides the
  // current status of a service.
  absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
      service_status_providers_;
  // The timestamp for when latency metrics were last recorded to the cloud.
  std::atomic<uint64_t> last_request_time_ms_;
  // Request handler for hashed lookup requests.
  HashedLookupTask hashed_lookup_task_;
  // Request handler for requests encrypted with a KMS-wrapped key.
  KmsEncryptedLookupTask kms_encrypted_lookup_task_;
  // Request handler for coordinator-encrypted lookup requests.
  CoordinatorEncryptedLookupTask coordinator_encrypted_lookup_task_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_LOOKUP_SERVICE_H_
