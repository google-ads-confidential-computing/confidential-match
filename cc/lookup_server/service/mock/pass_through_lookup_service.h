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

#ifndef CC_LOOKUP_SERVER_SERVICE_MOCK_PASS_THROUGH_LOOKUP_SERVICE_H_
#define CC_LOOKUP_SERVER_SERVICE_MOCK_PASS_THROUGH_LOOKUP_SERVICE_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "cc/core/interface/http_server_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/utils/metric_instance/src/aggregate_metric.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/lookup_server/interface/status_provider_interface.h"
#include "cc/lookup_server/service/src/lookup_service.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief A pass-through Lookup Service that exposes underlying API methods
 * for testing only.
 */
class PassThroughLookupService : public LookupService {
 public:
  PassThroughLookupService(
      std::shared_ptr<MatchDataStorageInterface> match_data_storage,
      std::shared_ptr<scp::core::HttpServerInterface> http_server,
      std::shared_ptr<CryptoClientInterface> aead_crypto_client,
      std::shared_ptr<CryptoClientInterface> hpke_crypto_client,
      std::shared_ptr<scp::cpio::AggregateMetricInterface>
          request_aggregate_metric,
      std::shared_ptr<scp::cpio::AggregateMetricInterface>
          error_aggregate_metric,
      std::shared_ptr<scp::cpio::AggregateMetricInterface>
          invalid_scheme_aggregate_metric,
      std::shared_ptr<MetricClientInterface> metric_client,
      absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
          service_status_providers)
      : LookupService(match_data_storage, http_server, aead_crypto_client,
                      hpke_crypto_client, metric_client, nullptr,
                      service_status_providers),
        request_aggregate_metric_(request_aggregate_metric),
        error_aggregate_metric_(error_aggregate_metric),
        invalid_scheme_aggregate_metric_(invalid_scheme_aggregate_metric) {}

  scp::core::ExecutionResult BuildAggregateMetrics() noexcept {
    request_count_metrics_ = request_aggregate_metric_;
    request_error_metrics_ = error_aggregate_metric_;
    request_invalid_scheme_metrics_ = invalid_scheme_aggregate_metric_;
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult GetHealthcheck(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept {
    return LookupService::GetHealthcheck(http_context);
  }

  scp::core::ExecutionResult PostLookup(
      scp::core::AsyncContext<scp::core::HttpRequest, scp::core::HttpResponse>&
          http_context) noexcept {
    return LookupService::PostLookup(http_context);
  }

  std::shared_ptr<scp::cpio::AggregateMetricInterface>
      request_aggregate_metric_;
  std::shared_ptr<scp::cpio::AggregateMetricInterface> error_aggregate_metric_;
  std::shared_ptr<scp::cpio::AggregateMetricInterface>
      invalid_scheme_aggregate_metric_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_MOCK_PASS_THROUGH_LOOKUP_SERVICE_H_
