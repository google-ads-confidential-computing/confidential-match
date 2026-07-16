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

#include "cc/match_service/service/match_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/functional/bind_front.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "cc/core/async/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/errors.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/converters/match_request_converters.h"
#include "cc/match_service/converters/match_response_converters.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/metrics/metrics_util.h"
#include "cc/public/core/interface/execution_result.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/security/alts_util.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::cmrt::sdk::metric_service::v1::Metric;
using ::google::cmrt::sdk::metric_service::v1::MetricType;
using ::google::cmrt::sdk::metric_service::v1::MetricUnit;
using ::google::confidential_match::match_service::api::v1::Application_Name;
using ::google::confidential_match::match_service::api::v1::DataRecord;
using ::google::confidential_match::match_service::api::v1::EncryptionKey;
using ::google::confidential_match::match_service::api::v1::MatchKey;
using ::google::confidential_match::match_service::api::v1::MatchKeyFormat_Name;
using ::google::confidential_match::match_service::api::v1::MatchRequest;
using ::google::confidential_match::match_service::api::v1::MatchResponse;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::AsyncPriority;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::cpio::MetricClientInterface;
using ::grpc::CallbackServerContext;
using ::grpc::ServerUnaryReactor;

// Generates a unique request identifier.
std::string GenerateRequestId(absl::BitGen& bitgen) {
  // Use a random 32-bit hex-encoded identifier (8 characters long)
  uint32_t random_val = absl::Uniform<uint32_t>(bitgen);
  return absl::StrCat(absl::Hex(random_val, absl::kZeroPad8));
}

const EncryptionKey* ResolveEncryptionKeyForMatchKey(
    const MatchRequest& request, const DataRecord& record,
    const MatchKey& key) {
  if (key.has_encryption_key()) return &key.encryption_key();
  if (record.has_encryption_key()) return &record.encryption_key();
  if (request.has_encryption_key()) return &request.encryption_key();
  return nullptr;
}

absl::string_view GetEncryptionKeyType(
    const EncryptionKey* key,
    absl::string_view not_encrypted = metrics::kNotEncrypted) {
  if (key == nullptr) {
    return not_encrypted;
  } else if (key->has_wrapped_key()) {
    return metrics::kWrappedKeyType;
  } else {
    return metrics::kCoordinatorKeyType;
  }
}

absl::string_view GetEncryptionKeyType(const MatchRequest& request) {
  return GetEncryptionKeyType(
      request.has_encryption_key() ? &request.encryption_key() : nullptr,
      metrics::kUnset);
}

absl::string_view GetApplication(const MatchRequest& request) {
  return Application_Name(request.application());
}

absl::string_view GetMatchKeyFormat(const MatchRequest& request) {
  return MatchKeyFormat_Name(request.match_key_format());
}

Metric CreateCountMetric(const MatchRequest& request) {
  Metric m;
  m.set_name(metrics::kRequestCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(request);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  return m;
}

Metric CreateDataRecordLevelRequestCountMetric(
    const MatchRequest& request, absl::string_view encryption_type,
    int64_t count) {
  Metric m;
  m.set_name(metrics::kDataRecordLevelRequestCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value(absl::StrCat(count));
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      std::string(encryption_type);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  return m;
}

Metric CreateMatchKeyLevelRequestCountMetric(const MatchRequest& request,
                                             absl::string_view encryption_type,
                                             int64_t count) {
  Metric m;
  m.set_name(metrics::kMatchKeyLevelRequestCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value(absl::StrCat(count));
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      std::string(encryption_type);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  return m;
}

Metric CreateLatencyMetric(const MatchRequest& request,
                           absl::Duration latency) {
  Metric m;
  m.set_name(metrics::kRequestLatencyMetricName);
  m.set_type(MetricType::METRIC_TYPE_HISTOGRAM);
  m.set_value(absl::StrCat(absl::ToInt64Milliseconds(latency)));
  m.set_unit(MetricUnit::METRIC_UNIT_MILLISECONDS);
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(request);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  return m;
}

Metric CreateErrorCountMetric(const MatchRequest& request,
                              absl::Status status) {
  Metric m;
  m.set_name(metrics::kErrorCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(request);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  (*m.mutable_labels())[metrics::kBackendErrorReasonLabel] =
      GetBackendErrorReasonString(status);
  return m;
}

Metric CreateGrpcErrorCountMetric(const MatchRequest& request,
                                  grpc::Status status) {
  Metric m;
  m.set_name(metrics::kGrpcErrorCountMetricName);
  m.set_type(MetricType::METRIC_TYPE_COUNTER);
  m.set_unit(MetricUnit::METRIC_UNIT_COUNT);
  m.set_value("1");
  (*m.mutable_labels())[metrics::kEncryptionTypeLabel] =
      GetEncryptionKeyType(request);
  (*m.mutable_labels())[metrics::kApplicationLabel] = GetApplication(request);
  (*m.mutable_labels())[metrics::kMatchKeyFormatLabel] =
      GetMatchKeyFormat(request);
  // There is no real way to stringify a grpc StatusCode. This is a workaround:
  // https://github.com/grpc/grpc/issues/34136#issuecomment-1698033324
  (*m.mutable_labels())[metrics::kGrpcStatusCodeLabel] =
      absl::StatusCodeToString(
          static_cast<absl::StatusCode>(status.error_code()));
  return m;
}

}  // namespace

// Helper to store context across a single request.
template <typename TRequest, typename TResponse>
class MatchService::RequestContext {
 public:
  // Constructs a RequestContext.
  //
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the HashedMatchTask object.
  //
  // The start time of the request is recorded at object construction time.
  //
  // server_context: The gRPC callback server context.
  // request: A pointer to the request object.
  // response: A pointer where the response should be written to.
  // logger: The logger object for the request.
  explicit RequestContext(CallbackServerContext* server_context,
                          const TRequest* request, TResponse* response,
                          MetricClientInterface* metric_client,
                          absl::string_view metric_namespace,
                          std::shared_ptr<Logger> logger)
      : grpc_context_(server_context),
        reactor_(server_context->DefaultReactor()),
        request_(request),
        response_(response),
        metric_client_(metric_client),
        metric_namespace_(metric_namespace),
        logger_(logger),
        start_time_(absl::Now()) {
    if constexpr (std::is_same_v<TRequest, MatchRequest>) {
      absl::flat_hash_map<absl::string_view, int64_t> record_encryption_counts;
      absl::flat_hash_map<absl::string_view, int64_t> key_encryption_counts;

      // Record record and match key level request counts.
      for (const auto& record : request_->data_records()) {
        absl::string_view record_encryption = GetEncryptionKeyType(
            record.has_encryption_key() ? &record.encryption_key() : nullptr,
            metrics::kUnset);
        record_encryption_counts[record_encryption]++;

        for (const auto& key : record.match_keys()) {
          // Resolve the actual encryption key for match key.
          absl::string_view key_encryption = GetEncryptionKeyType(
              ResolveEncryptionKeyForMatchKey(*request_, record, key));
          key_encryption_counts[key_encryption]++;
        }
      }

      std::vector<Metric> metrics;
      metrics.reserve(1 + record_encryption_counts.size() +
                      key_encryption_counts.size());
      // Record request level count.
      metrics.push_back(CreateCountMetric(*request_));
      // Record data record level count.
      for (const auto& [enc_type, count] : record_encryption_counts) {
        metrics.push_back(CreateDataRecordLevelRequestCountMetric(
            *request_, enc_type, count));
      }
      // Record match key level count.
      for (const auto& [enc_type, count] : key_encryption_counts) {
        metrics.push_back(
            CreateMatchKeyLevelRequestCountMetric(*request_, enc_type, count));
      }

      metrics::PutMetrics(logger_, metric_client_, metric_namespace_,
                          std::move(metrics));
    }
  }

  // Returns a pointer to the request object.
  const TRequest* Request() noexcept { return request_; }

  // Returns a pointer to where the response will be written.
  TResponse* Response() noexcept { return response_; }

  // Returns the gRPC server unary reactor for the request.
  ServerUnaryReactor* Reactor() noexcept { return reactor_; }

  // Returns a reference to the Logger object.
  const class Logger& Logger() noexcept { return *logger_; }

  // Returns the gRPC authentication context for this request.
  std::shared_ptr<const grpc::AuthContext> AuthContext() noexcept {
    return grpc_context_->auth_context();
  }

  // Finishes the request with the provided status.
  void Finish(absl::Status status) noexcept {
    if (!status.ok()) {
      if constexpr (std::is_same_v<TRequest, MatchRequest>) {
        metrics::PutMetric(logger_, metric_client_, metric_namespace_,
                           CreateErrorCountMetric(*request_, status));
      }
    }
    Finish(ToApiStatus(status), /*converted_from_absl=*/true);
  }

  // Finishes the request with the provided gRPC status.
  // We accept a converted_from_absl to help us separate the error metrics into
  // proper namespaces.
  void Finish(grpc::Status status, bool converted_from_absl = false) noexcept {
    if (!converted_from_absl && !status.ok()) {
      if constexpr (std::is_same_v<TRequest, MatchRequest>) {
        metrics::PutMetric(logger_, metric_client_, metric_namespace_,
                           CreateGrpcErrorCountMetric(*request_, status));
      }
    }
    const absl::Duration request_duration = absl::Now() - start_time_;
    double duration_sec = absl::ToDoubleSeconds(request_duration);

    // Log the time elapsed to handle the request
    LOG_INFO(*logger_, "Request %s, taking %.6fs to complete.%s",
             (status.ok() ? "succeeded" : "failed"), duration_sec,
             (status.ok()
                  ? ""
                  : absl::StrFormat(" (Error: %s)", status.error_message())));
    if constexpr (std::is_same_v<TRequest, MatchRequest>) {
      // Log the metric latency.
      metrics::PutMetric(logger_, metric_client_, metric_namespace_,
                         CreateLatencyMetric(*request_, request_duration));
    }

    reactor_->Finish(status);
  }

 private:
  // A pointer to the gRPC callback server context for the request.
  CallbackServerContext* grpc_context_;
  // A pointer to the gRPC reactor used to finish the request.
  ServerUnaryReactor* reactor_;
  // A pointer to the request object.
  const TRequest* request_;
  // A pointer to where the response should be written to.
  TResponse* response_;
  // The client to log metrics on.
  MetricClientInterface* metric_client_;
  // The metric namespace.
  absl::string_view metric_namespace_;
  // The logger object for the request.
  std::shared_ptr<class Logger> logger_;
  // The time at which the request started.
  absl::Time start_time_;
};

ServerUnaryReactor* MatchService::Match(CallbackServerContext* server_context,
                                        const MatchRequest* request,
                                        MatchResponse* response) noexcept {
  RequestContext ctx(server_context, request, response, metric_client_,
                     metric_namespace_,
                     std::make_shared<Logger>(GenerateRequestId(bitgen_)));

  LOG_INFO(ctx.Logger(), "Received match request.");
  if (!AuthorizeOrFinish(ctx)) {
    return ctx.Reactor();
  }

  ExecutionResult schedule_result = match_service_async_executor_.Schedule(
      [this, ctx]() -> void { ProcessMatchRequest(ctx); },
      AsyncPriority::Normal);
  if (!schedule_result.Successful()) {
    LOG_ERROR(ctx.Logger(), "Failed to schedule match request: %s",
              GetErrorMessage(schedule_result.status_code));
    ctx.Finish(grpc::Status(grpc::StatusCode::INTERNAL,
                            "Failed to schedule match request."));
  }

  return ctx.Reactor();
}

void MatchService::ProcessMatchRequest(
    RequestContext<MatchRequest, MatchResponse> ctx) noexcept {
  AsyncContext<backend::MatchRequest, backend::MatchResponse> async_context(
      std::make_shared<Logger>(ctx.Logger()));
  async_context.request = std::make_shared<backend::MatchRequest>();
  if (auto status = ToBackend(*ctx.Request(), *async_context.request);
      !status.ok()) {
    LOG_ERROR(ctx.Logger(),
              "Failed to convert match request to backend proto: %v", status);
    ctx.Finish(status);
    return;
  }
  async_context.callback =
      absl::bind_front(&MatchService::OnProcessMatchRequestCallback, this, ctx);

  match_task_->Match(async_context);
}

void MatchService::OnProcessMatchRequestCallback(
    RequestContext<MatchRequest, MatchResponse> ctx,
    AsyncContext<backend::MatchRequest, backend::MatchResponse>&
        result_context) noexcept {
  if (!result_context.status.ok()) {
    LOG_ERROR(ctx.Logger(), "Failed to process match request: %v",
              result_context.status);
    ctx.Finish(result_context.status);
    return;
  }

  if (auto status = ToApi(*result_context.response, *ctx.Response());
      !status.ok()) {
    LOG_ERROR(ctx.Logger(), "Failed to convert match response to API proto: %v",
              status);
    ctx.Finish(status);
    return;
  }

  LOG_INFO(ctx.Logger(), "Match request completed successfully.");
  ctx.Finish(absl::OkStatus());
}

template <typename TRequest, typename TResponse>
bool MatchService::AuthorizeOrFinish(
    RequestContext<TRequest, TResponse>& ctx) noexcept {
  if (allow_all_callers_) {
    LOG_INFO(ctx.Logger(),
             "Bypassing authorization, 'allow_all_callers' is enabled.");
    return true;
  }

  grpc::Status status = grpc::experimental::AltsClientAuthzCheck(
      ctx.AuthContext(), allowed_client_accounts_);
  if (status.ok()) {
    return true;
  }

  LOG_ERROR(ctx.Logger(), "Failed authorization: %s", status.error_message());
  ctx.Finish(std::move(status));
  return false;
}

}  // namespace google::confidential_match::match_service
