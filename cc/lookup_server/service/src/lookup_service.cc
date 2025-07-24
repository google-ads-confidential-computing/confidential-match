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

#include "cc/lookup_server/service/src/lookup_service.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/errors.h"
#include "cc/core/interface/http_types.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "cc/public/cpio/proto/metric_service/v1/metric_service.pb.h"
#include "cc/public/cpio/utils/metric_instance/interface/aggregate_metric_interface.h"
#include "cc/public/cpio/utils/metric_instance/interface/type_def.h"
#include "cc/public/cpio/utils/metric_instance/src/aggregate_metric.h"
#include "cc/public/cpio/utils/metric_instance/src/metric_utils.h"

#include "cc/lookup_server/converters/src/matched_data_record_converter.h"
#include "cc/lookup_server/converters/src/sharding_scheme_converter.h"
#include "cc/lookup_server/interface/metric_client_interface.h"
#include "cc/lookup_server/public/src/error_codes.h"
#include "cc/lookup_server/service/src/error_codes.h"
#include "cc/lookup_server/service/src/json_serialization_functions.h"
#include "cc/lookup_server/service/src/public_error_response_functions.h"
#include "protos/lookup_server/api/healthcheck.pb.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"
#include "protos/shared/api/errors/code.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_api::
    HealthcheckResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::lookup_server::proto_backend::
    ServiceStatus_Name;
using ::google::confidential_match::lookup_server::proto_backend::
    ShardingScheme;
using ::google::confidential_match::shared::api_errors::Code;
using ::google::confidential_match::shared::api_errors::Details;
using ::google::confidential_match::shared::api_errors::ErrorResponse;
using ::google::protobuf::Message;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::Byte;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpHandler;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::common::ToString;
using ::google::scp::core::common::Uuid;
using ::google::scp::core::errors::GetErrorHttpStatusCode;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::GetPublicErrorCode;
using ::google::scp::core::errors::HttpStatusCode;
using ::google::scp::cpio::AggregateMetricInterface;
using ::google::scp::cpio::Callback;
using ::google::scp::cpio::MetricDefinition;
using ::google::scp::cpio::MetricInstanceFactoryInterface;
using ::google::scp::cpio::MetricUtils;
using ::std::placeholders::_1;

using CpioMetricClientInterface = ::google::scp::cpio::MetricClientInterface;
using HashInfo = ::google::confidential_match::lookup_server::proto_api::
    LookupRequest::HashInfo;

constexpr absl::string_view kComponentName = "LookupService";
constexpr char kHealthcheckPath[] = "/v1/healthcheck";
constexpr char kLookupPath[] = "/v1/lookup";

constexpr absl::string_view kErrorReasonInvalidScheme = "INVALID_SCHEME";

constexpr char kMetricNameSpace[] = "gce_instance";
constexpr uint64_t kMetricWriteDelayMs = 10000;
constexpr absl::string_view kRequestCountMetricName = "request_count";
constexpr char kRequestCountMetricLabel[] = "Count";
constexpr char kRequestErrorMetricLabel[] = "Error";
constexpr char kRequestInvalidSchemeMetricLabel[] = "InvalidScheme";
// Metric measuring latency of requests.
constexpr absl::string_view kRequestLatencyMetricName = "request_latency";
// Metric measuring request errors requiring oncall investigation.
constexpr absl::string_view kRequestErrorMetricName = "request_error";
// Metric measuring requests with invalid sharding schemes. This is expected
// during a during scale-up operation.
constexpr absl::string_view kRequestInvalidSchemeMetricName =
    "request_invalid_scheme";

constexpr absl::string_view kContentTypeHeader = "content-type";
constexpr absl::string_view kContentTypeOctetStream =
    "application/octet-stream";

// Helper to retrieve the header value from HttpHeaders, if present.
ExecutionResultOr<std::string> GetHeader(const HttpHeaders& headers,
                                         absl::string_view header) {
  if (auto it = headers.find(std::string(header)); it != headers.end()) {
    return it->second;
  }
  return FailureExecutionResult(LOOKUP_SERVICE_HEADER_NOT_FOUND);
}

// Determines whether or not an HTTP request is binary or JSON formatted.
bool IsBinaryFormatRequest(const HttpRequest& request) {
  ExecutionResultOr<std::string> content_type_or =
      GetHeader(*request.headers, kContentTypeHeader);
  return content_type_or.Successful() &&
         *content_type_or == kContentTypeOctetStream;
}

// Helper to write a proto serialized into binary format into an HttpResponse.
ExecutionResult WriteBinaryHttpResponse(
    HttpResponse& http_response, const google::protobuf::Message& response) {
  std::string serialized_response = response.SerializeAsString();
  http_response.body.capacity = serialized_response.length();
  http_response.body.length = serialized_response.length();
  http_response.body.bytes = std::make_shared<std::vector<scp::core::Byte>>(
      serialized_response.begin(), serialized_response.end());
  return SuccessExecutionResult();
}

// Writes a proto to a response context, returning whether or not the operation
// was successful.
bool WriteMessageToResponse(
    const Message& message,
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  if (http_context.response == nullptr) {
    http_context.response = std::make_shared<HttpResponse>();
  }

  if (IsBinaryFormatRequest(*http_context.request)) {
    return WriteBinaryHttpResponse(*http_context.response, message)
        .Successful();
  }

  // Binary format not requested, use JSON output
  return ProtoToJsonBytesBuffer(message, &http_context.response->body).ok();
}

// Writes a generic error response to the response context based on a failed
// ExecutionResult.
ExecutionResult WriteFailedResultToResponse(
    const ExecutionResult& result,
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  ASSIGN_OR_RETURN(ErrorResponse error_response,
                   BuildPublicErrorResponse(result));
  if (!WriteMessageToResponse(error_response, http_context)) {
    return FailureExecutionResult(LOOKUP_SERVICE_INTERNAL_ERROR);
  }
  return SuccessExecutionResult();
}

ExecutionResult WriteInvalidSchemeResponse(
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  ErrorResponse error_response;
  error_response.set_code(Code::INVALID_ARGUMENT);
  error_response.set_message("The request sharding scheme is not valid.");
  Details* error_details = error_response.add_details();
  *error_details->mutable_reason() = kErrorReasonInvalidScheme;
  *error_details->mutable_domain() = kComponentName;

  if (!WriteMessageToResponse(error_response, http_context)) {
    return FailureExecutionResult(LOOKUP_SERVICE_INTERNAL_ERROR);
  }
  return SuccessExecutionResult();
}

std::shared_ptr<AggregateMetricInterface> CreateAggregateMetric(
    std::shared_ptr<MetricInstanceFactoryInterface>& metric_instance_factory,
    absl::string_view metric_name,
    std::vector<std::string>& label_list) noexcept {
  std::map<std::string, std::string> metric_labels =
      MetricUtils::CreateMetricLabelsWithComponentSignature(
          std::string(kComponentName));
  MetricDefinition metric_info(std::string(metric_name),
                               MetricUnit::METRIC_UNIT_COUNT, kMetricNameSpace,
                               std::move(metric_labels));
  return metric_instance_factory->ConstructAggregateMetricInstance(
      std::move(metric_info), label_list);
}

}  // namespace

ExecutionResult LookupService::Init() noexcept {
  last_request_time_ms_ = absl::ToUnixMillis(absl::Time());
  std::string healthcheck_path(kHealthcheckPath);
  HttpHandler get_healthcheck_handler =
      std::bind(&LookupService::GetHealthcheck, this, _1);
  http_server_->RegisterResourceHandler(HttpMethod::GET, healthcheck_path,
                                        get_healthcheck_handler);

  // Using POST instead of GET since the lookup API accepts a large payload
  // in the request
  std::string lookup_path(kLookupPath);
  HttpHandler post_lookup_handler =
      std::bind(&LookupService::PostLookup, this, _1);
  http_server_->RegisterResourceHandler(HttpMethod::POST, lookup_path,
                                        post_lookup_handler);

  RETURN_IF_FAILURE(InitMetrics());
  return SuccessExecutionResult();
}

ExecutionResult LookupService::InitMetrics() noexcept {
  RETURN_IF_FAILURE(BuildAggregateMetrics());
  RETURN_IF_FAILURE(request_count_metrics_->Init());
  RETURN_IF_FAILURE(request_error_metrics_->Init());
  RETURN_IF_FAILURE(request_invalid_scheme_metrics_->Init());
  return SuccessExecutionResult();
}

ExecutionResult LookupService::Run() noexcept {
  RETURN_IF_FAILURE(RunMetrics());
  return SuccessExecutionResult();
}

ExecutionResult LookupService::RunMetrics() noexcept {
  RETURN_IF_FAILURE(request_count_metrics_->Run());
  RETURN_IF_FAILURE(request_error_metrics_->Run());
  RETURN_IF_FAILURE(request_invalid_scheme_metrics_->Run());
  return SuccessExecutionResult();
}

ExecutionResult LookupService::Stop() noexcept {
  RETURN_IF_FAILURE(StopMetrics());
  return SuccessExecutionResult();
}

ExecutionResult LookupService::StopMetrics() noexcept {
  RETURN_IF_FAILURE(request_count_metrics_->Stop());
  RETURN_IF_FAILURE(request_error_metrics_->Stop());
  RETURN_IF_FAILURE(request_invalid_scheme_metrics_->Run());
  return SuccessExecutionResult();
}

ExecutionResult LookupService::GetHealthcheck(
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  ExecutionResult result = SuccessExecutionResult();

  HealthcheckResponse response;
  response.set_response_id(ToString(Uuid::GenerateUuid()));
  for (const auto& name_and_status_provider : service_status_providers_) {
    proto_api::Service* lookup_service = response.add_services();
    lookup_service->set_name(name_and_status_provider.first);
    lookup_service->set_status(
        ServiceStatus_Name(name_and_status_provider.second->GetStatus()));
  }

  if (!ProtoToJsonBytesBuffer(response, &http_context.response->body).ok()) {
    result = FailureExecutionResult(LOOKUP_SERVICE_INTERNAL_ERROR);
  }

  http_context.result = result;
  http_context.Finish();
  return SuccessExecutionResult();
}

ExecutionResult LookupService::PostLookup(
    AsyncContext<HttpRequest, HttpResponse>& http_context) noexcept {
  request_count_metrics_->Increment(kRequestCountMetricLabel);
  const absl::Time request_start_time = absl::Now();

  auto request = std::make_shared<LookupRequest>();
  bool request_parsed_successfully = false;
  if (IsBinaryFormatRequest(*http_context.request)) {
    absl::string_view binary_format_data(
        http_context.request->body.bytes->data(),
        http_context.request->body.length);
    request_parsed_successfully = request->ParseFromString(binary_format_data);
  } else {
    // No content type header provided, assume JSON input
    request_parsed_successfully =
        JsonBytesBufferToProto(http_context.request->body, request.get()).ok();
  }
  if (!request_parsed_successfully) {
    request_error_metrics_->Increment(kRequestErrorMetricLabel);
    PutLatencyMetrics(request_start_time);
    ExecutionResult result =
        FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST);
    SCP_ERROR_CONTEXT(
        kComponentName, http_context, result,
        absl::StrFormat("Request error: Failed to parse lookup request. "
                        "(Request format: %s)",
                        IsBinaryFormatRequest(*http_context.request)
                            ? "Binary format"
                            : "JSON"));
    http_context.result = result;
    http_context.Finish();
    return SuccessExecutionResult();
  }

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = request;
  lookup_context.callback =
      std::bind(&LookupService::OnPostLookupHandlerCallback, this, _1,
                http_context, request_start_time);
  PostLookupHandler(lookup_context);
  return SuccessExecutionResult();
}

void LookupService::PostLookupHandler(
    AsyncContext<LookupRequest, LookupResponse> context) noexcept {
  ShardingScheme request_scheme;
  ExecutionResult convert_result =
      ConvertShardingScheme(context.request->sharding_scheme(), request_scheme);
  if (!convert_result.Successful()) {
    context.result = convert_result;
    context.Finish();
    return;
  }
  if (!match_data_storage_->IsValidRequestScheme(request_scheme)) {
    SCP_INFO_CONTEXT(
        kComponentName, context,
        absl::StrFormat(
            "Request rejected due to unsupported sharding scheme: '%s'.",
            request_scheme.DebugString()));
    context.result =
        FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST_SCHEME);
    context.Finish();
    return;
  }

  if (context.request->hash_info().hash_type() != HashInfo::HASH_TYPE_SHA_256) {
    context.result = FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST);
    SCP_ERROR_CONTEXT(
        kComponentName, context, context.result,
        absl::StrFormat(
            "Request rejected due to invalid hash type: %s",
            HashInfo::HashType_Name(context.request->hash_info().hash_type())));
    context.Finish();
    return;
  }

  if (context.request->key_format() == LookupRequest::KEY_FORMAT_HASHED) {
    hashed_lookup_task_.HandleRequest(context);
    return;
  }

  if (context.request->key_format() ==
      LookupRequest::KEY_FORMAT_HASHED_ENCRYPTED) {
    if (context.request->encryption_key_info().has_wrapped_key_info()) {
      kms_encrypted_lookup_task_.HandleRequest(context);
      return;
    } else if (context.request->encryption_key_info()
                   .has_coordinator_key_info()) {
      coordinator_encrypted_lookup_task_.HandleRequest(context);
      return;
    }
    context.result = FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST);
    SCP_ERROR(kComponentName, kZeroUuid, context.result,
              "The encrypted lookup request is missing encryption key info.");
    context.Finish();
    return;
  }

  context.result = FailureExecutionResult(LOOKUP_SERVICE_INVALID_KEY_FORMAT);
  SCP_INFO_CONTEXT(
      kComponentName, context,
      absl::StrFormat(
          "Request rejected due to invalid key format: '%s'.",
          LookupRequest::KeyFormat_Name(context.request->key_format())));
  context.Finish();
}

void LookupService::OnPostLookupHandlerCallback(
    AsyncContext<LookupRequest, LookupResponse>& lookup_context,
    AsyncContext<HttpRequest, HttpResponse> http_context,
    absl::Time request_start_time) noexcept {
  const ExecutionResult lookup_result = lookup_context.result;
  if (!lookup_result.Successful()) {
    if (lookup_result ==
        FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST_SCHEME)) {
      request_invalid_scheme_metrics_->Increment(
          kRequestInvalidSchemeMetricLabel);
      if (!WriteInvalidSchemeResponse(http_context).Successful()) {
        request_error_metrics_->Increment(kRequestErrorMetricLabel);
        SCP_ERROR_CONTEXT(kComponentName, http_context, lookup_result,
                          "Request error: Failed to write error response for "
                          "invalid scheme.");
      }
      PutLatencyMetrics(request_start_time);
      http_context.result = lookup_result;
      http_context.Finish();
      return;
    }
    // Write a generic public-facing error response
    if (!WriteFailedResultToResponse(lookup_result, http_context)
             .Successful()) {
      SCP_ERROR_CONTEXT(kComponentName, http_context, lookup_result,
                        "Failed to write error response for failed result.");
    }
    request_error_metrics_->Increment(kRequestErrorMetricLabel);
    SCP_ERROR_CONTEXT(kComponentName, http_context, lookup_result,
                      "Request error: Failed to process lookup request.");
    PutLatencyMetrics(request_start_time);
    http_context.result = lookup_result;
    http_context.Finish();
    return;
  }

  if (!WriteMessageToResponse(*lookup_context.response, http_context)) {
    request_error_metrics_->Increment(kRequestErrorMetricLabel);
    PutLatencyMetrics(request_start_time);
    http_context.result = FailureExecutionResult(LOOKUP_SERVICE_INTERNAL_ERROR);
    SCP_ERROR_CONTEXT(
        kComponentName, http_context, http_context.result,
        absl::StrFormat("Request error: Failed to write lookup response. "
                        "(Response format: %s)",
                        IsBinaryFormatRequest(*http_context.request)
                            ? "Binary format"
                            : "JSON"));
    http_context.Finish();
    return;
  }

  http_context.result = SuccessExecutionResult();
  http_context.Finish();
  PutLatencyMetrics(request_start_time);
}

ExecutionResult LookupService::BuildAggregateMetrics() noexcept {
  std::vector<std::string> count_event_list = {kRequestCountMetricLabel};
  request_count_metrics_ = CreateAggregateMetric(
      metric_instance_factory_, kRequestCountMetricName, count_event_list);

  std::vector<std::string> error_event_list = {kRequestErrorMetricLabel};
  request_error_metrics_ = CreateAggregateMetric(
      metric_instance_factory_, kRequestErrorMetricName, error_event_list);

  std::vector<std::string> invalid_scheme_event_list = {
      kRequestInvalidSchemeMetricLabel};
  request_invalid_scheme_metrics_ = CreateAggregateMetric(
      metric_instance_factory_, kRequestInvalidSchemeMetricName,
      invalid_scheme_event_list);

  return SuccessExecutionResult();
}

ExecutionResult LookupService::PutLatencyMetrics(
    absl::Time request_start_time) noexcept {
  const absl::Duration request_duration = absl::Now() - request_start_time;
  double request_ms = absl::ToDoubleMilliseconds(request_duration);
  uint64_t last_request_time = last_request_time_ms_.load();
  uint64_t now = absl::ToUnixMillis(absl::Now());

  // TODO(b/309462821): refactor with batching when available from SCP
  if (now - last_request_time <= kMetricWriteDelayMs) {
    return SuccessExecutionResult();
  }

  if (!last_request_time_ms_.compare_exchange_strong(last_request_time, now)) {
    // Return since another thread is currently updating the metrics
    return SuccessExecutionResult();
  }

  return metric_client_->RecordMetric(kRequestLatencyMetricName,
                                      std::to_string(request_ms),
                                      MetricUnit::METRIC_UNIT_MILLISECONDS);
}

}  // namespace google::confidential_match::lookup_server
