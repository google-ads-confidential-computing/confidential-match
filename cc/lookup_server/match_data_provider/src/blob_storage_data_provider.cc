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

#include "cc/lookup_server/match_data_provider/src/blob_storage_data_provider.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/lookup_server/match_data_provider/src/error_codes.h"
#include "cc/lookup_server/metric_client/src/metric_client.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"
#include "cc/public/cpio/proto/blob_storage_service/v1/blob_storage_service.pb.h"
#include "protos/lookup_server/backend/location.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobRequest;
using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobResponse;
using ::google::confidential_match::lookup_server::proto_backend::Location;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::cpio::BlobStorageClientInterface;
using ::std::placeholders::_1;

constexpr absl::string_view kComponentName = "BlobStorageDataProvider";

ExecutionResult BlobStorageDataProvider::Init() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult BlobStorageDataProvider::Run() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult BlobStorageDataProvider::Stop() noexcept {
  return SuccessExecutionResult();
}

ExecutionResult BlobStorageDataProvider::Get(
    AsyncContext<Location, std::string> get_context) noexcept {
  RecordGetRequestCountMetric();
  absl::Time start_time = absl::Now();
  AsyncContext<GetBlobRequest, GetBlobResponse> get_blob_context;
  get_blob_context.request = std::make_shared<GetBlobRequest>();
  get_blob_context.request->mutable_blob_metadata()->set_bucket_name(
      get_context.request->blob_storage_location().bucket_name());
  get_blob_context.request->mutable_blob_metadata()->set_blob_name(
      get_context.request->blob_storage_location().path());
  get_blob_context.callback =
      std::bind(&BlobStorageDataProvider::OnGetBlobCallback, this, _1,
                get_context, start_time);

  blob_storage_client_->GetBlob(get_blob_context);
  return SuccessExecutionResult();
}

void BlobStorageDataProvider::OnGetBlobCallback(
    AsyncContext<GetBlobRequest, GetBlobResponse>& get_blob_context,
    AsyncContext<Location, std::string> parent_context,
    absl::Time start_time) noexcept {
  if (!get_blob_context.result.Successful()) {
    RecordGetRequestLatencyMetric(absl::Now() - start_time,
                                  /*is_successful=*/false);
    RecordGetRequestErrorCountMetric(get_blob_context.result);
    SCP_ERROR_CONTEXT(
        kComponentName, get_blob_context, get_blob_context.result,
        absl::StrFormat("Failed to read contents from blob storage: %s",
                        GetErrorMessage(get_blob_context.result.status_code)));
    parent_context.result =
        FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
    parent_context.Finish();
    return;
  }

  if (get_blob_context.response->has_result()) {
    ExecutionResult blob_result =
        ExecutionResult(get_blob_context.response->result());
    if (!blob_result.Successful()) {
      RecordGetRequestLatencyMetric(absl::Now() - start_time,
                                    /*is_successful=*/false);
      RecordGetRequestErrorCountMetric(blob_result);
      SCP_ERROR_CONTEXT(
          kComponentName, get_blob_context, blob_result,
          absl::StrFormat(
              "Got error response proto on reading blob storage contents: %s",
              GetErrorMessage(blob_result.status_code)));
      parent_context.result =
          FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
      parent_context.Finish();
      return;
    }
  }

  RecordGetRequestLatencyMetric(absl::Now() - start_time,
                                /*is_successful=*/true);
  parent_context.result = SuccessExecutionResult();
  parent_context.response = std::make_shared<std::string>(
      std::move(*get_blob_context.response->mutable_blob()->mutable_data()));
  parent_context.Finish();
}

void BlobStorageDataProvider::RecordGetRequestCountMetric() noexcept {
  if (metric_client_ == nullptr) {
    return;
  }
  metric_client_->RecordMetric(kDataProviderGetRequestCountMetricName, "1",
                               MetricUnit::METRIC_UNIT_COUNT,
                               MetricType::METRIC_TYPE_COUNTER,
                               /*labels=*/{});
}

void BlobStorageDataProvider::RecordGetRequestErrorCountMetric(
    const ExecutionResult& result) noexcept {
  if (metric_client_ == nullptr) {
    return;
  }
  absl::flat_hash_map<std::string, std::string> labels;
  labels[std::string(kBackendErrorReasonLabel)] =
      GetErrorMessage(result.status_code);
  metric_client_->RecordMetric(kDataProviderGetRequestErrorCountMetricName, "1",
                               MetricUnit::METRIC_UNIT_COUNT,
                               MetricType::METRIC_TYPE_COUNTER, labels);
}

void BlobStorageDataProvider::RecordGetRequestLatencyMetric(
    absl::Duration latency, bool is_successful) noexcept {
  if (metric_client_ == nullptr) {
    return;
  }
  absl::flat_hash_map<std::string, std::string> labels;
  labels[std::string(kIsSuccessfulLabel)] =
      is_successful ? std::string(kTrueMetricValue)
                    : std::string(kFalseMetricValue);
  metric_client_->RecordMetric(kDataProviderGetRequestLatencyMetricName,
                               absl::StrCat(absl::ToDoubleSeconds(latency)),
                               MetricUnit::METRIC_UNIT_SECONDS,
                               MetricType::METRIC_TYPE_GAUGE, labels);
}

}  // namespace google::confidential_match::lookup_server
