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

#include "cc/lookup_server/service/src/hashed_lookup_task.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"

#include "cc/lookup_server/converters/src/matched_data_record_converter.h"
#include "cc/lookup_server/service/src/error_codes.h"
#include "cc/lookup_server/service/src/public_error_response_functions.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::confidential_match::shared::api_errors::ErrorResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "HashedLookupTask";
}  // namespace

void HashedLookupTask::HandleRequest(
    AsyncContext<LookupRequest, LookupResponse> context) noexcept {
  context.response = std::make_shared<LookupResponse>();
  context.result = HandleRequest(*context.request, *context.response);
  context.Finish();
}

ExecutionResult HashedLookupTask::HandleRequest(
    const LookupRequest& request, LookupResponse& response) noexcept {
  for (const auto& record : request.data_records()) {
    LookupResult* lookup_result = response.add_lookup_results();
    *lookup_result->mutable_client_data_record() = record;

    ExecutionResultOr<std::vector<MatchDataRow>> matches_or =
        match_data_storage_->Get(record.lookup_key().key());
    if (!matches_or.Successful()) {
      lookup_result->set_status(LookupResult::STATUS_FAILED);
      ASSIGN_OR_LOG_AND_RETURN(
          ErrorResponse public_error_response,
          BuildPublicErrorResponse(matches_or.result()), kComponentName,
          kZeroUuid, "Failed to generate public-facing error response.");
      *lookup_result->mutable_error_response() =
          std::move(public_error_response);
      continue;
    }

    lookup_result->set_status(LookupResult::STATUS_SUCCESS);
    for (const auto& match_data_row : *matches_or) {
      RETURN_IF_FAILURE(ConvertToMatchedDataRecord(
          match_data_row, request.associated_data_keys(),
          *lookup_result->add_matched_data_records()));
    }
  }

  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
