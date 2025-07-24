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

#include "cc/lookup_server/scheme_validator/src/scheme_validator.h"

#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"

#include "cc/lookup_server/scheme_validator/src/error_codes.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    ShardingScheme;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;

constexpr absl::string_view kComponentName = "SchemeValidator";
constexpr absl::string_view kSupportedSchemeType = "jch";

ExecutionResult ValidateScheme(const ShardingScheme& sharding_scheme) {
  if (sharding_scheme.type() != kSupportedSchemeType) {
    ExecutionResult result =
        FailureExecutionResult(SCHEME_VALIDATOR_UNSUPPORTED_SCHEME_TYPE);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat("Scheme type '%s' is unsupported.",
                              sharding_scheme.type()));
    return result;
  }

  if (sharding_scheme.num_shards() <= 0) {
    ExecutionResult result =
        FailureExecutionResult(SCHEME_VALIDATOR_INVALID_REQUEST_SCHEME);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              absl::StrFormat("Number of shards '%d' must be greater than 0.",
                              sharding_scheme.num_shards()));
    return result;
  }

  return SuccessExecutionResult();
}

}  // namespace

ExecutionResult SchemeValidator::SetCurrentScheme(
    const ShardingScheme& sharding_scheme) noexcept {
  RETURN_IF_FAILURE(ValidateScheme(sharding_scheme));
  num_shards_.store(sharding_scheme.num_shards());
  return SuccessExecutionResult();
}

ExecutionResult SchemeValidator::SetPendingScheme(
    const ShardingScheme& sharding_scheme) noexcept {
  // Ensures that no other threads are setting or applying at the same time
  absl::MutexLock lock(&pending_num_shards_mutex_);

  RETURN_IF_FAILURE(ValidateScheme(sharding_scheme));
  pending_num_shards_ = sharding_scheme.num_shards();
  return SuccessExecutionResult();
}

ExecutionResult SchemeValidator::ApplyPendingScheme() noexcept {
  // Ensures that no other threads are setting or applying at the same time
  absl::MutexLock lock(&pending_num_shards_mutex_);

  if (pending_num_shards_ == 0) {
    ExecutionResult result =
        FailureExecutionResult(SCHEME_VALIDATOR_PENDING_SCHEME_NOT_SET);
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "The pending scheme must be set before applying.");
    return result;
  }

  num_shards_.store(pending_num_shards_);
  pending_num_shards_ = 0;
  return SuccessExecutionResult();
}

bool SchemeValidator::IsValidRequestScheme(
    const ShardingScheme& request_sharding_scheme) noexcept {
  if (!ValidateScheme(request_sharding_scheme).Successful()) {
    return false;
  }

  // The server can handle a request when it contains data for the full range
  // of hashes for the request's sharding scheme. This occurs if the server's
  // dataset is sharded with the same or fewer number of shards
  return num_shards_ <= request_sharding_scheme.num_shards();
}

}  // namespace google::confidential_match::lookup_server
