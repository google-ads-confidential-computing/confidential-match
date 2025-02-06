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

#include "cc/lookup_server/converters/src/sharding_scheme_converter.h"

#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/converters/src/error_codes.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

ExecutionResult ConvertShardingScheme(
    const proto_api::LookupRequest::ShardingScheme& sharding_scheme,
    proto_backend::ShardingScheme& out) {
  out = proto_backend::ShardingScheme();
  out.set_type(sharding_scheme.type());
  out.set_num_shards(sharding_scheme.num_shards());
  return SuccessExecutionResult();
}

ExecutionResult ConvertShardingScheme(
    const proto_backend::ShardingScheme& sharding_scheme,
    proto_api::LookupRequest::ShardingScheme& out) {
  out = proto_api::LookupRequest::ShardingScheme();
  out.set_type(sharding_scheme.type());
  out.set_num_shards(sharding_scheme.num_shards());
  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
