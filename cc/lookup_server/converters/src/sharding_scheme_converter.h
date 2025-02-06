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

#ifndef CC_LOOKUP_SERVER_CONVERTERS_SRC_SHARDING_SCHEME_CONVERTER_H_
#define CC_LOOKUP_SERVER_CONVERTERS_SRC_SHARDING_SCHEME_CONVERTER_H_

#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Converts a ShardingScheme API proto to a ShardingScheme backend
 * proto.
 *
 * @param sharding_scheme the ShardingScheme to convert
 * @param out the ShardingScheme to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertShardingScheme(
    const proto_api::LookupRequest::ShardingScheme& sharding_scheme,
    proto_backend::ShardingScheme& out);

/**
 * @brief Converts a ShardingScheme backend proto to a ShardingScheme API
 * proto.
 *
 * @param sharding_scheme the ShardingScheme to convert
 * @param out the ShardingScheme to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertShardingScheme(
    const proto_backend::ShardingScheme& sharding_scheme,
    proto_api::LookupRequest::ShardingScheme& out);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CONVERTERS_SRC_SHARDING_SCHEME_CONVERTER_H_
