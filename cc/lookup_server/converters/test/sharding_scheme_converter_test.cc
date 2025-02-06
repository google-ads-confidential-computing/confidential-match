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

#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "public/core/interface/execution_result.h"

#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

TEST(ShardingSchemeConverterTest,
     ConvertShardingSchemeToBackendProtoConvertsEmptyProto) {
  proto_api::LookupRequest::ShardingScheme sharding_scheme;
  proto_backend::ShardingScheme out;

  ExecutionResult result = ConvertShardingScheme(sharding_scheme, out);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(out.type(), "");
  EXPECT_EQ(out.num_shards(), 0);
}

TEST(ShardingSchemeConverterTest,
     ConvertShardingSchemeToBackendProtoConvertsType) {
  proto_api::LookupRequest::ShardingScheme sharding_scheme;
  sharding_scheme.set_type("test");
  proto_backend::ShardingScheme out;

  ExecutionResult result = ConvertShardingScheme(sharding_scheme, out);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(out.type(), "test");
}

TEST(ShardingSchemeConverterTest,
     ConvertShardingSchemeToBackendProtoConvertsNumShards) {
  proto_api::LookupRequest::ShardingScheme sharding_scheme;
  sharding_scheme.set_num_shards(1);
  proto_backend::ShardingScheme out;

  ExecutionResult result = ConvertShardingScheme(sharding_scheme, out);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(out.num_shards(), 1);
}

TEST(ShardingSchemeConverterTest,
     ConvertShardingSchemeToApiProtoConvertsEmptyProto) {
  proto_backend::ShardingScheme sharding_scheme;
  proto_api::LookupRequest::ShardingScheme out;

  ExecutionResult result = ConvertShardingScheme(sharding_scheme, out);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(out.type(), "");
  EXPECT_EQ(out.num_shards(), 0);
}

TEST(ShardingSchemeConverterTest, ConvertShardingSchemeToApiProtoConvertsType) {
  proto_backend::ShardingScheme sharding_scheme;
  sharding_scheme.set_type("test");
  proto_api::LookupRequest::ShardingScheme out;

  ExecutionResult result = ConvertShardingScheme(sharding_scheme, out);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(out.type(), "test");
}

TEST(ShardingSchemeConverterTest,
     ConvertShardingSchemeToApiProtoConvertsNumShards) {
  proto_backend::ShardingScheme sharding_scheme;
  sharding_scheme.set_num_shards(1);
  proto_api::LookupRequest::ShardingScheme out;

  ExecutionResult result = ConvertShardingScheme(sharding_scheme, out);

  EXPECT_SUCCESS(result);
  EXPECT_EQ(out.num_shards(), 1);
}

}  // namespace google::confidential_match::lookup_server
