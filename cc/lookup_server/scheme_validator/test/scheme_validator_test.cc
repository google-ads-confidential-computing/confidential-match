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

#include <memory>

#include "absl/strings/str_format.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

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
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;

constexpr absl::string_view kShardingSchemeType = "jch";

class SchemeValidatorTest : public testing::Test {
 protected:
  SchemeValidator scheme_validator_;
};

TEST_F(SchemeValidatorTest, SetCurrentSchemeIsSuccessful) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);

  ExecutionResult result = scheme_validator_.SetCurrentScheme(sharding_scheme);

  EXPECT_SUCCESS(result);
}

TEST_F(SchemeValidatorTest, SetCurrentSchemeWithInvalidSchemeReturnsError) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = "invalid";
  sharding_scheme.set_num_shards(1);

  ExecutionResult result = scheme_validator_.SetCurrentScheme(sharding_scheme);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_UNSUPPORTED_SCHEME_TYPE)));
}

TEST_F(SchemeValidatorTest, SetCurrentSchemeWithInvalidNumShardsReturnsError) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(0);

  ExecutionResult result = scheme_validator_.SetCurrentScheme(sharding_scheme);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_INVALID_REQUEST_SCHEME)));
}

TEST_F(SchemeValidatorTest, SetPendingSchemeIsSuccessful) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);

  ExecutionResult result = scheme_validator_.SetPendingScheme(sharding_scheme);

  EXPECT_SUCCESS(result);
}

TEST_F(SchemeValidatorTest, SetPendingSchemeWithInvalidSchemeReturnsError) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = "invalid";
  sharding_scheme.set_num_shards(1);

  ExecutionResult result = scheme_validator_.SetPendingScheme(sharding_scheme);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_UNSUPPORTED_SCHEME_TYPE)));
}

TEST_F(SchemeValidatorTest, SetPendingSchemeWithInvalidNumShardsReturnsError) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(0);

  ExecutionResult result = scheme_validator_.SetPendingScheme(sharding_scheme);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_INVALID_REQUEST_SCHEME)));
}

TEST_F(SchemeValidatorTest, ApplyPendingSchemeIsSuccessful) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetPendingScheme(sharding_scheme));

  ExecutionResult result = scheme_validator_.ApplyPendingScheme();

  EXPECT_SUCCESS(result);
}

TEST_F(SchemeValidatorTest, ApplyPendingSchemeMultipleTimesIsSuccessful) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetPendingScheme(sharding_scheme));
  ShardingScheme sharding_scheme2;
  *sharding_scheme2.mutable_type() = kShardingSchemeType;
  sharding_scheme2.set_num_shards(2);

  ExecutionResult result = scheme_validator_.ApplyPendingScheme();
  EXPECT_SUCCESS(scheme_validator_.SetPendingScheme(sharding_scheme2));
  ExecutionResult second_result = scheme_validator_.ApplyPendingScheme();

  EXPECT_SUCCESS(result);
  EXPECT_SUCCESS(second_result);
}

TEST_F(SchemeValidatorTest, ApplyPendingSchemeWithoutSettingReturnsError) {
  ExecutionResult result = scheme_validator_.ApplyPendingScheme();
  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          SCHEME_VALIDATOR_PENDING_SCHEME_NOT_SET)));
}

TEST_F(SchemeValidatorTest, ApplyPendingSchemeTwiceSettingOnceReturnsError) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetPendingScheme(sharding_scheme));

  ExecutionResult result = scheme_validator_.ApplyPendingScheme();
  ExecutionResult second_result = scheme_validator_.ApplyPendingScheme();

  EXPECT_SUCCESS(result);
  EXPECT_THAT(second_result, ResultIs(FailureExecutionResult(
                                 SCHEME_VALIDATOR_PENDING_SCHEME_NOT_SET)));
}

TEST_F(SchemeValidatorTest, IsValidRequestSchemeWithMatchingSchemeReturnsTrue) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));

  bool result = scheme_validator_.IsValidRequestScheme(sharding_scheme);

  EXPECT_TRUE(result);
}

TEST_F(SchemeValidatorTest,
       IsValidRequestSchemeWithLargerRequestSchemeReturnsTrue) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = kShardingSchemeType;
  request_sharding_scheme.set_num_shards(2);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_TRUE(result);
}

TEST_F(SchemeValidatorTest,
       IsValidRequestSchemeWithSmallerRequestSchemeReturnsFalse) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(2);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = kShardingSchemeType;
  request_sharding_scheme.set_num_shards(1);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_FALSE(result);
}

TEST_F(SchemeValidatorTest,
       IsValidRequestSchemeWithInvalidSchemeTypeReturnsFalse) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = "invalid";
  request_sharding_scheme.set_num_shards(1);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_FALSE(result);
}

TEST_F(SchemeValidatorTest,
       IsValidRequestSchemeWithInvalidNumShardsReturnsFalse) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = kShardingSchemeType;
  request_sharding_scheme.set_num_shards(-1);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_FALSE(result);
}

TEST_F(SchemeValidatorTest,
       IsValidRequestSchemeWithUnsetCurrentSchemeReturnsTrue) {
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = kShardingSchemeType;
  request_sharding_scheme.set_num_shards(1);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_TRUE(result);
}

TEST_F(SchemeValidatorTest, IsValidRequestSchemeWithCurrentAndPendingScheme) {
  ShardingScheme pending_sharding_scheme;
  *pending_sharding_scheme.mutable_type() = kShardingSchemeType;
  pending_sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetPendingScheme(pending_sharding_scheme));
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(3);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = kShardingSchemeType;
  request_sharding_scheme.set_num_shards(2);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_FALSE(result);
}

TEST_F(SchemeValidatorTest, IsValidRequestSchemeWithAppliedScheme) {
  ShardingScheme sharding_scheme;
  *sharding_scheme.mutable_type() = kShardingSchemeType;
  sharding_scheme.set_num_shards(3);
  EXPECT_SUCCESS(scheme_validator_.SetCurrentScheme(sharding_scheme));
  ShardingScheme pending_sharding_scheme;
  *pending_sharding_scheme.mutable_type() = kShardingSchemeType;
  pending_sharding_scheme.set_num_shards(1);
  EXPECT_SUCCESS(scheme_validator_.SetPendingScheme(pending_sharding_scheme));
  EXPECT_SUCCESS(scheme_validator_.ApplyPendingScheme());
  ShardingScheme request_sharding_scheme;
  *request_sharding_scheme.mutable_type() = kShardingSchemeType;
  request_sharding_scheme.set_num_shards(2);

  bool result = scheme_validator_.IsValidRequestScheme(request_sharding_scheme);

  EXPECT_TRUE(result);
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
