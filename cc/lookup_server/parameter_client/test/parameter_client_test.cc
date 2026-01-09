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

#include "cc/lookup_server/parameter_client/src/parameter_client.h"

#include <list>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_join.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/lookup_server/converters/src/error_codes.h"
#include "cc/lookup_server/parameter_client/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/interface/parameter_client/parameter_client_interface.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "public/cpio/mock/parameter_client/mock_parameter_client.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::parameter_service::v1::GetParameterRequest;
using ::google::cmrt::sdk::parameter_service::v1::GetParameterResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::testing::Return;

constexpr absl::string_view kParamPrefix = "CFM";
constexpr absl::string_view kTestEnvName = "test";
constexpr char kStringParameterName[] = "stringParam";
constexpr char kStringParameterVal[] = "val";
constexpr char kBoolParameterName[] = "boolParam";
constexpr char kBoolParameterVal[] = "true";
constexpr char kInvalidIntParameterName[] = "intParamInvalid";
constexpr char kInvalidIntParameterVal[] = "true";
constexpr char kIntListParameterName[] = "listParam";
constexpr char kIntListParameterVal[] = "[1,2,3]";
constexpr char kInvalidListParameterName[] = "listParamInvalid";
constexpr char kInvalidListParameterVal[] = "[";
constexpr char kInvalidListParameterName1[] = "listParamInvalid1";
constexpr char kInvalidListParameterVal1[] = "[1,2,3";

std::string FormatParameterName(absl::string_view name) {
  return absl::StrJoin({kParamPrefix, kTestEnvName, name}, "-");
}

absl::flat_hash_map<std::string, std::string> CreateParameterMap() {
  absl::flat_hash_map<std::string, std::string> parameter_map;
  parameter_map[FormatParameterName(kStringParameterName)] =
      kStringParameterVal;
  parameter_map[FormatParameterName(kBoolParameterName)] = kBoolParameterVal;
  parameter_map[FormatParameterName(kInvalidIntParameterName)] =
      kInvalidIntParameterVal;
  parameter_map[FormatParameterName(kIntListParameterName)] =
      kIntListParameterVal;
  parameter_map[FormatParameterName(kInvalidListParameterName)] =
      kInvalidListParameterVal;
  parameter_map[FormatParameterName(kInvalidListParameterName1)] =
      kInvalidListParameterVal1;
  return parameter_map;
}
}  // namespace

class ParameterClientTest : public testing::Test {
 public:
  ExecutionResultOr<GetParameterResponse> CaptureGetParameterSync(
      GetParameterRequest request);

 protected:
  ParameterClientTest()
      : mock_cpio_parameter_client_(
            std::make_shared<scp::cpio::MockParameterClient>()),
        parameter_client_(mock_cpio_parameter_client_, kTestEnvName),
        parameter_map_(CreateParameterMap()) {}

  std::shared_ptr<scp::cpio::MockParameterClient> mock_cpio_parameter_client_;
  ParameterClient parameter_client_;
  GetParameterRequest captured_request_;
  absl::flat_hash_map<std::string, std::string> parameter_map_;
};

ExecutionResultOr<GetParameterResponse>
ParameterClientTest::CaptureGetParameterSync(GetParameterRequest request) {
  captured_request_ = request;
  GetParameterResponse response;
  absl::string_view param_name = request.parameter_name();
  if (auto it = parameter_map_.find(param_name); it != parameter_map_.end()) {
    response.set_parameter_value(it->second);
    return response;
  } else {
    return scp::core::FailureExecutionResult(1);
  }
}

TEST_F(ParameterClientTest, StartStop) {
  EXPECT_CALL(*mock_cpio_parameter_client_, Init)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_parameter_client_, Run)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_cpio_parameter_client_, Stop)
      .WillOnce(Return(SuccessExecutionResult()));

  EXPECT_TRUE(parameter_client_.Init().Successful());
  EXPECT_TRUE(parameter_client_.Run().Successful());
  EXPECT_TRUE(parameter_client_.Stop().Successful());
}

TEST_F(ParameterClientTest, GetStringIsSuccessful) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  GetParameterRequest expected;
  expected.set_parameter_name(FormatParameterName(kStringParameterName));
  std::string out;

  EXPECT_SUCCESS(parameter_client_.GetString(kStringParameterName, out));

  EXPECT_EQ(out, kStringParameterVal);
  EXPECT_THAT(captured_request_, EqualsProto(expected));
}

TEST_F(ParameterClientTest, GetBoolIsSuccessful) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  GetParameterRequest expected;
  expected.set_parameter_name(FormatParameterName(kBoolParameterName));
  bool out;

  EXPECT_SUCCESS(parameter_client_.GetBool(kBoolParameterName, out));

  EXPECT_EQ(out, true);
  EXPECT_THAT(captured_request_, EqualsProto(expected));
}

TEST_F(ParameterClientTest, GetStringCpioReturnsError) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  std::string out;

  ExecutionResult result = parameter_client_.GetString("error", out);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(1)));
}

TEST_F(ParameterClientTest, GetBoolValueMismatch) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  int32_t out;

  ExecutionResult result =
      parameter_client_.GetInt32(kInvalidIntParameterName, out);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(CONVERTER_PARSE_ERROR)));
}

TEST_F(ParameterClientTest, GetIntListIsSuccessful) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  GetParameterRequest expected;
  expected.set_parameter_name(FormatParameterName(kIntListParameterName));
  std::list<int32_t> out;
  std::list<int32_t> expected_int32_t_list({1, 2, 3});

  EXPECT_SUCCESS(parameter_client_.GetInt32List(kIntListParameterName, out));

  EXPECT_EQ(out, expected_int32_t_list);
  EXPECT_THAT(captured_request_, EqualsProto(expected));
}

TEST_F(ParameterClientTest, GetInvalidListError) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  std::list<std::string> out;

  ExecutionResult result =
      parameter_client_.GetStringList(kInvalidListParameterName, out);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          PARAMETER_CLIENT_INVALID_LIST_PARAMETER_ERROR)));
}

TEST_F(ParameterClientTest, GetInvalidListMissingClosingBracketError) {
  EXPECT_CALL(*mock_cpio_parameter_client_, GetParameterSync)
      .WillOnce(Invoke(this, &ParameterClientTest::CaptureGetParameterSync));
  std::list<std::string> out;

  ExecutionResult result =
      parameter_client_.GetStringList(kInvalidListParameterName1, out);

  EXPECT_THAT(result, ResultIs(FailureExecutionResult(
                          PARAMETER_CLIENT_INVALID_LIST_PARAMETER_ERROR)));
}

}  // namespace google::confidential_match::lookup_server
