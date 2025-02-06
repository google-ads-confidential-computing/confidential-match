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

#include "cc/lookup_server/converters/src/string_converter.h"

#include "absl/strings/str_format.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gtest/gtest.h"

namespace google::confidential_match::lookup_server::test {

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;

TEST(LookupServerUtilsTest, StringToTypeBoolSuccess) {
  bool out_bool;

  EXPECT_SUCCESS(StringToType("true", out_bool));

  EXPECT_EQ(out_bool, true);
}

TEST(LookupServerUtilsTest, StringToTypeSizeTSuccess) {
  size_t out_size_t;
  size_t expected_size_t = 5000;

  EXPECT_SUCCESS(StringToType("5000", out_size_t));

  EXPECT_EQ(out_size_t, expected_size_t);
}

TEST(LookupServerUtilsTest, StringToTypeInt32Success) {
  int32_t out_int32_t;
  int32_t expected_int32_t = 6000;

  EXPECT_SUCCESS(StringToType("6000", out_int32_t));

  EXPECT_EQ(out_int32_t, expected_int32_t);
}

TEST(LookupServerUtilsTest, ConvertValToStringListSuccess) {
  std::list<std::string> out_string_list;
  std::list<std::string> expect_string_list({"1", "2"});
  char string_list_val[] = "1,2";

  EXPECT_SUCCESS(ConvertValToList(string_list_val, out_string_list));

  EXPECT_EQ(out_string_list, expect_string_list);
}

TEST(LookupServerUtilsTest, ConvertEmptyStringListSuccess) {
  std::list<std::string> out_string_list;
  std::list<std::string> expect_string_list({});
  char string_list_val[] = "";

  EXPECT_SUCCESS(ConvertValToList(string_list_val, out_string_list));

  EXPECT_EQ(out_string_list, expect_string_list);
}

TEST(LookupServerUtilsTest, ConvertEmptyIntListSuccess) {
  std::list<uint32_t> out_int_list;
  std::list<uint32_t> expect_int_list({});
  char list_val[] = "";

  EXPECT_SUCCESS(ConvertValToList(list_val, out_int_list));

  EXPECT_EQ(out_int_list, expect_int_list);
}

TEST(LookupServerUtilsTest, ConvertValToInt32ListSuccess) {
  std::list<int32_t> out_int32_t_list;
  std::list<int32_t> expected_int32_t_list({1, 2});
  char int32_t_list[] = "1,2";

  EXPECT_SUCCESS(ConvertValToList(int32_t_list, out_int32_t_list));

  EXPECT_EQ(out_int32_t_list, expected_int32_t_list);
}

TEST(LookupServerUtilsTest, ConvertValToSizeTListSuccess) {
  std::list<size_t> out_size_t_list;
  std::list<size_t> expected_size_t_list({3, 4});
  char size_t_list[] = "3,4";

  EXPECT_SUCCESS(ConvertValToList(size_t_list, out_size_t_list));

  EXPECT_EQ(out_size_t_list, expected_size_t_list);
}

TEST(LookupServerUtilsTest, ConvertValToBoolListSuccess) {
  std::list<bool> out_bool_list;
  std::list<bool> expected_bool_list({true, false});
  char bool_list[] = "true,false";

  EXPECT_SUCCESS(ConvertValToList(bool_list, out_bool_list));

  EXPECT_EQ(out_bool_list, expected_bool_list);
}

TEST(LookupServerUtilsTest, StringToTypeEmptyStringFail) {
  int32_t out_int32;

  EXPECT_THAT(StringToType("", out_int32),
              ResultIs(FailureExecutionResult(CONVERTER_PARSE_ERROR)));
}

TEST(LookupServerUtilsTest, StringToTypeTypeMismatchFail) {
  char val[] = "hello";
  int32_t out_val;

  EXPECT_THAT(StringToType(val, out_val),
              ResultIs(FailureExecutionResult(CONVERTER_PARSE_ERROR)));
}

TEST(EnvConfigProviderTest, ConvertValToListMixedTypesFail) {
  char val_list[] = "a,true,c";
  std::list<bool> out_val;

  EXPECT_THAT(ConvertValToList(val_list, out_val),
              ResultIs(FailureExecutionResult(CONVERTER_PARSE_ERROR)));
}

}  // namespace google::confidential_match::lookup_server::test
