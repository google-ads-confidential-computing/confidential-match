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

#ifndef CC_LOOKUP_SERVER_CONVERTERS_SRC_STRING_CONVERTER_H_
#define CC_LOOKUP_SERVER_CONVERTERS_SRC_STRING_CONVERTER_H_

#include <list>
#include <sstream>
#include <string>
#include "cc/core/utils/src/string_util.h"
#include "cc/lookup_server/converters/src/error_codes.h"
#include "cc/public/core/interface/execution_result.h"

namespace google::confidential_match::lookup_server {

static constexpr char kCommaDelimiter[] = ",";

/**
 * @brief Convert a string to the desired, supported type.
 *
 * @tparam T the desired output type.
 * @param str The input string.
 * @param out The output value.
 * @return ExecutionResult failure if the conversion failed.
 */
template <typename T>
static scp::core::ExecutionResult StringToType(const std::string& str, T& out) {
  std::stringstream string_stream;
  if (std::is_same<T, bool>::value) {
    string_stream << std::boolalpha << str;
    string_stream >> std::boolalpha >> out;
  } else {
    string_stream << str;
    string_stream >> out;
  }

  // Empty is only allowed for string.
  if (std::is_same<T, std::string>::value && str.empty()) {
    string_stream.clear();
  }

  if (string_stream.fail()) {
    return scp::core::FailureExecutionResult(CONVERTER_PARSE_ERROR);
  }

  return scp::core::SuccessExecutionResult();
}

/**
 * @brief Convert a given string to a list of an expected type
 * The input string is assumed to be comma-delimited.
 *
 * For example, the string "1,2,3,4" would be turned into ["1","2","3","4"] of
 * string. An empty string is accepted as an empty list.
 *
 * @tparam T The type to convert the list items.
 * @param value The string to convert
 * @return ExecutionResult Failure if the list can't be parsed
 */
template <typename T>
static scp::core::ExecutionResult ConvertValToList(std::string value,
                                                   std::list<T>& out) noexcept {
  // Return empty list if input is empty string
  if (value.empty()) {
    return scp::core::SuccessExecutionResult();
  }

  std::list<std::string> parts;
  const std::string delimiter(kCommaDelimiter);
  scp::core::utils::SplitStringByDelimiter(value, delimiter, parts);

  for (const auto& part : parts) {
    T out_part;
    auto result = StringToType(part, out_part);
    if (!result.Successful()) {
      return result;
    }
    out.push_back(out_part);
  }
  return scp::core::SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CONVERTERS_SRC_STRING_CONVERTER_H_
