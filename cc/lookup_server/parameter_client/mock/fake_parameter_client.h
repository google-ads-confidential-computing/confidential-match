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

#ifndef CC_LOOKUP_SERVER_PARAMETER_CLIENT_MOCK_FAKE_PARAMETER_CLIENT_H_
#define CC_LOOKUP_SERVER_PARAMETER_CLIENT_MOCK_FAKE_PARAMETER_CLIENT_H_

#include <list>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/converters/src/string_converter.h"
#include "cc/lookup_server/interface/parameter_client_interface.h"

namespace google::confidential_match::lookup_server {

class FakeParameterClient : public ParameterClientInterface {
 public:
  explicit FakeParameterClient(
      absl::flat_hash_map<std::string, std::string> parameter_map)
      : parameter_map_(parameter_map) {}

  scp::core::ExecutionResult Init() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Run() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Stop() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult GetString(
      absl::string_view parameter_name,
      std::string& out_string) noexcept override {
    ASSIGN_OR_RETURN(out_string, GetParameterFromMap(parameter_name));
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult GetInt32(absl::string_view parameter_name,
                                      int32_t& out_int) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    return StringToType(val, out_int);
  }

  scp::core::ExecutionResult GetSizeT(absl::string_view parameter_name,
                                      size_t& out_sizet) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    return StringToType(val, out_sizet);
  }

  scp::core::ExecutionResult GetBool(absl::string_view parameter_name,
                                     bool& out_bool) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    return StringToType(val, out_bool);
  }

  scp::core::ExecutionResult GetStringList(
      absl::string_view parameter_name,
      std::list<std::string>& out_list) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    std::list<std::string> out;
    return ConvertValToList(val, out);
  }

  scp::core::ExecutionResult GetInt32List(
      absl::string_view parameter_name,
      std::list<int32_t>& out_list) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    std::list<int32_t> out;
    return ConvertValToList(val, out);
  }

  scp::core::ExecutionResult GetSizeTList(
      absl::string_view parameter_name,
      std::list<size_t>& out_list) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    return ConvertValToList(val, out_list);
  }

  scp::core::ExecutionResult GetBoolList(
      absl::string_view parameter_name,
      std::list<bool>& out_list) noexcept override {
    ASSIGN_OR_RETURN(std::string val, GetParameterFromMap(parameter_name));
    return ConvertValToList(val, out_list);
  }

 private:
  scp::core::ExecutionResultOr<std::string> GetParameterFromMap(
      absl::string_view name) noexcept {
    if (auto it = parameter_map_.find(name); it != parameter_map_.end()) {
      return it->second;
    } else {
      return scp::core::FailureExecutionResult(1);
    }
  }

  // Mapping of parameter_name -> value
  absl::flat_hash_map<std::string, std::string> parameter_map_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_PARAMETER_CLIENT_MOCK_FAKE_PARAMETER_CLIENT_H_
