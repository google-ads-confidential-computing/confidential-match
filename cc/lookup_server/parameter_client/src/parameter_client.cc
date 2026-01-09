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

#include <future>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"
#include "cc/public/cpio/proto/parameter_service/v1/parameter_service.pb.h"

#include "cc/lookup_server/converters/src/string_converter.h"
#include "cc/lookup_server/parameter_client/src/error_codes.h"
#include "cc/public/cpio/interface/type_def.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::parameter_service::v1::GetParameterRequest;
using ::google::cmrt::sdk::parameter_service::v1::GetParameterResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::cpio::Callback;

constexpr absl::string_view kComponentName = "ParameterClient";
constexpr absl::string_view kParameterFormatString = "CFM-%s-%s";

ExecutionResultOr<std::string> ParseListString(
    absl::string_view list_parameter) {
  if (list_parameter.size() < 2 || list_parameter.front() != '[' ||
      list_parameter.back() != ']') {
    return FailureExecutionResult(
        PARAMETER_CLIENT_INVALID_LIST_PARAMETER_ERROR);
  }
  std::string result(list_parameter);
  result.pop_back();
  result.erase(result.begin());
  return result;
}

}  // namespace

ExecutionResult ParameterClient::Init() noexcept {
  return cpio_parameter_client_->Init();
}

ExecutionResult ParameterClient::Run() noexcept {
  return cpio_parameter_client_->Run();
}

ExecutionResult ParameterClient::Stop() noexcept {
  return cpio_parameter_client_->Stop();
}

scp::core::ExecutionResult ParameterClient::GetString(
    absl::string_view parameter_name, std::string& out_string) noexcept {
  ASSIGN_OR_RETURN(out_string, GetParameter(parameter_name));
  return scp::core::SuccessExecutionResult();
}

scp::core::ExecutionResult ParameterClient::GetInt32(
    absl::string_view parameter_name, int32_t& out_int) noexcept {
  ASSIGN_OR_RETURN(std::string val, GetParameter(parameter_name));
  return StringToType(val, out_int);
}

scp::core::ExecutionResult ParameterClient::GetSizeT(
    absl::string_view parameter_name, size_t& out_sizet) noexcept {
  ASSIGN_OR_RETURN(std::string val, GetParameter(parameter_name));
  return StringToType(val, out_sizet);
}

scp::core::ExecutionResult ParameterClient::GetBool(
    absl::string_view parameter_name, bool& out_bool) noexcept {
  ASSIGN_OR_RETURN(std::string val, GetParameter(parameter_name));
  return StringToType(val, out_bool);
}

scp::core::ExecutionResult ParameterClient::GetStringList(
    absl::string_view parameter_name,
    std::list<std::string>& out_list) noexcept {
  ASSIGN_OR_RETURN(std::string parameter_value, GetParameter(parameter_name));
  ASSIGN_OR_RETURN(parameter_value, ParseListString(parameter_value));
  return ConvertValToList(parameter_value, out_list);
}

scp::core::ExecutionResult ParameterClient::GetInt32List(
    absl::string_view parameter_name, std::list<int32_t>& out_list) noexcept {
  ASSIGN_OR_RETURN(std::string parameter_value, GetParameter(parameter_name));
  ASSIGN_OR_RETURN(parameter_value, ParseListString(parameter_value));
  return ConvertValToList(parameter_value, out_list);
}

scp::core::ExecutionResult ParameterClient::GetSizeTList(
    absl::string_view parameter_name, std::list<size_t>& out_list) noexcept {
  ASSIGN_OR_RETURN(std::string parameter_value, GetParameter(parameter_name));
  ASSIGN_OR_RETURN(parameter_value, ParseListString(parameter_value));
  return ConvertValToList(parameter_value, out_list);
}

scp::core::ExecutionResult ParameterClient::GetBoolList(
    absl::string_view parameter_name, std::list<bool>& out_list) noexcept {
  ASSIGN_OR_RETURN(std::string parameter_value, GetParameter(parameter_name));
  ASSIGN_OR_RETURN(parameter_value, ParseListString(parameter_value));
  return ConvertValToList(parameter_value, out_list);
}

ExecutionResultOr<std::string> ParameterClient::GetParameter(
    absl::string_view name) noexcept {
  std::string formatted_name =
      absl::StrFormat(kParameterFormatString, environment_name_, name);
  GetParameterRequest get_parameter_request;
  get_parameter_request.set_parameter_name(formatted_name);
  ASSIGN_OR_RETURN(
      GetParameterResponse parameter_response,
      cpio_parameter_client_->GetParameterSync(get_parameter_request));
  return parameter_response.parameter_value();
}

}  // namespace google::confidential_match::lookup_server
