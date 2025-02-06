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

#ifndef CC_LOOKUP_SERVER_PARAMETER_CLIENT_MOCK_MOCK_PARAMETER_CLIENT_H_
#define CC_LOOKUP_SERVER_PARAMETER_CLIENT_MOCK_MOCK_PARAMETER_CLIENT_H_

#include <list>
#include <string>

#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"

#include "cc/lookup_server/interface/parameter_client_interface.h"

namespace google::confidential_match::lookup_server {

class MockParameterClient : public ParameterClientInterface {
 public:
  MOCK_METHOD(scp::core::ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Stop, (), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::string>, GetString,
              (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::int32_t>, GetInt32,
              (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::size_t>, GetSizeT,
              (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::bool>, GetBool,
              (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::list<std::string>>,
              GetStringList, (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::list<int32_t>>, GetInt32List,
              (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::list<size_t>>, GetSizeTList,
              (std::string_view), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<std::list<bool>>, GetBoolList,
              (std::string_view), (noexcept, override));
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_PARAMETER_CLIENT_MOCK_MOCK_PARAMETER_CLIENT_H_
