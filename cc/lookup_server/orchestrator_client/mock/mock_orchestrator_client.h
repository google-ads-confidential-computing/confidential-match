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

#ifndef CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_MOCK_MOCK_ORCHESTRATOR_CLIENT_H_
#define CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_MOCK_MOCK_ORCHESTRATOR_CLIENT_H_

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"

#include "cc/lookup_server/interface/orchestrator_client_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

class MockOrchestratorClient : public OrchestratorClientInterface {
 public:
  MOCK_METHOD(scp::core::ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Stop, (), (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResultOr<GetDataExportInfoResponse>,
              GetDataExportInfo, ((const GetDataExportInfoRequest&)),
              (noexcept, override));

  MOCK_METHOD(scp::core::ExecutionResult, GetDataExportInfo,
              ((scp::core::AsyncContext<GetDataExportInfoRequest,
                                        GetDataExportInfoResponse>&)),
              (noexcept, override));
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_ORCHESTRATOR_CLIENT_MOCK_MOCK_ORCHESTRATOR_CLIENT_H_
