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

#ifndef CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_MOCK_ORCHESTRATOR_CLIENT_H_
#define CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_MOCK_ORCHESTRATOR_CLIENT_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "cc/core/async/async_context.h"
#include "cc/match_service/orchestrator_client/orchestrator_client_interface.h"
#include "gmock/gmock.h"

namespace google::confidential_match::match_service {

class MockOrchestratorClient : public OrchestratorClientInterface {
 public:
  MOCK_METHOD(absl::Status, Init, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Run, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Stop, (), (noexcept, override));

  MOCK_METHOD(void, GetCurrentShardingScheme,
              ((AsyncContext<orchestrator::GetCurrentShardingSchemeRequest,
                             orchestrator::GetCurrentShardingSchemeResponse> &
                get_sharding_scheme_context)),
              (noexcept, override));
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_ORCHESTRATOR_CLIENT_MOCK_ORCHESTRATOR_CLIENT_H_
