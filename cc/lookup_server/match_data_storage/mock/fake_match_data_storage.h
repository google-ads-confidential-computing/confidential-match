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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_MOCK_FAKE_MATCH_DATA_STORAGE_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_MOCK_FAKE_MATCH_DATA_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "cc/core/interface/type_def.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"

namespace google::confidential_match::lookup_server {

class FakeMatchDataStorage : public MatchDataStorageInterface {
 public:
  scp::core::ExecutionResult Init() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Run() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Stop() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResultOr<std::vector<proto_backend::MatchDataRow>> Get(
      absl::string_view key) noexcept override {
    return std::vector<proto_backend::MatchDataRow>();
  }

  scp::core::ExecutionResult StartUpdate(
      const proto_backend::DataExportInfo& data_export_info) noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult FinalizeUpdate() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult CancelUpdate() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Insert(
      const proto_backend::MatchDataRow& row) noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Replace(
      absl::string_view key,
      absl::Span<const proto_backend::MatchDataRow> rows) noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Remove(
      const proto_backend::MatchDataRow& row) noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  bool IsValidRequestScheme(
      const proto_backend::ShardingScheme& sharding_scheme) noexcept override {
    return true;
  }

  std::string GetDatasetId() noexcept override { return ""; }

  std::string GetPendingDatasetId() noexcept override { return ""; }

  lookup_server::proto_backend::ServiceStatus GetStatus() noexcept override {
    return lookup_server::proto_backend::ServiceStatus::OK;
  }
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_MATCH_DATA_STORAGE_MOCK_FAKE_MATCH_DATA_STORAGE_H_
