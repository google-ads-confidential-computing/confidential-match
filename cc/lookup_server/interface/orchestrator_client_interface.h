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

#ifndef CC_LOOKUP_SERVER_INTERFACE_ORCHESTRATOR_CLIENT_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_ORCHESTRATOR_CLIENT_INTERFACE_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Represents a request to get the data export from Orchestrator. */
struct GetDataExportInfoRequest {
  /// The cluster group id for this server's cluster.
  std::string cluster_group_id;
  /// The id for the cluster that this server is in.
  std::string cluster_id;
};

/** @brief Represents a response from getting an Orchestrator data export. */
struct GetDataExportInfoResponse {
  /// Contains information about the latest export batch for this server.
  std::shared_ptr<proto_backend::DataExportInfo> data_export_info;
};

/** @brief Interface for client that communicates with the Orchestrator. */
class OrchestratorClientInterface : public scp::core::ServiceInterface {
 public:
  virtual ~OrchestratorClientInterface() = default;

  /**
   * @brief Gets the latest data export info for the instance synchronously.
   *
   * @param request the request containing information about the instance
   * @return a response object containing information about the latest
   * export batch available for this server or an error on failure
   */
  virtual scp::core::ExecutionResultOr<GetDataExportInfoResponse>
  GetDataExportInfo(const GetDataExportInfoRequest& request) noexcept = 0;

  /**
   * @brief Gets the latest data export info for the instance asynchronously.
   *
   * @param request the AsyncContext containing information about the instance
   * in the request, and provides a response object containing information
   * about the latest export batch
   * @return a result indicating whether the job was scheduled successfully
   */
  virtual scp::core::ExecutionResult GetDataExportInfo(
      scp::core::AsyncContext<GetDataExportInfoRequest,
                              GetDataExportInfoResponse>& context) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_ORCHESTRATOR_CLIENT_INTERFACE_H_
