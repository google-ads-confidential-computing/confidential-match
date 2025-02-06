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

#ifndef CC_LOOKUP_SERVER_INTERFACE_MATCH_DATA_LOADER_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_MATCH_DATA_LOADER_INTERFACE_H_

#include "absl/strings/string_view.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Interface for service that loads match data from a data provider into
 * a data storage component.
 */
class MatchDataLoaderInterface : public scp::core::ServiceInterface {
 public:
  virtual ~MatchDataLoaderInterface() = default;

  /**
   * @brief Loads data from a provider into a storage component.
   *
   * @param data_export_info the data export to be loaded into storage
   * @param encrypted_dek the base-64 encoded encrypted DEK for the match data
   * @return an ExecutionResult indicating whether the operation was successful
   */
  virtual scp::core::ExecutionResult Load(
      const proto_backend::DataExportInfo& data_export_info,
      absl::string_view encrypted_dek) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_MATCH_DATA_LOADER_INTERFACE_H_
