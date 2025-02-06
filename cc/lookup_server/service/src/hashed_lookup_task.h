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

#ifndef CC_LOOKUP_SERVER_SERVICE_SRC_HASHED_LOOKUP_TASK_H_
#define CC_LOOKUP_SERVER_SERVICE_SRC_HASHED_LOOKUP_TASK_H_

#include <memory>

#include "cc/core/interface/async_context.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/match_data_storage_interface.h"
#include "protos/lookup_server/api/lookup.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Handles a lookup request with hashed unencrypted keys.
 */
class HashedLookupTask {
 public:
  explicit HashedLookupTask(
      std::shared_ptr<MatchDataStorageInterface> match_data_storage)
      : match_data_storage_(match_data_storage) {}

  /**
   * @brief Handles a lookup request, producing a lookup response.
   *
   * @param context the AsyncContext containing the LookupRequest object and
   * the resulting LookupResponse if successful, or a failed result
   */
  void HandleRequest(scp::core::AsyncContext<proto_api::LookupRequest,
                                             proto_api::LookupResponse>
                         context) noexcept;

 private:
  /**
   * @brief Handles a lookup request synchronously, producing a lookup response.
   *
   * @param request the proto containing information about the lookup request
   * @param response the proto to write the response to
   * @return whether or not the operation was successful
   */
  scp::core::ExecutionResult HandleRequest(
      const proto_api::LookupRequest& request,
      proto_api::LookupResponse& response) noexcept;

  // A storage service containing match data.
  std::shared_ptr<MatchDataStorageInterface> match_data_storage_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_SERVICE_SRC_HASHED_LOOKUP_TASK_H_
