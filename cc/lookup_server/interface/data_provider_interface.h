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

#ifndef CC_LOOKUP_SERVER_INTERFACE_DATA_PROVIDER_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_DATA_PROVIDER_INTERFACE_H_

#include <memory>
#include <string>

#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/location.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface to provide data from cloud storage. */
class DataProviderInterface : public scp::core::ServiceInterface {
 public:
  virtual ~DataProviderInterface() = default;

  /**
   * @brief Retrieves data from the provided location.
   *
   * Fetches data asynchronously and assembles it into its full contents
   * before returning to the caller.
   *
   * @param context the context supplying the data location and
   * a callback to process the data
   * @return an ExecutionResult indicating whether the fetch operation
   * was successfully scheduled
   */
  virtual scp::core::ExecutionResult Get(
      google::scp::core::AsyncContext<lookup_server::proto_backend::Location,
                                      std::string>
          context) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_DATA_PROVIDER_INTERFACE_H_
