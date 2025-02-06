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

#ifndef CC_LOOKUP_SERVER_INTERFACE_STATUS_PROVIDER_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_STATUS_PROVIDER_INTERFACE_H_

#include "protos/lookup_server/backend/service_status.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Interface used by services to provide their health status. */
class StatusProviderInterface {
 public:
  virtual ~StatusProviderInterface() = default;

  /**
   * @brief Provides the status of the service.
   */
  virtual lookup_server::proto_backend::ServiceStatus GetStatus() noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_STATUS_PROVIDER_INTERFACE_H_
