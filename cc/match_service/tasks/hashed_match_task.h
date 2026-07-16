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

#ifndef CC_MATCH_SERVICE_TASKS_HASHED_MATCH_TASK_H_
#define CC_MATCH_SERVICE_TASKS_HASHED_MATCH_TASK_H_

#include "cc/core/async/async_context.h"
#include "cc/core/hash/hasher_interface.h"
#include "cc/match_service/lookup_service_client/lookup_service_client_interface.h"
#include "cc/match_service/tasks/match_task_interface.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// A task to perform a matching operation for hashed data.
class HashedMatchTask : public MatchTaskInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the HashedMatchTask object.
  explicit HashedMatchTask(LookupServiceClientInterface* lookup_service_client,
                           HasherInterface* sha256_hasher)
      : lookup_service_client_(lookup_service_client),
        sha256_hasher_(sha256_hasher) {}

  // Performs the match operation.
  void Match(AsyncContext<backend::MatchRequest, backend::MatchResponse>
                 context) noexcept override;

 private:
  // Handles the response from a Lookup Service match operation.
  void OnLookupCallback(
      AsyncContext<backend::MatchRequest, backend::MatchResponse> match_context,
      AsyncContext<backend::LookupServiceRequest,
                   backend::LookupServiceResponse>& lookup_context) noexcept;

  // The client used to call Lookup Service.
  LookupServiceClientInterface* lookup_service_client_;
  // The hasher used to generate SHA-256 hashes.
  HasherInterface* sha256_hasher_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_TASKS_HASHED_MATCH_TASK_H_
