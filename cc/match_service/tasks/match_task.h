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

#ifndef CC_MATCH_SERVICE_TASKS_MATCH_TASK_H_
#define CC_MATCH_SERVICE_TASKS_MATCH_TASK_H_

#include "cc/core/async/async_context.h"
#include "cc/match_service/tasks/match_task_interface.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// A task to perform a matching operation.
class MatchTask : public MatchTaskInterface {
 public:
  // Constructs a match task.
  //
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the MatchService object.
  explicit MatchTask(MatchTaskInterface* hashed_match_task,
                     MatchTaskInterface* kms_encrypted_match_task);

  // Performs the match operation.
  void Match(AsyncContext<backend::MatchRequest, backend::MatchResponse>
                 context) noexcept override;

 private:
  // Match task that handles hashed non-encrypted requests.
  MatchTaskInterface* hashed_match_task_;
  // Match task that handles kms encrypted requests.
  MatchTaskInterface* kms_encrypted_match_task_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_TASKS_MATCH_TASK_H_
