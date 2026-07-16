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

#include "cc/match_service/tasks/match_task.h"

#include "absl/status/status.h"

#include "cc/core/async/async_context.h"
#include "cc/core/logger/log.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::MatchKeyFormat;
using ::google::confidential_match::match_service::backend::MatchRequest;
using ::google::confidential_match::match_service::backend::MatchResponse;

}  // namespace

MatchTask::MatchTask(MatchTaskInterface* hashed_match_task,
                     MatchTaskInterface* kms_encrypted_match_task)
    : hashed_match_task_(hashed_match_task),
      kms_encrypted_match_task_(kms_encrypted_match_task) {}

void MatchTask::Match(
    AsyncContext<MatchRequest, MatchResponse> context) noexcept {
  switch (context.request->match_key_format()) {
    case MatchKeyFormat::MATCH_KEY_FORMAT_UNSPECIFIED:
      context.status = Status(Error::INVALID_MATCH_KEY_FORMAT,
                              "The match key format must be specified.");
      context.Finish();
      return;

    case MatchKeyFormat::MATCH_KEY_FORMAT_HASHED:
      hashed_match_task_->Match(context);
      return;

    case MatchKeyFormat::MATCH_KEY_FORMAT_HASHED_ENCRYPTED:
      kms_encrypted_match_task_->Match(context);
      return;

    default:
      context.status = Status(Error::INVALID_MATCH_KEY_FORMAT,
                              "Got an unrecognized match key format.");
      context.Finish();
      return;
  }
}

}  // namespace google::confidential_match::match_service
