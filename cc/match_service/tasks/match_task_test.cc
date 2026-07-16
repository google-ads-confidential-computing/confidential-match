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

#include <memory>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/core/async/async_context.h"
#include "cc/core/logger/log.h"
#include "cc/core/logger/logger_interface.h"
#include "cc/match_service/tasks/mock_match_task.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::google::confidential_match::match_service::backend::Application;
using ::google::confidential_match::match_service::backend::KeyType;
using ::google::confidential_match::match_service::backend::MatchKeyEncoding;
using ::google::confidential_match::match_service::backend::MatchKeyFormat;
using ::google::confidential_match::match_service::backend::MatchRequest;
using ::google::confidential_match::match_service::backend::MatchResponse;

void MockMatchSuccessfulResponse(
    AsyncContext<MatchRequest, MatchResponse> context) {
  context.response = std::make_shared<MatchResponse>();
  context.Finish();
}

class MatchTaskTest : public ::testing::Test {
 protected:
  MatchTaskTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        hashed_match_task_(std::make_unique<MockMatchTask>()),
        kms_encrypted_match_task_(std::make_unique<MockMatchTask>()),
        match_task_(hashed_match_task_.get(), kms_encrypted_match_task_.get()) {
  }

  std::shared_ptr<LoggerInterface> logger_;
  std::unique_ptr<MockMatchTask> hashed_match_task_;
  std::unique_ptr<MockMatchTask> kms_encrypted_match_task_;
  MatchTask match_task_;
};

TEST_F(MatchTaskTest, MatchWithUnsetKeyFormatReturnsError) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  context.request->set_application(Application::APPLICATION_ECL);
  context.request->set_key_encoding(
      MatchKeyEncoding::MATCH_KEY_ENCODING_BASE64);

  context.callback = [](AsyncContext<MatchRequest, MatchResponse>& context) {
    EXPECT_EQ(context.status.code(), absl::StatusCode::kInvalidArgument);
  };
  EXPECT_CALL(*hashed_match_task_, Match).Times(0);

  match_task_.Match(context);
}

TEST_F(MatchTaskTest, MatchWithHashedRequestUsesHashedTask) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  context.request->set_application(Application::APPLICATION_ECL);
  context.request->set_match_key_format(
      MatchKeyFormat::MATCH_KEY_FORMAT_HASHED);
  context.request->set_key_encoding(
      MatchKeyEncoding::MATCH_KEY_ENCODING_BASE64);

  context.callback = [](AsyncContext<MatchRequest, MatchResponse>& context) {
    EXPECT_THAT(context.status, IsOk());
  };
  EXPECT_CALL(*hashed_match_task_, Match).WillOnce(MockMatchSuccessfulResponse);
  EXPECT_CALL(*kms_encrypted_match_task_, Match).Times(0);

  match_task_.Match(context);
}

TEST_F(MatchTaskTest, MatchWithEncryptedRequestUsesEncryptedTask) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  context.request->set_application(Application::APPLICATION_ECL);
  context.request->set_match_key_format(
      MatchKeyFormat::MATCH_KEY_FORMAT_HASHED_ENCRYPTED);
  context.request->set_key_encoding(
      MatchKeyEncoding::MATCH_KEY_ENCODING_BASE64);

  context.callback = [](AsyncContext<MatchRequest, MatchResponse>& context) {
    EXPECT_THAT(context.status, IsOk());
  };
  EXPECT_CALL(*kms_encrypted_match_task_, Match)
      .WillOnce(MockMatchSuccessfulResponse);
  EXPECT_CALL(*hashed_match_task_, Match).Times(0);

  match_task_.Match(context);
}

}  // namespace
}  // namespace google::confidential_match::match_service
