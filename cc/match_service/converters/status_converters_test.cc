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

#include "cc/match_service/converters/status_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(StatusConvertersTest, ToBackendConvertsUnspecified) {
  api::v1::Status in = api::v1::STATUS_UNSPECIFIED;
  backend::Status out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::STATUS_UNSPECIFIED);
}

TEST(StatusConvertersTest, ToBackendConvertsSuccessMatched) {
  api::v1::Status in = api::v1::STATUS_SUCCESS_MATCHED;
  backend::Status out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::STATUS_SUCCESS_MATCHED);
}

TEST(StatusConvertersTest, ToBackendConvertsSuccessUnmatched) {
  api::v1::Status in = api::v1::STATUS_SUCCESS_UNMATCHED;
  backend::Status out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::STATUS_SUCCESS_UNMATCHED);
}

TEST(StatusConvertersTest, ToBackendConvertsFailed) {
  api::v1::Status in = api::v1::STATUS_FAILED;
  backend::Status out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::STATUS_FAILED);
}

TEST(StatusConvertersTest, ToBackendFailsOnInvalidInput) {
  api::v1::Status in = static_cast<api::v1::Status>(-1);
  backend::Status out;
  EXPECT_THAT(ToBackend(in, out), StatusIs(StatusCode::kInvalidArgument));
}

TEST(StatusConvertersTest, ToApiConvertsUnspecified) {
  backend::Status in = backend::STATUS_UNSPECIFIED;
  api::v1::Status out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::STATUS_UNSPECIFIED);
}

TEST(StatusConvertersTest, ToApiConvertsSuccessMatched) {
  backend::Status in = backend::STATUS_SUCCESS_MATCHED;
  api::v1::Status out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::STATUS_SUCCESS_MATCHED);
}

TEST(StatusConvertersTest, ToApiConvertsSuccessUnmatched) {
  backend::Status in = backend::STATUS_SUCCESS_UNMATCHED;
  api::v1::Status out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(StatusConvertersTest, ToApiConvertsFailed) {
  backend::Status in = backend::STATUS_FAILED;
  api::v1::Status out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::STATUS_FAILED);
}

TEST(StatusConvertersTest, ToApiFailsOnInvalidInput) {
  backend::Status in = static_cast<backend::Status>(-1);
  api::v1::Status out;
  EXPECT_THAT(ToApi(in, out), StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
