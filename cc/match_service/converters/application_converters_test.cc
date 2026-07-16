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

#include "cc/match_service/converters/application_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(ApplicationConvertersTest, ToBackendConvertsUnspecified) {
  api::v1::Application in = api::v1::APPLICATION_UNSPECIFIED;
  backend::Application out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::APPLICATION_UNSPECIFIED);
}

TEST(ApplicationConvertersTest, ToBackendConvertsEcl) {
  api::v1::Application in = api::v1::APPLICATION_ECL;
  backend::Application out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::APPLICATION_ECL);
}

TEST(ApplicationConvertersTest, ToBackendConvertsOnlineEc) {
  api::v1::Application in = api::v1::APPLICATION_ONLINE_EC;
  backend::Application out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::APPLICATION_ONLINE_EC);
}

TEST(ApplicationConvertersTest, ToBackendConvertsVoyager) {
  api::v1::Application in = api::v1::APPLICATION_VOYAGER;
  backend::Application out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, backend::APPLICATION_VOYAGER);
}

TEST(ApplicationConvertersTest, ToApiConvertsUnspecified) {
  backend::Application in = backend::APPLICATION_UNSPECIFIED;
  api::v1::Application out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::APPLICATION_UNSPECIFIED);
}

TEST(ApplicationConvertersTest, ToApiConvertsEcl) {
  backend::Application in = backend::APPLICATION_ECL;
  api::v1::Application out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::APPLICATION_ECL);
}

TEST(ApplicationConvertersTest, ToApiConvertsOnlineEc) {
  backend::Application in = backend::APPLICATION_ONLINE_EC;
  api::v1::Application out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::APPLICATION_ONLINE_EC);
}

TEST(ApplicationConvertersTest, ToApiConvertsVoyager) {
  backend::Application in = backend::APPLICATION_VOYAGER;
  api::v1::Application out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out, api::v1::APPLICATION_VOYAGER);
}

}  // namespace google::confidential_match::match_service
