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

#include "cc/match_service/converters/key_type_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(KeyTypeConvertersTest, ToBackendConvertsUnspecified) {
  api::v1::KeyType in = api::v1::KEY_TYPE_UNSPECIFIED;
  backend::KeyType out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::KEY_TYPE_UNSPECIFIED);
}

TEST(KeyTypeConvertersTest, ToBackendConvertsXChacha20Poly1305) {
  api::v1::KeyType in = api::v1::KEY_TYPE_XCHACHA20_POLY1305;
  backend::KeyType out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::KEY_TYPE_XCHACHA20_POLY1305);
}

TEST(KeyTypeConvertersTest, ToBackendFailsOnInvalidInput) {
  api::v1::KeyType in = static_cast<api::v1::KeyType>(-1);
  backend::KeyType out;
  EXPECT_THAT(ToBackend(in, out), StatusIs(StatusCode::kInvalidArgument));
}

TEST(KeyTypeConvertersTest, ToApiConvertsUnspecified) {
  backend::KeyType in = backend::KEY_TYPE_UNSPECIFIED;
  api::v1::KeyType out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::KEY_TYPE_UNSPECIFIED);
}

TEST(KeyTypeConvertersTest, ToApiConvertsXChacha20Poly1305) {
  backend::KeyType in = backend::KEY_TYPE_XCHACHA20_POLY1305;
  api::v1::KeyType out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::KEY_TYPE_XCHACHA20_POLY1305);
}

TEST(KeyTypeConvertersTest, ToApiFailsOnInvalidInput) {
  backend::KeyType in = static_cast<backend::KeyType>(-1);
  api::v1::KeyType out;
  EXPECT_THAT(ToApi(in, out), StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
