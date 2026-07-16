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

#include "cc/match_service/converters/error_reason_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl::StatusCode;
using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(ErrorReasonConvertersTest, ToBackendConvertsUnknown) {
  api::v1::ErrorReason in = api::v1::ERROR_REASON_UNKNOWN;
  backend::ErrorReason out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::ERROR_REASON_UNKNOWN);
}

TEST(ErrorReasonConvertersTest, ToBackendConvertsInternalError) {
  api::v1::ErrorReason in = api::v1::ERROR_REASON_INTERNAL_ERROR;
  backend::ErrorReason out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::ERROR_REASON_INTERNAL_ERROR);
}

TEST(ErrorReasonConvertersTest, ToBackendConvertsDecryptionError) {
  api::v1::ErrorReason in = api::v1::ERROR_REASON_DECRYPTION_ERROR;
  backend::ErrorReason out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::ERROR_REASON_DECRYPTION_ERROR);
}

TEST(ErrorReasonConvertersTest, ToBackendConvertsDekDecryptionError) {
  api::v1::ErrorReason in = api::v1::ERROR_REASON_DEK_DECRYPTION_ERROR;
  backend::ErrorReason out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::ERROR_REASON_DEK_DECRYPTION_ERROR);
}

TEST(ErrorReasonConvertersTest, ToBackendConvertsInvalidDek) {
  api::v1::ErrorReason in = api::v1::ERROR_REASON_INVALID_DEK;
  backend::ErrorReason out;
  EXPECT_THAT(ToBackend(in, out), IsOk());
  EXPECT_EQ(out, backend::ERROR_REASON_INVALID_DEK);
}

TEST(ErrorReasonConvertersTest, ToBackendFailsOnInvalidInput) {
  api::v1::ErrorReason in = static_cast<api::v1::ErrorReason>(-1);
  backend::ErrorReason out;
  EXPECT_THAT(ToBackend(in, out), StatusIs(StatusCode::kInvalidArgument));
}

TEST(ErrorReasonConvertersTest, ToApiConvertsUnknown) {
  backend::ErrorReason in = backend::ERROR_REASON_UNKNOWN;
  api::v1::ErrorReason out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::ERROR_REASON_UNKNOWN);
}

TEST(ErrorReasonConvertersTest, ToApiConvertsInternalError) {
  backend::ErrorReason in = backend::ERROR_REASON_INTERNAL_ERROR;
  api::v1::ErrorReason out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::ERROR_REASON_INTERNAL_ERROR);
}

TEST(ErrorReasonConvertersTest, ToApiConvertsDecryptionError) {
  backend::ErrorReason in = backend::ERROR_REASON_DECRYPTION_ERROR;
  api::v1::ErrorReason out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::ERROR_REASON_DECRYPTION_ERROR);
}

TEST(ErrorReasonConvertersTest, ToApiConvertsDekDecryptionError) {
  backend::ErrorReason in = backend::ERROR_REASON_DEK_DECRYPTION_ERROR;
  api::v1::ErrorReason out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::ERROR_REASON_DEK_DECRYPTION_ERROR);
}

TEST(ErrorReasonConvertersTest, ToApiConvertsInvalidDek) {
  backend::ErrorReason in = backend::ERROR_REASON_INVALID_DEK;
  api::v1::ErrorReason out;
  EXPECT_THAT(ToApi(in, out), IsOk());
  EXPECT_EQ(out, api::v1::ERROR_REASON_INVALID_DEK);
}

TEST(ErrorReasonConvertersTest, ToApiFailsOnInvalidInput) {
  backend::ErrorReason in = static_cast<backend::ErrorReason>(-1);
  api::v1::ErrorReason out;
  EXPECT_THAT(ToApi(in, out), StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
