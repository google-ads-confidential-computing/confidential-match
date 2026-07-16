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

#include "cc/match_service/converters/matched_field_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(MatchedFieldConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::MatchedField in;
  backend::MatchedField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), backend::STATUS_UNSPECIFIED);
  EXPECT_EQ(out.error_reason(), backend::ERROR_REASON_UNKNOWN);
  EXPECT_FALSE(out.has_matched_field_info());
  EXPECT_EQ(out.matched_associated_data_indices_size(), 0);
}

TEST(MatchedFieldConvertersTest, ToBackendConvertsStatus) {
  api::v1::MatchedField in;
  in.set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest, ToBackendConvertsErrorReason) {
  api::v1::MatchedField in;
  in.set_error_reason(api::v1::ERROR_REASON_INTERNAL_ERROR);
  backend::MatchedField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.error_reason(), backend::ERROR_REASON_INTERNAL_ERROR);
}

TEST(MatchedFieldConvertersTest, ToBackendConvertsMatchedFieldInfo) {
  api::v1::MatchedField in;
  in.mutable_matched_field_info()->set_field_type(api::v1::FIELD_TYPE_EMAIL);
  in.mutable_matched_field_info()->set_field_value("test@google.com");
  backend::MatchedField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_matched_field_info());
  EXPECT_EQ(out.matched_field_info().field_type(), backend::FIELD_TYPE_EMAIL);
  EXPECT_EQ(out.matched_field_info().field_value(), "test@google.com");
}

TEST(MatchedFieldConvertersTest,
     ToBackendConvertsMatchedAssociatedDataIndices) {
  api::v1::MatchedField in;
  in.add_matched_associated_data_indices(1);
  in.add_matched_associated_data_indices(2);
  backend::MatchedField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_associated_data_indices_size(), 2);
  EXPECT_EQ(out.matched_associated_data_indices(0), 1);
  EXPECT_EQ(out.matched_associated_data_indices(1), 2);
}

TEST(MatchedFieldConvertersTest, ToApiConvertsEmptyObject) {
  backend::MatchedField in;
  api::v1::MatchedField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), api::v1::STATUS_UNSPECIFIED);
  EXPECT_EQ(out.error_reason(), api::v1::ERROR_REASON_UNKNOWN);
  EXPECT_FALSE(out.has_matched_field_info());
  EXPECT_EQ(out.matched_associated_data_indices_size(), 0);
}

TEST(MatchedFieldConvertersTest, ToApiConvertsStatus) {
  backend::MatchedField in;
  in.set_status(backend::STATUS_SUCCESS_UNMATCHED);
  api::v1::MatchedField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(MatchedFieldConvertersTest, ToApiConvertsErrorReason) {
  backend::MatchedField in;
  in.set_error_reason(backend::ERROR_REASON_INTERNAL_ERROR);
  api::v1::MatchedField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.error_reason(), api::v1::ERROR_REASON_INTERNAL_ERROR);
}

TEST(MatchedFieldConvertersTest, ToApiConvertsMatchedFieldInfo) {
  backend::MatchedField in;
  in.mutable_matched_field_info()->set_field_type(backend::FIELD_TYPE_PHONE);
  in.mutable_matched_field_info()->set_field_value("650-555-1234");
  api::v1::MatchedField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_matched_field_info());
  EXPECT_EQ(out.matched_field_info().field_type(), api::v1::FIELD_TYPE_PHONE);
  EXPECT_EQ(out.matched_field_info().field_value(), "650-555-1234");
}

TEST(MatchedFieldConvertersTest, ToApiConvertsMatchedAssociatedDataIndices) {
  backend::MatchedField in;
  in.add_matched_associated_data_indices(3);
  in.add_matched_associated_data_indices(4);
  api::v1::MatchedField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_associated_data_indices_size(), 2);
  EXPECT_EQ(out.matched_associated_data_indices(0), 3);
  EXPECT_EQ(out.matched_associated_data_indices(1), 4);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToBackendConvertsEmptyObject) {
  api::v1::MatchedCompositeField in;
  backend::MatchedCompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), backend::STATUS_UNSPECIFIED);
  EXPECT_FALSE(out.has_error_reason());
  EXPECT_EQ(out.matched_field_info_size(), 0);
  EXPECT_EQ(out.matched_associated_data_indices_size(), 0);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToBackendConvertsStatus) {
  api::v1::MatchedCompositeField in;
  in.set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedCompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToBackendConvertsErrorReason) {
  api::v1::MatchedCompositeField in;
  in.set_error_reason(api::v1::ERROR_REASON_INTERNAL_ERROR);
  backend::MatchedCompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.error_reason(), backend::ERROR_REASON_INTERNAL_ERROR);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToBackendConvertsMatchedFieldInfo) {
  api::v1::MatchedCompositeField in;
  api::v1::MatchedFieldInfo* info = in.add_matched_field_info();
  info->set_field_type(api::v1::FIELD_TYPE_EMAIL);
  backend::MatchedCompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_field_info_size(), 1);
  EXPECT_EQ(out.matched_field_info(0).field_type(), backend::FIELD_TYPE_EMAIL);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToBackendConvertsMatchedAssociatedDataIndices) {
  api::v1::MatchedCompositeField in;
  in.add_matched_associated_data_indices(1);
  in.add_matched_associated_data_indices(2);
  backend::MatchedCompositeField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_associated_data_indices_size(), 2);
  EXPECT_EQ(out.matched_associated_data_indices(0), 1);
  EXPECT_EQ(out.matched_associated_data_indices(1), 2);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToApiConvertsEmptyObject) {
  backend::MatchedCompositeField in;
  api::v1::MatchedCompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), api::v1::STATUS_UNSPECIFIED);
  EXPECT_FALSE(out.has_error_reason());
  EXPECT_EQ(out.matched_field_info_size(), 0);
  EXPECT_EQ(out.matched_associated_data_indices_size(), 0);
}

TEST(MatchedFieldConvertersTest, MatchedCompositeField_ToApiConvertsStatus) {
  backend::MatchedCompositeField in;
  in.set_status(backend::STATUS_SUCCESS_UNMATCHED);
  api::v1::MatchedCompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToApiConvertsErrorReason) {
  backend::MatchedCompositeField in;
  in.set_error_reason(backend::ERROR_REASON_UNKNOWN);
  api::v1::MatchedCompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.error_reason(), api::v1::ERROR_REASON_UNKNOWN);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToApiConvertsMatchedFieldInfo) {
  backend::MatchedCompositeField in;
  backend::MatchedFieldInfo* info = in.add_matched_field_info();
  info->set_field_type(backend::FIELD_TYPE_PHONE);
  api::v1::MatchedCompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_field_info_size(), 1);
  EXPECT_EQ(out.matched_field_info(0).field_type(), api::v1::FIELD_TYPE_PHONE);
}

TEST(MatchedFieldConvertersTest,
     MatchedCompositeField_ToApiConvertsMatchedAssociatedDataIndices) {
  backend::MatchedCompositeField in;
  in.add_matched_associated_data_indices(3);
  in.add_matched_associated_data_indices(4);
  api::v1::MatchedCompositeField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.matched_associated_data_indices_size(), 2);
  EXPECT_EQ(out.matched_associated_data_indices(0), 3);
  EXPECT_EQ(out.matched_associated_data_indices(1), 4);
}

TEST(MatchedFieldConvertersTest,
     MatchedFanOutField_ToBackendConvertsEmptyObject) {
  api::v1::MatchedFanOutField in;
  backend::MatchedFanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), backend::STATUS_UNSPECIFIED);
  EXPECT_EQ(out.fanned_out_keys_size(), 0);
}

TEST(MatchedFieldConvertersTest, MatchedFanOutField_ToBackendConvertsStatus) {
  api::v1::MatchedFanOutField in;
  in.set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedFanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest,
     MatchedFanOutField_ToBackendConvertsErrorReason) {
  api::v1::MatchedFanOutField in;
  in.set_error_reason(api::v1::ERROR_REASON_INTERNAL_ERROR);
  backend::MatchedFanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.error_reason(), backend::ERROR_REASON_INTERNAL_ERROR);
}

TEST(MatchedFieldConvertersTest,
     MatchedFanOutField_ToBackendConvertsFannedOutKeys) {
  api::v1::MatchedFanOutField in;
  api::v1::FannedOutMatchedKey* key = in.add_fanned_out_keys();
  api::v1::MatchedField* field = key->mutable_field();
  field->set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::MatchedFanOutField out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.fanned_out_keys_size(), 1);
  EXPECT_EQ(out.fanned_out_keys(0).field().status(),
            backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest, MatchedFanOutField_ToApiConvertsEmptyObject) {
  backend::MatchedFanOutField in;
  api::v1::MatchedFanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), api::v1::STATUS_UNSPECIFIED);
  EXPECT_EQ(out.fanned_out_keys_size(), 0);
}

TEST(MatchedFieldConvertersTest, MatchedFanOutField_ToApiConvertsStatus) {
  backend::MatchedFanOutField in;
  in.set_status(backend::STATUS_SUCCESS_UNMATCHED);
  api::v1::MatchedFanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.status(), api::v1::STATUS_SUCCESS_UNMATCHED);
}

TEST(MatchedFieldConvertersTest, MatchedFanOutField_ToApiConvertsErrorReason) {
  backend::MatchedFanOutField in;
  in.set_error_reason(backend::ERROR_REASON_UNKNOWN);
  api::v1::MatchedFanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.error_reason(), api::v1::ERROR_REASON_UNKNOWN);
}

TEST(MatchedFieldConvertersTest,
     MatchedFanOutField_ToApiConvertsFannedOutKeys) {
  backend::MatchedFanOutField in;
  backend::FannedOutMatchedKey* key = in.add_fanned_out_keys();
  backend::MatchedCompositeField* field = key->mutable_composite_field();
  field->set_status(backend::STATUS_FAILED);
  api::v1::MatchedFanOutField out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.fanned_out_keys_size(), 1);
  EXPECT_EQ(out.fanned_out_keys(0).composite_field().status(),
            api::v1::STATUS_FAILED);
}

TEST(MatchedFieldConvertersTest,
     FannedOutMatchedKey_ToBackendConvertsEmptyObject) {
  api::v1::FannedOutMatchedKey in;
  backend::FannedOutMatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_info_case(),
            backend::FannedOutMatchedKey::FIELD_INFO_NOT_SET);
}

TEST(MatchedFieldConvertersTest, FannedOutMatchedKey_ToBackendConvertsField) {
  api::v1::FannedOutMatchedKey in;
  in.mutable_field()->set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::FannedOutMatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field().status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest,
     FannedOutMatchedKey_ToBackendConvertsCompositeField) {
  api::v1::FannedOutMatchedKey in;
  in.mutable_composite_field()->set_status(api::v1::STATUS_SUCCESS_MATCHED);
  backend::FannedOutMatchedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.composite_field().status(), backend::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest, FannedOutMatchedKey_ToApiConvertsEmptyObject) {
  backend::FannedOutMatchedKey in;
  api::v1::FannedOutMatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_info_case(),
            api::v1::FannedOutMatchedKey::FIELD_INFO_NOT_SET);
}

TEST(MatchedFieldConvertersTest, FannedOutMatchedKey_ToApiConvertsField) {
  backend::FannedOutMatchedKey in;
  in.mutable_field()->set_status(backend::STATUS_SUCCESS_MATCHED);
  api::v1::FannedOutMatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field().status(), api::v1::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest,
     FannedOutMatchedKey_ToApiConvertsCompositeField) {
  backend::FannedOutMatchedKey in;
  in.mutable_composite_field()->set_status(backend::STATUS_SUCCESS_MATCHED);
  api::v1::FannedOutMatchedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.composite_field().status(), api::v1::STATUS_SUCCESS_MATCHED);
}

TEST(MatchedFieldConvertersTest,
     MatchedFieldInfo_ToBackendConvertsEmptyObject) {
  api::v1::MatchedFieldInfo in;
  backend::MatchedFieldInfo out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_type(), backend::FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.field_value(), "");
  EXPECT_FALSE(out.has_gtag_metadata());
}

TEST(MatchedFieldConvertersTest, MatchedFieldInfo_ToBackendConvertsFieldType) {
  api::v1::MatchedFieldInfo in;
  in.set_field_type(api::v1::FIELD_TYPE_EMAIL);
  backend::MatchedFieldInfo out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_type(), backend::FIELD_TYPE_EMAIL);
}

TEST(MatchedFieldConvertersTest, MatchedFieldInfo_ToBackendConvertsFieldValue) {
  api::v1::MatchedFieldInfo in;
  in.set_field_value("test@google.com");
  backend::MatchedFieldInfo out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_value(), "test@google.com");
}

TEST(MatchedFieldConvertersTest,
     MatchedFieldInfo_ToBackendConvertsGTagMetadata) {
  api::v1::MatchedFieldInfo in;
  in.mutable_gtag_metadata()->set_index(1);
  in.mutable_gtag_metadata()->set_error_code("error");
  backend::MatchedFieldInfo out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_gtag_metadata());
  EXPECT_EQ(out.gtag_metadata().index(), 1);
  EXPECT_EQ(out.gtag_metadata().error_code(), "error");
}

TEST(MatchedFieldConvertersTest, MatchedFieldInfo_ToApiConvertsEmptyObject) {
  backend::MatchedFieldInfo in;
  api::v1::MatchedFieldInfo out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_type(), api::v1::FIELD_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.field_value(), "");
  EXPECT_FALSE(out.has_gtag_metadata());
}

TEST(MatchedFieldConvertersTest, MatchedFieldInfo_ToApiConvertsFieldType) {
  backend::MatchedFieldInfo in;
  in.set_field_type(backend::FIELD_TYPE_PHONE);
  api::v1::MatchedFieldInfo out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_type(), api::v1::FIELD_TYPE_PHONE);
}

TEST(MatchedFieldConvertersTest, MatchedFieldInfo_ToApiConvertsFieldValue) {
  backend::MatchedFieldInfo in;
  in.set_field_value("+16505551234");
  api::v1::MatchedFieldInfo out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.field_value(), "+16505551234");
}

TEST(MatchedFieldConvertersTest, MatchedFieldInfo_ToApiConvertsGTagMetadata) {
  backend::MatchedFieldInfo in;
  in.mutable_gtag_metadata()->set_index(2);
  in.mutable_gtag_metadata()->set_error_code("error_code");
  api::v1::MatchedFieldInfo out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_TRUE(out.has_gtag_metadata());
  EXPECT_EQ(out.gtag_metadata().index(), 2);
  EXPECT_EQ(out.gtag_metadata().error_code(), "error_code");
}

}  // namespace google::confidential_match::match_service
