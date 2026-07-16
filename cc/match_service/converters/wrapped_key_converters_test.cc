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

#include "cc/match_service/converters/wrapped_key_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;

TEST(WrappedKeyConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::WrappedKey in;
  backend::WrappedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_type(), backend::KEY_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.encrypted_dek(), "");
  EXPECT_EQ(out.kek_kms_resource_id(), "");
  EXPECT_EQ(out.cloud_wrapped_key_info_case(),
            backend::WrappedKey::CLOUD_WRAPPED_KEY_INFO_NOT_SET);
}

TEST(WrappedKeyConvertersTest, ToBackendConvertsKeyType) {
  api::v1::WrappedKey in;
  in.set_key_type(api::v1::KEY_TYPE_XCHACHA20_POLY1305);
  backend::WrappedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_type(), backend::KEY_TYPE_XCHACHA20_POLY1305);
}

TEST(WrappedKeyConvertersTest, ToBackendConvertsEncryptedDek) {
  api::v1::WrappedKey in;
  in.set_encrypted_dek("encrypted_dek");
  backend::WrappedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.encrypted_dek(), "encrypted_dek");
}

TEST(WrappedKeyConvertersTest, ToBackendConvertsKekKmsResourceId) {
  api::v1::WrappedKey in;
  in.set_kek_kms_resource_id("kek_kms_resource_id");
  backend::WrappedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.kek_kms_resource_id(), "kek_kms_resource_id");
}

TEST(WrappedKeyConvertersTest, ToBackendConvertsGcpWrappedKeyInfo) {
  api::v1::WrappedKey in;
  in.mutable_gcp_wrapped_key_info()->set_wip_provider("wip_provider");
  backend::WrappedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.gcp_wrapped_key_info().wip_provider(), "wip_provider");
}

TEST(WrappedKeyConvertersTest, ToBackendConvertsAwsWrappedKeyInfo) {
  api::v1::WrappedKey in;
  in.mutable_aws_wrapped_key_info()->set_role_arn("role_arn");
  in.mutable_aws_wrapped_key_info()->add_signatures("signature");
  backend::WrappedKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.aws_wrapped_key_info().role_arn(), "role_arn");
  EXPECT_EQ(out.aws_wrapped_key_info().signatures_size(), 1);
  EXPECT_EQ(out.aws_wrapped_key_info().signatures(0), "signature");
}

TEST(WrappedKeyConvertersTest, ToApiConvertsEmptyObject) {
  backend::WrappedKey in;
  api::v1::WrappedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_type(), api::v1::KEY_TYPE_UNSPECIFIED);
  EXPECT_EQ(out.encrypted_dek(), "");
  EXPECT_EQ(out.kek_kms_resource_id(), "");
  EXPECT_EQ(out.cloud_wrapped_key_info_case(),
            api::v1::WrappedKey::CLOUD_WRAPPED_KEY_INFO_NOT_SET);
}

TEST(WrappedKeyConvertersTest, ToApiConvertsKeyType) {
  backend::WrappedKey in;
  in.set_key_type(backend::KEY_TYPE_XCHACHA20_POLY1305);
  api::v1::WrappedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_type(), api::v1::KEY_TYPE_XCHACHA20_POLY1305);
}

TEST(WrappedKeyConvertersTest, ToApiConvertsEncryptedDek) {
  backend::WrappedKey in;
  in.set_encrypted_dek("encrypted_dek");
  api::v1::WrappedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.encrypted_dek(), "encrypted_dek");
}

TEST(WrappedKeyConvertersTest, ToApiConvertsKekKmsResourceId) {
  backend::WrappedKey in;
  in.set_kek_kms_resource_id("kek_kms_resource_id");
  api::v1::WrappedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.kek_kms_resource_id(), "kek_kms_resource_id");
}

TEST(WrappedKeyConvertersTest, ToApiConvertsGcpWrappedKeyInfo) {
  backend::WrappedKey in;
  in.mutable_gcp_wrapped_key_info()->set_wip_provider("wip_provider");
  api::v1::WrappedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.gcp_wrapped_key_info().wip_provider(), "wip_provider");
}

TEST(WrappedKeyConvertersTest, ToApiConvertsAwsWrappedKeyInfo) {
  backend::WrappedKey in;
  in.mutable_aws_wrapped_key_info()->set_role_arn("role_arn");
  in.mutable_aws_wrapped_key_info()->add_signatures("signature");
  api::v1::WrappedKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.aws_wrapped_key_info().role_arn(), "role_arn");
  EXPECT_EQ(out.aws_wrapped_key_info().signatures_size(), 1);
  EXPECT_EQ(out.aws_wrapped_key_info().signatures(0), "signature");
}

}  // namespace google::confidential_match::match_service
