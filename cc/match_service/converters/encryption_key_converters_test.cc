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

#include "cc/match_service/converters/encryption_key_converters.h"

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "gtest/gtest.h"

#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

TEST(EncryptionKeyConvertersTest, ToBackendConvertsEmptyObject) {
  api::v1::EncryptionKey in;
  backend::EncryptionKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_case(), backend::EncryptionKey::KEY_NOT_SET);
}

TEST(EncryptionKeyConvertersTest, ToBackendConvertsWrappedKey) {
  api::v1::EncryptionKey in;
  in.mutable_wrapped_key()->set_encrypted_dek("test_dek");
  backend::EncryptionKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_case(), backend::EncryptionKey::kWrappedKey);
  EXPECT_EQ(out.wrapped_key().encrypted_dek(), "test_dek");
}

TEST(EncryptionKeyConvertersTest, ToBackendConvertsCoordinatorKey) {
  api::v1::EncryptionKey in;
  in.mutable_coordinator_key()->set_key_id("test_key_id");
  backend::EncryptionKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_case(), backend::EncryptionKey::kCoordinatorKey);
  EXPECT_EQ(out.coordinator_key().key_id(), "test_key_id");
}

TEST(EncryptionKeyConvertersTest, ToApiConvertsEmptyObject) {
  backend::EncryptionKey in;
  api::v1::EncryptionKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_case(), api::v1::EncryptionKey::KEY_NOT_SET);
}

TEST(EncryptionKeyConvertersTest, ToApiConvertsWrappedKey) {
  backend::EncryptionKey in;
  in.mutable_wrapped_key()->set_encrypted_dek("test_dek");
  api::v1::EncryptionKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_case(), api::v1::EncryptionKey::kWrappedKey);
  EXPECT_EQ(out.wrapped_key().encrypted_dek(), "test_dek");
}

TEST(EncryptionKeyConvertersTest, ToApiConvertsCoordinatorKey) {
  backend::EncryptionKey in;
  in.mutable_coordinator_key()->set_key_id("test_key_id");
  api::v1::EncryptionKey out;

  absl::Status status = ToApi(in, out);

  EXPECT_THAT(status, IsOk());
  EXPECT_EQ(out.key_case(), api::v1::EncryptionKey::kCoordinatorKey);
  EXPECT_EQ(out.coordinator_key().key_id(), "test_key_id");
}

TEST(EncryptionKeyConvertersTest, ToCoreGcpWrappedKey) {
  backend::EncryptionKey in;
  auto* wrapped = in.mutable_wrapped_key();
  wrapped->set_key_type(backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("gcp_dek");
  wrapped->set_kek_kms_resource_id("gcp_kek");
  auto* gcp = wrapped->mutable_gcp_wrapped_key_info();
  gcp->set_wip_provider(
      "projects/123/locations/global/workloadIdentityPools/pool/providers/"
      "prov");
  EncryptionKeyInfo out;

  absl::Status status = ToCore(in, out);

  EXPECT_THAT(status, IsOk());
  const auto& out_wrapped = out.wrapped_key_info();
  EXPECT_EQ(out_wrapped.encrypted_dek(), "gcp_dek");
  ASSERT_TRUE(out_wrapped.has_gcp_wrapped_key_info());
  const auto& out_gcp = out_wrapped.gcp_wrapped_key_info();
  EXPECT_EQ(out_gcp.wip_provider(),
            "projects/123/locations/global/workloadIdentityPools/pool/"
            "providers/prov");
}

TEST(EncryptionKeyConvertersTest, ToCoreAwsWrappedKey) {
  backend::EncryptionKey in;
  auto* wrapped = in.mutable_wrapped_key();
  wrapped->set_key_type(backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("test_dek");
  wrapped->set_kek_kms_resource_id("test_kek");
  auto* aws = wrapped->mutable_aws_wrapped_key_info();
  aws->set_role_arn("arn:aws:iam::123456789012:role/test-role");
  aws->add_signatures("sig1");
  aws->add_signatures("sig2");
  EncryptionKeyInfo out;

  absl::Status status = ToCore(in, out);

  EXPECT_THAT(status, IsOk());
  ASSERT_TRUE(out.has_wrapped_key_info());
  const auto& out_wrapped = out.wrapped_key_info();
  EXPECT_EQ(out_wrapped.key_type(),
            EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305);
  EXPECT_EQ(out_wrapped.encrypted_dek(), "test_dek");
  EXPECT_EQ(out_wrapped.kek_kms_resource_id(), "test_kek");
  ASSERT_TRUE(out_wrapped.has_aws_wrapped_key_info());
  const auto& out_aws = out_wrapped.aws_wrapped_key_info();
  EXPECT_EQ(out_aws.role_arn(), "arn:aws:iam::123456789012:role/test-role");
  EXPECT_EQ(out_aws.signatures_size(), 2);
  EXPECT_EQ(out_aws.signatures(0), "sig1");
}

TEST(EncryptionKeyConvertersTest, ToCoreCoordinatorKey) {
  backend::EncryptionKey in;
  auto* coord = in.mutable_coordinator_key();
  coord->set_key_id("coord-key-123");
  EncryptionKeyInfo out;

  absl::Status status = ToCore(in, out);

  EXPECT_THAT(status, IsOk());
  ASSERT_TRUE(out.has_coordinator_key_info());
  EXPECT_EQ(out.coordinator_key_info().key_id(), "coord-key-123");
}

TEST(EncryptionKeyConvertersTest, ToBackendGcpWrappedKey) {
  EncryptionKeyInfo in;
  auto* wrapped = in.mutable_wrapped_key_info();
  wrapped->set_key_type(
      EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("test_dek");
  wrapped->set_kek_kms_resource_id("test_kek");
  auto* gcp = wrapped->mutable_gcp_wrapped_key_info();
  gcp->set_wip_provider("test-wip");
  backend::EncryptionKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  ASSERT_TRUE(out.has_wrapped_key());
  ASSERT_TRUE(out.wrapped_key().has_gcp_wrapped_key_info());
  EXPECT_EQ(out.wrapped_key().gcp_wrapped_key_info().wip_provider(),
            "test-wip");
}

TEST(EncryptionKeyConvertersTest, ToBackendAwsWrappedKey) {
  EncryptionKeyInfo in;
  auto* wrapped = in.mutable_wrapped_key_info();
  wrapped->set_key_type(
      EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("test_dek");
  wrapped->set_kek_kms_resource_id("test_kek");
  auto* aws = wrapped->mutable_aws_wrapped_key_info();
  aws->set_role_arn("test-role");
  backend::EncryptionKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  ASSERT_TRUE(out.has_wrapped_key());
  const auto& out_wrapped = out.wrapped_key();
  EXPECT_EQ(out_wrapped.key_type(),
            backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  ASSERT_TRUE(out_wrapped.has_aws_wrapped_key_info());
  EXPECT_EQ(out_wrapped.aws_wrapped_key_info().role_arn(), "test-role");
}

TEST(EncryptionKeyConvertersTest, ToBackendCoordinatorKey) {
  EncryptionKeyInfo in;
  auto* coord = in.mutable_coordinator_key_info();
  coord->set_key_id("coord-key-id");
  backend::EncryptionKey out;

  absl::Status status = ToBackend(in, out);

  EXPECT_THAT(status, IsOk());
  ASSERT_TRUE(out.has_coordinator_key());
  EXPECT_EQ(out.coordinator_key().key_id(), "coord-key-id");
}

TEST(EncryptionKeyConvertersTest, ToCoreInvalidKeyType) {
  backend::EncryptionKey in;
  auto* wrapped = in.mutable_wrapped_key();
  // Cast an invalid integer to the enum to simulate an unknown enum
  wrapped->set_key_type(static_cast<backend::KeyType>(99));
  EncryptionKeyInfo out;

  absl::Status status = ToCore(in, out);

  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace google::confidential_match::match_service
