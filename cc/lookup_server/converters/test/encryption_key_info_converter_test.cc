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

#include "cc/lookup_server/converters/src/encryption_key_info_converter.h"

#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "googletest/include/gtest/gtest.h"

#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::test::EqualsProto;
using ApiEncryptionKeyInfo =
    ::google::confidential_match::lookup_server::proto_api::EncryptionKeyInfo;
using ApiWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo_WrappedKeyInfo;
using ApiCoordinatorKeyInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo_CoordinatorKeyInfo;
using ApiCoordinatorInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo_CoordinatorInfo;
using BackendEncryptionKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo;
using BackendWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo_WrappedKeyInfo;
using BackendCoordinatorKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo_CoordinatorKeyInfo;
using BackendCoordinatorInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo_CoordinatorInfo;

namespace google::confidential_match::lookup_server {

constexpr absl::string_view kKeyId = "test-key-id";
constexpr absl::string_view kEncryptedDek = "test-encrypted-dek";
constexpr absl::string_view kKekKmsResourceId = "test-kek-kms-resource-id";
constexpr absl::string_view kKmsIdentity = "test-kms-identity";
constexpr absl::string_view kKmsWipProvider = "test-kms-wip-provider";
constexpr absl::string_view kKeyServiceEndpoint = "test-key-service-endpoint";
constexpr absl::string_view kKeyServiceEndpoint2 = "test-key-service-endpoint2";
constexpr absl::string_view kKeyServiceAudienceUrl =
    "test-key-service-audience-url";
constexpr absl::string_view kRoleArn = "test-role-arn";
constexpr absl::string_view kAudience = "test-audience";
constexpr absl::string_view kImpersonatedServiceAccount =
    "test-impersonated-service-account";

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsWrappedKeyType) {
  ApiEncryptionKeyInfo api_info;
  ApiWrappedKeyInfo* wrapped_key_info = api_info.mutable_wrapped_key_info();
  wrapped_key_info->set_key_type(
      ApiEncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()->set_key_type(
      BackendEncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsWrappedEncryptedDek) {
  ApiEncryptionKeyInfo api_info;
  ApiWrappedKeyInfo* wrapped_key_info = api_info.mutable_wrapped_key_info();
  wrapped_key_info->set_encrypted_dek(kEncryptedDek);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()->set_encrypted_dek(
      kEncryptedDek);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsWrappedKekKmsResourceId) {
  ApiEncryptionKeyInfo api_info;
  ApiWrappedKeyInfo* wrapped_key_info = api_info.mutable_wrapped_key_info();
  wrapped_key_info->set_kek_kms_resource_id(kKekKmsResourceId);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()->set_kek_kms_resource_id(
      kKekKmsResourceId);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsWrappedKmsIdentity) {
  ApiEncryptionKeyInfo api_info;
  ApiWrappedKeyInfo* wrapped_key_info = api_info.mutable_wrapped_key_info();
  wrapped_key_info->set_kms_identity(kKmsIdentity);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()
      ->mutable_gcp_wrapped_key_info()
      ->set_service_account_to_impersonate(kKmsIdentity);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsWrappedKmsWipProvider) {
  ApiEncryptionKeyInfo api_info;
  ApiWrappedKeyInfo* wrapped_key_info = api_info.mutable_wrapped_key_info();
  wrapped_key_info->set_kms_wip_provider(kKmsWipProvider);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()
      ->mutable_gcp_wrapped_key_info()
      ->set_wip_provider(kKmsWipProvider);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsAwsWrappedRoleArn) {
  ApiEncryptionKeyInfo api_info;
  api_info.mutable_wrapped_key_info()
      ->mutable_aws_wrapped_key_info()
      ->set_role_arn(kRoleArn);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()
      ->mutable_aws_wrapped_key_info()
      ->set_role_arn(kRoleArn);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsAwsWrappedAudience) {
  ApiEncryptionKeyInfo api_info;
  api_info.mutable_wrapped_key_info()
      ->mutable_aws_wrapped_key_info()
      ->set_audience(kAudience);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()
      ->mutable_aws_wrapped_key_info()
      ->set_audience(kAudience);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsGcpWrappedWipProvider) {
  ApiEncryptionKeyInfo api_info;
  api_info.mutable_wrapped_key_info()
      ->mutable_gcp_wrapped_key_info()
      ->set_wip_provider(kKmsWipProvider);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()
      ->mutable_gcp_wrapped_key_info()
      ->set_wip_provider(kKmsWipProvider);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsGcpWrappedImpersonatedSA) {
  ApiEncryptionKeyInfo api_info;
  api_info.mutable_wrapped_key_info()
      ->mutable_gcp_wrapped_key_info()
      ->set_service_account_to_impersonate(kImpersonatedServiceAccount);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_wrapped_key_info()
      ->mutable_gcp_wrapped_key_info()
      ->set_service_account_to_impersonate(kImpersonatedServiceAccount);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsCoordinatorKeyId) {
  ApiEncryptionKeyInfo api_info;
  api_info.mutable_coordinator_key_info()->set_key_id(kKeyId);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_coordinator_key_info()->set_key_id(kKeyId);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsCoordinatorKeyServiceEndpoint) {
  ApiEncryptionKeyInfo api_info;
  ApiCoordinatorInfo* api_coordinator_info =
      api_info.mutable_coordinator_key_info()->add_coordinator_info();
  api_coordinator_info->set_key_service_endpoint(kKeyServiceEndpoint);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_key_service_endpoint(kKeyServiceEndpoint);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsCoordinatorKmsIdentity) {
  ApiEncryptionKeyInfo api_info;
  ApiCoordinatorInfo* api_coordinator_info =
      api_info.mutable_coordinator_key_info()->add_coordinator_info();
  api_coordinator_info->set_kms_identity(kKmsIdentity);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_kms_identity(kKmsIdentity);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsCoordinatorKmsWipProvider) {
  ApiEncryptionKeyInfo api_info;
  ApiCoordinatorInfo* api_coordinator_info =
      api_info.mutable_coordinator_key_info()->add_coordinator_info();
  api_coordinator_info->set_kms_wip_provider(kKmsWipProvider);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_kms_wip_provider(kKmsWipProvider);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(
    EncryptionKeyInfoConverterTest,
    ConvertEncryptionKeyInfoToBackendConvertsCoordinatorKeyServiceAudienceUrl) {
  ApiEncryptionKeyInfo api_info;
  ApiCoordinatorInfo* api_coordinator_info =
      api_info.mutable_coordinator_key_info()->add_coordinator_info();
  api_coordinator_info->set_key_service_audience_url(kKeyServiceAudienceUrl);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_key_service_audience_url(kKeyServiceAudienceUrl);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

TEST(EncryptionKeyInfoConverterTest,
     ConvertEncryptionKeyInfoToBackendConvertsMultipleCoordinatorInfo) {
  ApiEncryptionKeyInfo api_info;
  api_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_key_service_endpoint(kKeyServiceEndpoint);
  api_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_key_service_endpoint(kKeyServiceEndpoint2);
  api_info.mutable_coordinator_key_info()
      ->mutable_coordinator_info(1)
      ->set_kms_wip_provider(kKmsWipProvider);
  BackendEncryptionKeyInfo expected_backend_info;
  expected_backend_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_key_service_endpoint(kKeyServiceEndpoint);
  expected_backend_info.mutable_coordinator_key_info()
      ->add_coordinator_info()
      ->set_key_service_endpoint(kKeyServiceEndpoint2);
  expected_backend_info.mutable_coordinator_key_info()
      ->mutable_coordinator_info(1)
      ->set_kms_wip_provider(kKmsWipProvider);

  BackendEncryptionKeyInfo converted_info;
  ExecutionResult result = ConvertEncryptionKeyInfo(api_info, converted_info);

  EXPECT_SUCCESS(result);
  EXPECT_THAT(converted_info, EqualsProto(expected_backend_info));
}

}  // namespace google::confidential_match::lookup_server
