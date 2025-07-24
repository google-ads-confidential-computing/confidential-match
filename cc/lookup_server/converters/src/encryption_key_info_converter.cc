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

#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"

#include "cc/lookup_server/converters/src/error_codes.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ApiEncryptionKeyInfo =
    ::google::confidential_match::lookup_server::proto_api::EncryptionKeyInfo;
using ApiWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo::WrappedKeyInfo;
using ApiAwsWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo::WrappedKeyInfo::AwsWrappedKeyInfo;
using ApiGcpWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo::WrappedKeyInfo::GcpWrappedKeyInfo;
using ApiCoordinatorKeyInfo = ::google::confidential_match::lookup_server::
    proto_api::EncryptionKeyInfo::CoordinatorKeyInfo;
using BackendEncryptionKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo;
using BackendWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo::WrappedKeyInfo;
using BackendAwsWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo::WrappedKeyInfo::AwsWrappedKeyInfo;
using BackendGcpWrappedKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo::WrappedKeyInfo::GcpWrappedKeyInfo;
using BackendCoordinatorKeyInfo = ::google::confidential_match::lookup_server::
    proto_backend::EncryptionKeyInfo::CoordinatorKeyInfo;

constexpr absl::string_view kComponentName = "EncryptionKeyInfoConverter";

// Helper to convert AwsWrappedKeyInfo objects.
ExecutionResult ConvertAwsWrappedKeyInfo(
    const ApiAwsWrappedKeyInfo& api_key_info, BackendAwsWrappedKeyInfo& out) {
  out.set_role_arn(api_key_info.role_arn());
  out.set_audience(api_key_info.audience());
  return SuccessExecutionResult();
}

// Helper to convert GcpWrappedKeyInfo objects.
ExecutionResult ConvertGcpWrappedKeyInfo(
    const ApiGcpWrappedKeyInfo& api_key_info, BackendGcpWrappedKeyInfo& out) {
  out.set_wip_provider(api_key_info.wip_provider());
  out.set_service_account_to_impersonate(
      api_key_info.service_account_to_impersonate());
  return SuccessExecutionResult();
}

// Helper to convert WrappedKeyInfo objects.
ExecutionResult ConvertWrappedKeyInfo(const ApiWrappedKeyInfo& wrapped_key_info,
                                      BackendWrappedKeyInfo& out) {
  out.Clear();

  BackendWrappedKeyInfo::KeyType backend_key_type;
  bool is_parse_successful = BackendWrappedKeyInfo::KeyType_Parse(
      ApiWrappedKeyInfo::KeyType_Name(wrapped_key_info.key_type()),
      &backend_key_type);
  if (!is_parse_successful) {
    FailureExecutionResult error(CONVERTER_PARSE_ERROR);
    SCP_ERROR(kComponentName, kZeroUuid, error,
              absl::StrFormat("Failed to parse wrapped key info KeyType: %s",
                              ApiWrappedKeyInfo::KeyType_Name(
                                  wrapped_key_info.key_type())));
    return error;
  }

  out.set_key_type(backend_key_type);
  out.set_encrypted_dek(wrapped_key_info.encrypted_dek());
  out.set_kek_kms_resource_id(wrapped_key_info.kek_kms_resource_id());

  switch (wrapped_key_info.cloud_wrapped_key_info_case()) {
    case ApiWrappedKeyInfo::kAwsWrappedKeyInfo:
      RETURN_IF_FAILURE(
          ConvertAwsWrappedKeyInfo(wrapped_key_info.aws_wrapped_key_info(),
                                   *out.mutable_aws_wrapped_key_info()));
      break;

    case ApiWrappedKeyInfo::kGcpWrappedKeyInfo:
      RETURN_IF_FAILURE(
          ConvertGcpWrappedKeyInfo(wrapped_key_info.gcp_wrapped_key_info(),
                                   *out.mutable_gcp_wrapped_key_info()));
      break;

    case ApiWrappedKeyInfo::CLOUD_WRAPPED_KEY_INFO_NOT_SET:
      break;

    default:
      ExecutionResult error = FailureExecutionResult(CONVERTER_PARSE_ERROR);
      SCP_ERROR(
          kComponentName, kZeroUuid, error,
          absl::StrFormat(
              "Got an unknown key info case for cloud_wrapped_key_info: %d",
              static_cast<int>(
                  wrapped_key_info.cloud_wrapped_key_info_case())));
  }

  // Copy over legacy fields while fields have not yet been deprecated
  if (!wrapped_key_info.kms_identity().empty() &&
      wrapped_key_info.gcp_wrapped_key_info()
          .service_account_to_impersonate()
          .empty()) {
    out.mutable_gcp_wrapped_key_info()->set_service_account_to_impersonate(
        wrapped_key_info.kms_identity());
  }
  if (!wrapped_key_info.kms_wip_provider().empty() &&
      wrapped_key_info.gcp_wrapped_key_info().wip_provider().empty()) {
    out.mutable_gcp_wrapped_key_info()->set_wip_provider(
        wrapped_key_info.kms_wip_provider());
  }

  return SuccessExecutionResult();
}

// Helper to convert CoordinatorKeyInfo objects.
ExecutionResult ConvertCoordinatorKeyInfo(
    const ApiCoordinatorKeyInfo& coordinator_key_info,
    BackendCoordinatorKeyInfo& out) {
  out.set_key_id(coordinator_key_info.key_id());
  for (const auto& info : coordinator_key_info.coordinator_info()) {
    BackendEncryptionKeyInfo::CoordinatorInfo* out_info =
        out.add_coordinator_info();
    out_info->set_key_service_endpoint(info.key_service_endpoint());
    out_info->set_kms_identity(info.kms_identity());
    out_info->set_kms_wip_provider(info.kms_wip_provider());
    out_info->set_key_service_audience_url(info.key_service_audience_url());
  }

  return SuccessExecutionResult();
}

}  // namespace

ExecutionResult ConvertEncryptionKeyInfo(
    const ApiEncryptionKeyInfo& encryption_key_info,
    BackendEncryptionKeyInfo& out) {
  out.clear_key_info();

  switch (encryption_key_info.key_info_case()) {
    case ApiEncryptionKeyInfo::kWrappedKeyInfo:
      RETURN_IF_FAILURE(
          ConvertWrappedKeyInfo(encryption_key_info.wrapped_key_info(),
                                *out.mutable_wrapped_key_info()));
      break;

    case ApiEncryptionKeyInfo::kCoordinatorKeyInfo:
      RETURN_IF_FAILURE(
          ConvertCoordinatorKeyInfo(encryption_key_info.coordinator_key_info(),
                                    *out.mutable_coordinator_key_info()));
      break;

    case ApiEncryptionKeyInfo::KEY_INFO_NOT_SET:
      break;

    default:
      ExecutionResult error = FailureExecutionResult(CONVERTER_PARSE_ERROR);
      SCP_ERROR(kComponentName, kZeroUuid, error,
                absl::StrFormat(
                    "Got an unknown key info case for EncryptionKeyInfo: %d",
                    static_cast<int>(encryption_key_info.key_info_case())));
  }

  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
