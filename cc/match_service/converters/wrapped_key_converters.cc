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

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/key_type_converters.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;

// Converts an API GcpWrappedKeyInfo to the backend format.
absl::Status ToBackend(const api::v1::WrappedKey::GcpWrappedKeyInfo& in,
                       backend::WrappedKey::GcpWrappedKeyInfo& out) {
  out.Clear();
  out.set_wip_provider(in.wip_provider());
  return absl::OkStatus();
}

// Converts a backend GcpWrappedKeyInfo to the API format.
absl::Status ToApi(const backend::WrappedKey::GcpWrappedKeyInfo& in,
                   api::v1::WrappedKey::GcpWrappedKeyInfo& out) {
  out.Clear();
  out.set_wip_provider(in.wip_provider());
  return absl::OkStatus();
}

// Converts an API AwsWrappedKeyInfo to the backend format.
absl::Status ToBackend(const api::v1::WrappedKey::AwsWrappedKeyInfo& in,
                       backend::WrappedKey::AwsWrappedKeyInfo& out) {
  out.Clear();
  out.set_role_arn(in.role_arn());
  for (const auto& signature : in.signatures()) {
    out.add_signatures(signature);
  }
  return absl::OkStatus();
}

// Converts a backend AwsWrappedKeyInfo to the API format.
absl::Status ToApi(const backend::WrappedKey::AwsWrappedKeyInfo& in,
                   api::v1::WrappedKey::AwsWrappedKeyInfo& out) {
  out.Clear();
  out.set_role_arn(in.role_arn());
  for (const auto& signature : in.signatures()) {
    out.add_signatures(signature);
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ToBackend(const api::v1::WrappedKey& in,
                       backend::WrappedKey& out) {
  out.Clear();
  backend::KeyType key_type;
  RETURN_IF_ERROR(ToBackend(in.key_type(), key_type));
  out.set_key_type(key_type);
  out.set_encrypted_dek(in.encrypted_dek());
  out.set_kek_kms_resource_id(in.kek_kms_resource_id());
  switch (in.cloud_wrapped_key_info_case()) {
    case api::v1::WrappedKey::kGcpWrappedKeyInfo:
      RETURN_IF_ERROR(ToBackend(in.gcp_wrapped_key_info(),
                                *out.mutable_gcp_wrapped_key_info()));
      break;
    case api::v1::WrappedKey::kAwsWrappedKeyInfo:
      RETURN_IF_ERROR(ToBackend(in.aws_wrapped_key_info(),
                                *out.mutable_aws_wrapped_key_info()));
      break;
    case api::v1::WrappedKey::CLOUD_WRAPPED_KEY_INFO_NOT_SET:
      break;
    default:
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          absl::StrCat("Failed to parse WrappedKey cloud_wrapped_key_info: ",
                       in.cloud_wrapped_key_info_case()));
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::WrappedKey& in, api::v1::WrappedKey& out) {
  out.Clear();
  api::v1::KeyType key_type;
  RETURN_IF_ERROR(ToApi(in.key_type(), key_type));
  out.set_key_type(key_type);
  out.set_encrypted_dek(in.encrypted_dek());
  out.set_kek_kms_resource_id(in.kek_kms_resource_id());
  switch (in.cloud_wrapped_key_info_case()) {
    case backend::WrappedKey::kGcpWrappedKeyInfo:
      RETURN_IF_ERROR(ToApi(in.gcp_wrapped_key_info(),
                            *out.mutable_gcp_wrapped_key_info()));
      break;
    case backend::WrappedKey::kAwsWrappedKeyInfo:
      RETURN_IF_ERROR(ToApi(in.aws_wrapped_key_info(),
                            *out.mutable_aws_wrapped_key_info()));
      break;
    case backend::WrappedKey::CLOUD_WRAPPED_KEY_INFO_NOT_SET:
      break;
    default:
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          absl::StrCat("Failed to parse WrappedKey cloud_wrapped_key_info: ",
                       in.cloud_wrapped_key_info_case()));
  }
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
