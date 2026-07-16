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

#include "cc/match_service/converters/coordinator_key_converters.h"
#include "cc/match_service/converters/wrapped_key_converters.h"
#include "cc/match_service/error/error.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

namespace {

using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::match_service::backend::CoordinatorKey;
using ::google::confidential_match::match_service::backend::EncryptionKey;
using ::google::confidential_match::match_service::backend::KeyType;
using ::google::confidential_match::match_service::backend::WrappedKey;

absl::Status ToEncryptionKeyInfoKeyType(
    KeyType in, EncryptionKeyInfo::WrappedKeyInfo::KeyType& out) {
  switch (in) {
    case KeyType::KEY_TYPE_XCHACHA20_POLY1305:
      out = EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305;
      return absl::OkStatus();
    case KeyType::KEY_TYPE_UNSPECIFIED:
      out = EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Unsupported backend KeyType: ", in));
  }
}

absl::Status ToMatchServiceKeyType(
    EncryptionKeyInfo::WrappedKeyInfo::KeyType in, KeyType& out) {
  switch (in) {
    case EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_XCHACHA20_POLY1305:
      out = KeyType::KEY_TYPE_XCHACHA20_POLY1305;
      return absl::OkStatus();
    case EncryptionKeyInfo::WrappedKeyInfo::KEY_TYPE_UNSPECIFIED:
      out = KeyType::KEY_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Unsupported internal KeyType: ", in));
  }
}
}  // namespace

absl::Status ToBackend(const api::v1::EncryptionKey& in,
                       backend::EncryptionKey& out) {
  out.Clear();
  switch (in.key_case()) {
    case api::v1::EncryptionKey::kWrappedKey:
      return ToBackend(in.wrapped_key(), *out.mutable_wrapped_key());
    case api::v1::EncryptionKey::kCoordinatorKey:
      return ToBackend(in.coordinator_key(), *out.mutable_coordinator_key());
    case api::v1::EncryptionKey::KEY_NOT_SET:
      return absl::OkStatus();
  }
  return Status(
      Error::CONVERTER_PARSE_ERROR,
      absl::StrCat("Failed to parse EncryptionKey key_case: ", in.key_case()));
}

absl::Status ToApi(const backend::EncryptionKey& in,
                   api::v1::EncryptionKey& out) {
  out.Clear();
  switch (in.key_case()) {
    case backend::EncryptionKey::kWrappedKey:
      return ToApi(in.wrapped_key(), *out.mutable_wrapped_key());
    case backend::EncryptionKey::kCoordinatorKey:
      return ToApi(in.coordinator_key(), *out.mutable_coordinator_key());
    case backend::EncryptionKey::KEY_NOT_SET:
      return absl::OkStatus();
  }
  return Status(
      Error::CONVERTER_PARSE_ERROR,
      absl::StrCat("Failed to parse EncryptionKey key_case: ", in.key_case()));
}

absl::Status ToCore(const EncryptionKey& input, EncryptionKeyInfo& output) {
  if (input.has_wrapped_key()) {
    const auto& in_wrapped = input.wrapped_key();
    auto* out_wrapped = output.mutable_wrapped_key_info();

    EncryptionKeyInfo::WrappedKeyInfo::KeyType key_type;
    auto status = ToEncryptionKeyInfoKeyType(in_wrapped.key_type(), key_type);
    if (!status.ok()) return status;
    out_wrapped->set_key_type(key_type);
    out_wrapped->set_encrypted_dek(in_wrapped.encrypted_dek());
    out_wrapped->set_kek_kms_resource_id(in_wrapped.kek_kms_resource_id());

    if (in_wrapped.has_aws_wrapped_key_info()) {
      const auto& in_aws = in_wrapped.aws_wrapped_key_info();
      auto* out_aws = out_wrapped->mutable_aws_wrapped_key_info();
      out_aws->set_role_arn(in_aws.role_arn());
      *out_aws->mutable_signatures() = in_aws.signatures();
    } else if (in_wrapped.has_gcp_wrapped_key_info()) {
      const auto& in_gcp = in_wrapped.gcp_wrapped_key_info();
      auto* out_gcp = out_wrapped->mutable_gcp_wrapped_key_info();
      out_gcp->set_wip_provider(in_gcp.wip_provider());
    }
  } else if (input.has_coordinator_key()) {
    const auto& in_coord = input.coordinator_key();
    auto* out_coord = output.mutable_coordinator_key_info();
    out_coord->set_key_id(in_coord.key_id());
    for (const auto& in_info : in_coord.coordinator_info()) {
      auto* out_info = out_coord->add_coordinator_info();
      out_info->set_key_service_endpoint(in_info.key_service_endpoint());
      out_info->set_kms_wip_provider(in_info.kms_wip_provider());
      out_info->set_key_service_audience_url(
          in_info.key_service_audience_url());
    }
  }

  return absl::OkStatus();
}

absl::Status ToBackend(const EncryptionKeyInfo& input, EncryptionKey& output) {
  if (input.has_wrapped_key_info()) {
    const auto& in_wrapped = input.wrapped_key_info();
    auto* out_wrapped = output.mutable_wrapped_key();

    KeyType key_type;
    auto status = ToMatchServiceKeyType(in_wrapped.key_type(), key_type);
    if (!status.ok()) return status;
    out_wrapped->set_key_type(key_type);

    out_wrapped->set_encrypted_dek(in_wrapped.encrypted_dek());
    out_wrapped->set_kek_kms_resource_id(in_wrapped.kek_kms_resource_id());

    if (in_wrapped.has_aws_wrapped_key_info()) {
      const auto& in_aws = in_wrapped.aws_wrapped_key_info();
      auto* out_aws = out_wrapped->mutable_aws_wrapped_key_info();

      out_aws->set_role_arn(in_aws.role_arn());
      *out_aws->mutable_signatures() = in_aws.signatures();
    } else if (in_wrapped.has_gcp_wrapped_key_info()) {
      const auto& in_gcp = in_wrapped.gcp_wrapped_key_info();
      auto* out_gcp = out_wrapped->mutable_gcp_wrapped_key_info();

      out_gcp->set_wip_provider(in_gcp.wip_provider());
    }
  } else if (input.has_coordinator_key_info()) {
    const auto& in_coord = input.coordinator_key_info();
    auto* out_coord = output.mutable_coordinator_key();
    out_coord->set_key_id(in_coord.key_id());
    for (const auto& in_info : in_coord.coordinator_info()) {
      auto* out_info = out_coord->add_coordinator_info();
      out_info->set_key_service_endpoint(in_info.key_service_endpoint());
      out_info->set_kms_wip_provider(in_info.kms_wip_provider());
      out_info->set_key_service_audience_url(
          in_info.key_service_audience_url());
    }
  }

  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
