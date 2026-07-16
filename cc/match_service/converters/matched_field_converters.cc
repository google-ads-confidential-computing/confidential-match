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

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/error_reason_converters.h"
#include "cc/match_service/converters/field_type_converters.h"
#include "cc/match_service/converters/status_converters.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

// Converts an API GTagMetadata to the backend format.
absl::Status ToBackend(const api::v1::MatchedFieldInfo::GTagMetadata& in,
                       backend::MatchedFieldInfo::GTagMetadata& out) {
  out.Clear();
  out.set_index(in.index());
  out.set_error_code(in.error_code());
  return absl::OkStatus();
}

// Converts a backend GTagMetadata to the API format.
absl::Status ToApi(const backend::MatchedFieldInfo::GTagMetadata& in,
                   api::v1::MatchedFieldInfo::GTagMetadata& out) {
  out.Clear();
  out.set_index(in.index());
  out.set_error_code(in.error_code());
  return absl::OkStatus();
}

}  // namespace

absl::Status ToBackend(const api::v1::MatchedField& in,
                       backend::MatchedField& out) {
  out.Clear();

  backend::Status status;
  RETURN_IF_ERROR(ToBackend(in.status(), status));
  out.set_status(status);

  if (in.has_error_reason()) {
    backend::ErrorReason error_reason;
    RETURN_IF_ERROR(ToBackend(in.error_reason(), error_reason));
    out.set_error_reason(error_reason);
  }

  if (in.has_matched_field_info()) {
    RETURN_IF_ERROR(
        ToBackend(in.matched_field_info(), *out.mutable_matched_field_info()));
  }

  for (int64_t index : in.matched_associated_data_indices()) {
    out.add_matched_associated_data_indices(index);
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchedField& in,
                   api::v1::MatchedField& out) {
  out.Clear();

  api::v1::Status status;
  RETURN_IF_ERROR(ToApi(in.status(), status));
  out.set_status(status);

  if (in.has_error_reason()) {
    api::v1::ErrorReason error_reason;
    RETURN_IF_ERROR(ToApi(in.error_reason(), error_reason));
    out.set_error_reason(error_reason);
  }

  if (in.has_matched_field_info()) {
    RETURN_IF_ERROR(
        ToApi(in.matched_field_info(), *out.mutable_matched_field_info()));
  }

  for (int64_t index : in.matched_associated_data_indices()) {
    out.add_matched_associated_data_indices(index);
  }

  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::MatchedCompositeField& in,
                       backend::MatchedCompositeField& out) {
  out.Clear();

  backend::Status status;
  RETURN_IF_ERROR(ToBackend(in.status(), status));
  out.set_status(status);

  if (in.has_error_reason()) {
    backend::ErrorReason error_reason;
    RETURN_IF_ERROR(ToBackend(in.error_reason(), error_reason));
    out.set_error_reason(error_reason);
  }

  for (const auto& info : in.matched_field_info()) {
    RETURN_IF_ERROR(ToBackend(info, *out.add_matched_field_info()));
  }

  for (const auto& index : in.matched_associated_data_indices()) {
    out.add_matched_associated_data_indices(index);
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchedCompositeField& in,
                   api::v1::MatchedCompositeField& out) {
  out.Clear();

  api::v1::Status status;
  RETURN_IF_ERROR(ToApi(in.status(), status));
  out.set_status(status);

  if (in.has_error_reason()) {
    api::v1::ErrorReason error_reason;
    RETURN_IF_ERROR(ToApi(in.error_reason(), error_reason));
    out.set_error_reason(error_reason);
  }

  for (const auto& info : in.matched_field_info()) {
    RETURN_IF_ERROR(ToApi(info, *out.add_matched_field_info()));
  }

  for (const auto& index : in.matched_associated_data_indices()) {
    out.add_matched_associated_data_indices(index);
  }

  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::MatchedFanOutField& in,
                       backend::MatchedFanOutField& out) {
  out.Clear();

  backend::Status status;
  RETURN_IF_ERROR(ToBackend(in.status(), status));
  out.set_status(status);

  if (in.has_error_reason()) {
    backend::ErrorReason error_reason;
    RETURN_IF_ERROR(ToBackend(in.error_reason(), error_reason));
    out.set_error_reason(error_reason);
  }

  for (const auto& fanned_out_key : in.fanned_out_keys()) {
    RETURN_IF_ERROR(ToBackend(fanned_out_key, *out.add_fanned_out_keys()));
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchedFanOutField& in,
                   api::v1::MatchedFanOutField& out) {
  out.Clear();

  api::v1::Status status;
  RETURN_IF_ERROR(ToApi(in.status(), status));
  out.set_status(status);

  if (in.has_error_reason()) {
    api::v1::ErrorReason error_reason;
    RETURN_IF_ERROR(ToApi(in.error_reason(), error_reason));
    out.set_error_reason(error_reason);
  }

  for (const auto& fanned_out_key : in.fanned_out_keys()) {
    RETURN_IF_ERROR(ToApi(fanned_out_key, *out.add_fanned_out_keys()));
  }

  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::FannedOutMatchedKey& in,
                       backend::FannedOutMatchedKey& out) {
  out.Clear();
  switch (in.field_info_case()) {
    case api::v1::FannedOutMatchedKey::kField:
      RETURN_IF_ERROR(ToBackend(in.field(), *out.mutable_field()));
      break;
    case api::v1::FannedOutMatchedKey::kCompositeField:
      RETURN_IF_ERROR(
          ToBackend(in.composite_field(), *out.mutable_composite_field()));
      break;
    case api::v1::FannedOutMatchedKey::FIELD_INFO_NOT_SET:
      break;
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::FannedOutMatchedKey& in,
                   api::v1::FannedOutMatchedKey& out) {
  out.Clear();
  switch (in.field_info_case()) {
    case backend::FannedOutMatchedKey::kField:
      RETURN_IF_ERROR(ToApi(in.field(), *out.mutable_field()));
      break;
    case backend::FannedOutMatchedKey::kCompositeField:
      RETURN_IF_ERROR(
          ToApi(in.composite_field(), *out.mutable_composite_field()));
      break;
    case backend::FannedOutMatchedKey::FIELD_INFO_NOT_SET:
      break;
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::MatchedFieldInfo& in,
                       backend::MatchedFieldInfo& out) {
  out.Clear();

  backend::FieldType field_type;
  RETURN_IF_ERROR(ToBackend(in.field_type(), field_type));
  out.set_field_type(field_type);

  out.set_field_value(in.field_value());

  if (in.has_gtag_metadata()) {
    RETURN_IF_ERROR(
        ToBackend(in.gtag_metadata(), *out.mutable_gtag_metadata()));
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchedFieldInfo& in,
                   api::v1::MatchedFieldInfo& out) {
  out.Clear();

  api::v1::FieldType field_type;
  RETURN_IF_ERROR(ToApi(in.field_type(), field_type));
  out.set_field_type(field_type);

  out.set_field_value(in.field_value());

  if (in.has_gtag_metadata()) {
    RETURN_IF_ERROR(ToApi(in.gtag_metadata(), *out.mutable_gtag_metadata()));
  }

  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
