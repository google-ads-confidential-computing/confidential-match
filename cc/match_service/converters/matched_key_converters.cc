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

#include "cc/match_service/converters/matched_key_converters.h"

#include "absl/status/status.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/key_value_converters.h"
#include "cc/match_service/converters/matched_field_converters.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::MatchedKey& in,
                       backend::MatchedKey& out) {
  out.Clear();

  switch (in.field_info_case()) {
    case api::v1::MatchedKey::kField:
      RETURN_IF_ERROR(ToBackend(in.field(), *out.mutable_field()));
      break;
    case api::v1::MatchedKey::kCompositeField:
      RETURN_IF_ERROR(
          ToBackend(in.composite_field(), *out.mutable_composite_field()));
      break;
    case api::v1::MatchedKey::kFanOutKeyField:
      RETURN_IF_ERROR(
          ToBackend(in.fan_out_key_field(), *out.mutable_fan_out_key_field()));
      break;
    case api::v1::MatchedKey::FIELD_INFO_NOT_SET:
      break;
  }

  for (const auto& metadata : in.metadata()) {
    RETURN_IF_ERROR(ToBackend(metadata, *out.add_metadata()));
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchedKey& in, api::v1::MatchedKey& out) {
  out.Clear();

  switch (in.field_info_case()) {
    case backend::MatchedKey::kField:
      RETURN_IF_ERROR(ToApi(in.field(), *out.mutable_field()));
      break;
    case backend::MatchedKey::kCompositeField:
      RETURN_IF_ERROR(
          ToApi(in.composite_field(), *out.mutable_composite_field()));
      break;
    case backend::MatchedKey::kFanOutKeyField:
      RETURN_IF_ERROR(
          ToApi(in.fan_out_key_field(), *out.mutable_fan_out_key_field()));
      break;
    case backend::MatchedKey::FIELD_INFO_NOT_SET:
      break;
  }

  for (const auto& metadata : in.metadata()) {
    RETURN_IF_ERROR(ToApi(metadata, *out.add_metadata()));
  }

  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
