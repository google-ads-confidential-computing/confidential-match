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

#include "cc/match_service/converters/field_converters.h"

#include "absl/status/status.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/field_type_converters.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::Field& in, backend::Field& out) {
  out.Clear();
  backend::FieldType type;
  RETURN_IF_ERROR(ToBackend(in.type(), type));
  out.set_type(type);
  out.set_value(in.value());
  return absl::OkStatus();
}

absl::Status ToApi(const backend::Field& in, api::v1::Field& out) {
  out.Clear();
  api::v1::FieldType type;
  RETURN_IF_ERROR(ToApi(in.type(), type));
  out.set_type(type);
  out.set_value(in.value());
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::CompositeField& in,
                       backend::CompositeField& out) {
  out.Clear();
  backend::CompositeFieldType type;
  RETURN_IF_ERROR(ToBackend(in.type(), type));
  out.set_type(type);
  for (const auto& value : in.values()) {
    RETURN_IF_ERROR(ToBackend(value, *out.add_values()));
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::CompositeField& in,
                   api::v1::CompositeField& out) {
  out.Clear();
  api::v1::CompositeFieldType type;
  RETURN_IF_ERROR(ToApi(in.type(), type));
  out.set_type(type);
  for (const auto& value : in.values()) {
    RETURN_IF_ERROR(ToApi(value, *out.add_values()));
  }
  return absl::OkStatus();
}

absl::Status ToBackend(const api::v1::FanOutField& in,
                       backend::FanOutField& out) {
  out.Clear();
  backend::FanOutFieldType type;
  RETURN_IF_ERROR(ToBackend(in.type(), type));
  out.set_type(type);
  out.set_value(in.value());
  return absl::OkStatus();
}

absl::Status ToApi(const backend::FanOutField& in, api::v1::FanOutField& out) {
  out.Clear();
  api::v1::FanOutFieldType type;
  RETURN_IF_ERROR(ToApi(in.type(), type));
  out.set_type(type);
  out.set_value(in.value());
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
