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

#include "cc/match_service/converters/match_request_converters.h"

#include "absl/status/status.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/application_converters.h"
#include "cc/match_service/converters/associated_data_type_converters.h"
#include "cc/match_service/converters/data_record_converters.h"
#include "cc/match_service/converters/encryption_key_converters.h"
#include "cc/match_service/converters/key_value_converters.h"
#include "cc/match_service/converters/match_key_encoding_converters.h"
#include "cc/match_service/converters/match_key_format_converters.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::MatchRequest& in,
                       backend::MatchRequest& out) {
  out.Clear();

  backend::Application application;
  RETURN_IF_ERROR(ToBackend(in.application(), application));
  out.set_application(application);

  for (const auto& data_record : in.data_records()) {
    RETURN_IF_ERROR(ToBackend(data_record, *out.add_data_records()));
  }

  backend::MatchKeyFormat match_key_format;
  RETURN_IF_ERROR(ToBackend(in.match_key_format(), match_key_format));
  out.set_match_key_format(match_key_format);

  backend::MatchKeyEncoding key_encoding;
  RETURN_IF_ERROR(ToBackend(in.key_encoding(), key_encoding));
  out.set_key_encoding(key_encoding);

  if (in.has_encryption_key()) {
    RETURN_IF_ERROR(
        ToBackend(in.encryption_key(), *out.mutable_encryption_key()));
  }

  for (const auto& associated_data_type : in.associated_data_types()) {
    backend::AssociatedDataType temp;
    RETURN_IF_ERROR(ToBackend(
        static_cast<api::v1::AssociatedDataType>(associated_data_type), temp));
    out.add_associated_data_types(temp);
  }

  for (const auto& metadata : in.metadata()) {
    RETURN_IF_ERROR(ToBackend(metadata, *out.add_metadata()));
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchRequest& in,
                   api::v1::MatchRequest& out) {
  out.Clear();

  api::v1::Application application;
  RETURN_IF_ERROR(ToApi(in.application(), application));
  out.set_application(application);

  for (const auto& data_record : in.data_records()) {
    RETURN_IF_ERROR(ToApi(data_record, *out.add_data_records()));
  }

  api::v1::MatchKeyFormat match_key_format;
  RETURN_IF_ERROR(ToApi(in.match_key_format(), match_key_format));
  out.set_match_key_format(match_key_format);

  api::v1::MatchKeyEncoding key_encoding;
  RETURN_IF_ERROR(ToApi(in.key_encoding(), key_encoding));
  out.set_key_encoding(key_encoding);

  if (in.has_encryption_key()) {
    RETURN_IF_ERROR(ToApi(in.encryption_key(), *out.mutable_encryption_key()));
  }

  for (const auto& associated_data_type : in.associated_data_types()) {
    api::v1::AssociatedDataType temp;
    RETURN_IF_ERROR(ToApi(
        static_cast<backend::AssociatedDataType>(associated_data_type), temp));
    out.add_associated_data_types(temp);
  }

  for (const auto& metadata : in.metadata()) {
    RETURN_IF_ERROR(ToApi(metadata, *out.add_metadata()));
  }

  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
