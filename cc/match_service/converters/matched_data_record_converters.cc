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

#include "cc/match_service/converters/matched_data_record_converters.h"

#include "absl/status/status.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/data_record_associated_data_converter.h"
#include "cc/match_service/converters/key_value_converters.h"
#include "cc/match_service/converters/matched_key_converters.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::MatchedDataRecord& in,
                       backend::MatchedDataRecord& out) {
  out.Clear();

  for (const auto& matched_key : in.matched_keys()) {
    RETURN_IF_ERROR(ToBackend(matched_key, *out.add_matched_keys()));
  }

  for (const auto& metadata : in.metadata()) {
    RETURN_IF_ERROR(ToBackend(metadata, *out.add_metadata()));
  }

  if (in.has_data_record_associated_data()) {
    RETURN_IF_ERROR(ToBackend(in.data_record_associated_data(),
                              *out.mutable_data_record_associated_data()));
  }

  return absl::OkStatus();
}

absl::Status ToApi(const backend::MatchedDataRecord& in,
                   api::v1::MatchedDataRecord& out) {
  out.Clear();

  for (const auto& matched_key : in.matched_keys()) {
    RETURN_IF_ERROR(ToApi(matched_key, *out.add_matched_keys()));
  }

  for (const auto& metadata : in.metadata()) {
    RETURN_IF_ERROR(ToApi(metadata, *out.add_metadata()));
  }

  if (in.has_data_record_associated_data()) {
    RETURN_IF_ERROR(ToApi(in.data_record_associated_data(),
                          *out.mutable_data_record_associated_data()));
  }

  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
