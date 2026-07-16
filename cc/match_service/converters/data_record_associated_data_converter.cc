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

#include "cc/match_service/converters/data_record_associated_data_converter.h"

#include "absl/status/status.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::DataRecordAssociatedData& in,
                       backend::DataRecordAssociatedData& out) {
  out.Clear();
  out.set_selected_first_party_identifier(in.selected_first_party_identifier());
  return absl::OkStatus();
}

absl::Status ToApi(const backend::DataRecordAssociatedData& in,
                   api::v1::DataRecordAssociatedData& out) {
  out.Clear();
  out.set_selected_first_party_identifier(in.selected_first_party_identifier());
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
