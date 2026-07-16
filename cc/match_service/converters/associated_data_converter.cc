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

#include "cc/match_service/converters/associated_data_converter.h"

#include "absl/status/status.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/converters/first_party_identifier_converters.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

absl::Status ToBackend(const api::v1::AssociatedData& in,
                       backend::AssociatedData& out) {
  out.Clear();
  if (in.has_first_party_identifier()) {
    RETURN_IF_ERROR(ToBackend(in.first_party_identifier(),
                              *out.mutable_first_party_identifier()));
  }
  return absl::OkStatus();
}

absl::Status ToApi(const backend::AssociatedData& in,
                   api::v1::AssociatedData& out) {
  out.Clear();
  if (in.has_first_party_identifier()) {
    RETURN_IF_ERROR(ToApi(in.first_party_identifier(),
                          *out.mutable_first_party_identifier()));
  }
  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
