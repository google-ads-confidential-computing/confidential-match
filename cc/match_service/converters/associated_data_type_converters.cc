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

#include "cc/match_service/converters/associated_data_type_converters.h"

#include "absl/status/status.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

absl::Status ToBackend(const api::v1::AssociatedDataType& in,
                       backend::AssociatedDataType& out) {
  switch (in) {
    case api::v1::ASSOCIATED_DATA_TYPE_UNSPECIFIED:
      out = backend::ASSOCIATED_DATA_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER:
      out = backend::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER;
      return absl::OkStatus();
    case api::v1::ASSOCIATED_DATA_TYPE_PRIVACY_INFO:
      out = backend::ASSOCIATED_DATA_TYPE_PRIVACY_INFO;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse AssociatedDataType: ", in));
  }
}

absl::Status ToApi(const backend::AssociatedDataType& in,
                   api::v1::AssociatedDataType& out) {
  switch (in) {
    case backend::ASSOCIATED_DATA_TYPE_UNSPECIFIED:
      out = api::v1::ASSOCIATED_DATA_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case backend::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER:
      out = api::v1::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER;
      return absl::OkStatus();
    case backend::ASSOCIATED_DATA_TYPE_PRIVACY_INFO:
      out = api::v1::ASSOCIATED_DATA_TYPE_PRIVACY_INFO;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse AssociatedDataType: ", in));
  }
}

}  // namespace google::confidential_match::match_service
