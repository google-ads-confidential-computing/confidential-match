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

#include "cc/match_service/converters/match_key_format_converters.h"

#include "absl/status/status.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

absl::Status ToBackend(const api::v1::MatchKeyFormat& in,
                       backend::MatchKeyFormat& out) {
  switch (in) {
    case api::v1::MATCH_KEY_FORMAT_UNSPECIFIED:
      out = backend::MATCH_KEY_FORMAT_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::MATCH_KEY_FORMAT_HASHED:
      out = backend::MATCH_KEY_FORMAT_HASHED;
      return absl::OkStatus();
    case api::v1::MATCH_KEY_FORMAT_HASHED_ENCRYPTED:
      out = backend::MATCH_KEY_FORMAT_HASHED_ENCRYPTED;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse MatchKeyFormat: ", in));
  }
}

absl::Status ToApi(const backend::MatchKeyFormat& in,
                   api::v1::MatchKeyFormat& out) {
  switch (in) {
    case backend::MATCH_KEY_FORMAT_UNSPECIFIED:
      out = api::v1::MATCH_KEY_FORMAT_UNSPECIFIED;
      return absl::OkStatus();
    case backend::MATCH_KEY_FORMAT_HASHED:
      out = api::v1::MATCH_KEY_FORMAT_HASHED;
      return absl::OkStatus();
    case backend::MATCH_KEY_FORMAT_HASHED_ENCRYPTED:
      out = api::v1::MATCH_KEY_FORMAT_HASHED_ENCRYPTED;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse MatchKeyFormat: ", in));
  }
}

}  // namespace google::confidential_match::match_service
