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

#include "cc/match_service/converters/match_key_encoding_converters.h"

#include "absl/status/status.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

absl::Status ToBackend(const api::v1::MatchKeyEncoding& in,
                       backend::MatchKeyEncoding& out) {
  switch (in) {
    case api::v1::MATCH_KEY_ENCODING_UNSPECIFIED:
      out = backend::MATCH_KEY_ENCODING_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::MATCH_KEY_ENCODING_BASE64:
      out = backend::MATCH_KEY_ENCODING_BASE64;
      return absl::OkStatus();
    case api::v1::MATCH_KEY_ENCODING_BASE64_WEB_SAFE:
      out = backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE;
      return absl::OkStatus();
    case api::v1::MATCH_KEY_ENCODING_HEX:
      out = backend::MATCH_KEY_ENCODING_HEX;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse MatchKeyEncoding: ", in));
  }
}

absl::Status ToApi(const backend::MatchKeyEncoding& in,
                   api::v1::MatchKeyEncoding& out) {
  switch (in) {
    case backend::MATCH_KEY_ENCODING_UNSPECIFIED:
      out = api::v1::MATCH_KEY_ENCODING_UNSPECIFIED;
      return absl::OkStatus();
    case backend::MATCH_KEY_ENCODING_BASE64:
      out = api::v1::MATCH_KEY_ENCODING_BASE64;
      return absl::OkStatus();
    case backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE:
      out = api::v1::MATCH_KEY_ENCODING_BASE64_WEB_SAFE;
      return absl::OkStatus();
    case backend::MATCH_KEY_ENCODING_HEX:
      out = api::v1::MATCH_KEY_ENCODING_HEX;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse MatchKeyEncoding: ", in));
  }
}

}  // namespace google::confidential_match::match_service
