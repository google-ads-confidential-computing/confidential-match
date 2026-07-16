// Copyright 2026 Google LLC
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

#include "cc/match_service/validators/match_key_encoding_validator.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

namespace {

absl::Status ValidateBase64(absl::string_view match_key) {
  std::string decoded;
  if (!absl::Base64Unescape(match_key, &decoded)) {
    return Status(Error::INVALID_MATCH_KEY_ENCODING,
                  "Match key is not properly encoded as base-64.");
  }
  return absl::OkStatus();
}

absl::Status ValidateBase64WebSafe(absl::string_view match_key) {
  std::string decoded;
  if (!absl::WebSafeBase64Unescape(match_key, &decoded)) {
    return Status(Error::INVALID_MATCH_KEY_ENCODING,
                  "Match key is not properly encoded as web-safe base-64.");
  }
  return absl::OkStatus();
}

absl::Status ValidateHex(absl::string_view match_key) {
  std::string decoded;
  if (!absl::HexStringToBytes(match_key, &decoded)) {
    return Status(Error::INVALID_MATCH_KEY_ENCODING,
                  "Match key is not properly encoded as hex.");
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ValidateMatchKeyEncoding(backend::MatchKeyEncoding encoding,
                                      absl::string_view match_key) {
  switch (encoding) {
    case backend::MATCH_KEY_ENCODING_UNSPECIFIED:
      return Status(Error::INVALID_MATCH_KEY_ENCODING,
                    "Match key encoding must be specified.");
    case backend::MATCH_KEY_ENCODING_BASE64:
      return ValidateBase64(match_key);
    case backend::MATCH_KEY_ENCODING_BASE64_WEB_SAFE:
      return ValidateBase64WebSafe(match_key);
    case backend::MATCH_KEY_ENCODING_HEX:
      return ValidateHex(match_key);
    default:
      return Status(Error::INTERNAL_ERROR,
                    "Got unexpected match key encoding.");
  }
}

}  // namespace google::confidential_match::match_service
