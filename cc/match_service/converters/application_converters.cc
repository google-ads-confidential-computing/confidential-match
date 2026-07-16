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

#include "cc/match_service/converters/application_converters.h"

#include "absl/status/status.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

absl::Status ToBackend(const api::v1::Application& in,
                       backend::Application& out) {
  switch (in) {
    case api::v1::APPLICATION_UNSPECIFIED:
      out = backend::APPLICATION_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::APPLICATION_ECL:
      out = backend::APPLICATION_ECL;
      return absl::OkStatus();
    case api::v1::APPLICATION_ONLINE_EC:
      out = backend::APPLICATION_ONLINE_EC;
      return absl::OkStatus();
    case api::v1::APPLICATION_VOYAGER:
      out = backend::APPLICATION_VOYAGER;
      return absl::OkStatus();
    default:
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          absl::StrCat("Failed to parse Application enum value: ", in));
  }
}

absl::Status ToApi(const backend::Application& in, api::v1::Application& out) {
  switch (in) {
    case backend::APPLICATION_UNSPECIFIED:
      out = api::v1::APPLICATION_UNSPECIFIED;
      return absl::OkStatus();
    case backend::APPLICATION_ECL:
      out = api::v1::APPLICATION_ECL;
      return absl::OkStatus();
    case backend::APPLICATION_ONLINE_EC:
      out = api::v1::APPLICATION_ONLINE_EC;
      return absl::OkStatus();
    case backend::APPLICATION_VOYAGER:
      out = api::v1::APPLICATION_VOYAGER;
      return absl::OkStatus();
    default:
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          absl::StrCat("Failed to parse Application enum value: ", in));
  }
}

}  // namespace google::confidential_match::match_service
