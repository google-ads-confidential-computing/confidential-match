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

#include "cc/match_service/converters/field_type_converters.h"

#include "absl/status/status.h"
#include "cc/match_service/error/error.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

absl::Status ToBackend(const api::v1::FieldType& in, backend::FieldType& out) {
  switch (in) {
    case api::v1::FIELD_TYPE_UNSPECIFIED:
      out = backend::FIELD_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::FIELD_TYPE_EMAIL:
      out = backend::FIELD_TYPE_EMAIL;
      return absl::OkStatus();
    case api::v1::FIELD_TYPE_PHONE:
      out = backend::FIELD_TYPE_PHONE;
      return absl::OkStatus();
    case api::v1::FIELD_TYPE_FIRST_NAME:
      out = backend::FIELD_TYPE_FIRST_NAME;
      return absl::OkStatus();
    case api::v1::FIELD_TYPE_LAST_NAME:
      out = backend::FIELD_TYPE_LAST_NAME;
      return absl::OkStatus();
    case api::v1::FIELD_TYPE_COUNTRY_CODE:
      out = backend::FIELD_TYPE_COUNTRY_CODE;
      return absl::OkStatus();
    case api::v1::FIELD_TYPE_ZIP_CODE:
      out = backend::FIELD_TYPE_ZIP_CODE;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse FieldType: ", in));
  }
}

absl::Status ToApi(const backend::FieldType& in, api::v1::FieldType& out) {
  switch (in) {
    case backend::FIELD_TYPE_UNSPECIFIED:
      out = api::v1::FIELD_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case backend::FIELD_TYPE_EMAIL:
      out = api::v1::FIELD_TYPE_EMAIL;
      return absl::OkStatus();
    case backend::FIELD_TYPE_PHONE:
      out = api::v1::FIELD_TYPE_PHONE;
      return absl::OkStatus();
    case backend::FIELD_TYPE_FIRST_NAME:
      out = api::v1::FIELD_TYPE_FIRST_NAME;
      return absl::OkStatus();
    case backend::FIELD_TYPE_LAST_NAME:
      out = api::v1::FIELD_TYPE_LAST_NAME;
      return absl::OkStatus();
    case backend::FIELD_TYPE_COUNTRY_CODE:
      out = api::v1::FIELD_TYPE_COUNTRY_CODE;
      return absl::OkStatus();
    case backend::FIELD_TYPE_ZIP_CODE:
      out = api::v1::FIELD_TYPE_ZIP_CODE;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse FieldType: ", in));
  }
}

absl::Status ToBackend(const api::v1::CompositeFieldType& in,
                       backend::CompositeFieldType& out) {
  switch (in) {
    case api::v1::COMPOSITE_FIELD_TYPE_UNSPECIFIED:
      out = backend::COMPOSITE_FIELD_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::COMPOSITE_FIELD_TYPE_ADDRESS:
      out = backend::COMPOSITE_FIELD_TYPE_ADDRESS;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse CompositeFieldType: ", in));
  }
}

absl::Status ToApi(const backend::CompositeFieldType& in,
                   api::v1::CompositeFieldType& out) {
  switch (in) {
    case backend::COMPOSITE_FIELD_TYPE_UNSPECIFIED:
      out = api::v1::COMPOSITE_FIELD_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case backend::COMPOSITE_FIELD_TYPE_ADDRESS:
      out = api::v1::COMPOSITE_FIELD_TYPE_ADDRESS;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse CompositeFieldType: ", in));
  }
}

absl::Status ToBackend(const api::v1::FanOutFieldType& in,
                       backend::FanOutFieldType& out) {
  switch (in) {
    case api::v1::FAN_OUT_FIELD_TYPE_UNSPECIFIED:
      out = backend::FAN_OUT_FIELD_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case api::v1::FAN_OUT_FIELD_TYPE_GTAG:
      out = backend::FAN_OUT_FIELD_TYPE_GTAG;
      return absl::OkStatus();
    case api::v1::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG:
      out = backend::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse FanOutFieldType: ", in));
  }
}

absl::Status ToApi(const backend::FanOutFieldType& in,
                   api::v1::FanOutFieldType& out) {
  switch (in) {
    case backend::FAN_OUT_FIELD_TYPE_UNSPECIFIED:
      out = api::v1::FAN_OUT_FIELD_TYPE_UNSPECIFIED;
      return absl::OkStatus();
    case backend::FAN_OUT_FIELD_TYPE_GTAG:
      out = api::v1::FAN_OUT_FIELD_TYPE_GTAG;
      return absl::OkStatus();
    case backend::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG:
      out = api::v1::FAN_OUT_FIELD_TYPE_ENCRYPTED_GTAG;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse FanOutFieldType: ", in));
  }
}

}  // namespace google::confidential_match::match_service
