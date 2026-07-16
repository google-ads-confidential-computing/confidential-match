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

#include "cc/match_service/converters/key_value_converters.h"

#include "absl/status/status.h"

#include "cc/match_service/error/error.h"
#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::Error;
}

absl::Status ToBackend(const api::v1::KeyValue& in, backend::KeyValue& out) {
  out.Clear();

  out.set_key(in.key());

  switch (in.value_case()) {
    case api::v1::KeyValue::kStringValue:
      out.set_string_value(in.string_value());
      return absl::OkStatus();
    case api::v1::KeyValue::kIntValue:
      out.set_int_value(in.int_value());
      return absl::OkStatus();
    case api::v1::KeyValue::kDoubleValue:
      out.set_double_value(in.double_value());
      return absl::OkStatus();
    case api::v1::KeyValue::kBoolValue:
      out.set_bool_value(in.bool_value());
      return absl::OkStatus();
    case api::v1::KeyValue::kBytesValue:
      out.set_bytes_value(in.bytes_value());
      return absl::OkStatus();
    case api::v1::KeyValue::VALUE_NOT_SET:
      return absl::OkStatus();
  }
  // Exhaustive switch, should never reach here.
  return Status(Error::CONVERTER_PARSE_ERROR,
                absl::StrCat("Failed to parse KeyValuePair value_case: ",
                             in.value_case()));
}

absl::Status ToApi(const backend::KeyValue& in, api::v1::KeyValue& out) {
  // Clear the output to ensure no stale data remains
  out.Clear();

  out.set_key(in.key());

  switch (in.value_case()) {
    case backend::KeyValue::kStringValue:
      out.set_string_value(in.string_value());
      return absl::OkStatus();
    case backend::KeyValue::kIntValue:
      out.set_int_value(in.int_value());
      return absl::OkStatus();
    case backend::KeyValue::kDoubleValue:
      out.set_double_value(in.double_value());
      return absl::OkStatus();
    case backend::KeyValue::kBoolValue:
      out.set_bool_value(in.bool_value());
      return absl::OkStatus();
    case backend::KeyValue::kBytesValue:
      out.set_bytes_value(in.bytes_value());
      return absl::OkStatus();
    case backend::KeyValue::VALUE_NOT_SET:
      // out was already cleared, so value is unset.
      return absl::OkStatus();
  }
  // Exhaustive switch, should never reach here.
  return Status(Error::CONVERTER_PARSE_ERROR,
                absl::StrCat("Failed to parse KeyValuePair value_case: ",
                             in.value_case()));
}

}  // namespace google::confidential_match::match_service
