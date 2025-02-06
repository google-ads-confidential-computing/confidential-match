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

#include "cc/lookup_server/converters/src/key_value_converter.h"

#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/converters/src/error_codes.h"
#include "protos/core/data_value.pb.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/exporter_data_row.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_backend::
    ExporterAssociatedData_KeyValue;
using ::google::confidential_match::lookup_server::proto_backend::
    ExporterDataRow;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
namespace proto_core = ::google::confidential_match;

constexpr absl::string_view kComponentName = "KeyValueConverter";

// Converts a DataValue key-value pair to an ExporterAssociatedData key-value
// pair.
ExecutionResult ConvertKeyValue(const proto_core::KeyValue& kv,
                                ExporterAssociatedData_KeyValue& out) {
  *out.mutable_key() = kv.key();

  switch (kv.value_case()) {
    case proto_core::KeyValue::kStringValue:
      *out.mutable_string_value() = kv.string_value();
      return SuccessExecutionResult();

    case proto_core::KeyValue::kIntValue:
      out.set_int_value(kv.int_value());
      return SuccessExecutionResult();

    case proto_core::KeyValue::kDoubleValue:
      out.set_double_value(kv.double_value());
      return SuccessExecutionResult();

    case proto_core::KeyValue::kBoolValue:
      out.set_bool_value(kv.bool_value());
      return SuccessExecutionResult();

    case proto_core::KeyValue::kBytesValue:
      *out.mutable_bytes_value() = kv.bytes_value();
      return SuccessExecutionResult();

    case proto_core::KeyValue::VALUE_NOT_SET:
      return SuccessExecutionResult();
  }

  // Exhaustive switch, should never reach here
  ExecutionResult error = FailureExecutionResult(CONVERTER_PARSE_ERROR);
  SCP_ERROR(kComponentName, kZeroUuid, error,
            absl::StrFormat("Got an unknown value case for DataValue KV: %d",
                            static_cast<int>(kv.value_case())));
  return error;
}

// Converts an ExporterAssociatedData key-value pair to a DataValue key-value
// pair.
ExecutionResult ConvertKeyValue(const ExporterAssociatedData_KeyValue& kv,
                                proto_core::KeyValue& out) {
  *out.mutable_key() = kv.key();

  switch (kv.value_case()) {
    case ExporterAssociatedData_KeyValue::kStringValue:
      *out.mutable_string_value() = kv.string_value();
      return SuccessExecutionResult();

    case ExporterAssociatedData_KeyValue::kIntValue:
      out.set_int_value(kv.int_value());
      return SuccessExecutionResult();

    case ExporterAssociatedData_KeyValue::kDoubleValue:
      out.set_double_value(kv.double_value());
      return SuccessExecutionResult();

    case ExporterAssociatedData_KeyValue::kBoolValue:
      out.set_bool_value(kv.bool_value());
      return SuccessExecutionResult();

    case ExporterAssociatedData_KeyValue::kBytesValue:
      *out.mutable_bytes_value() = kv.bytes_value();
      return SuccessExecutionResult();

    case ExporterAssociatedData_KeyValue::VALUE_NOT_SET:
      return SuccessExecutionResult();
  }

  // Exhaustive switch, should never reach here
  ExecutionResult error = FailureExecutionResult(CONVERTER_PARSE_ERROR);
  SCP_ERROR(
      kComponentName, kZeroUuid, error,
      absl::StrFormat("Got an unknown value case for exporter data row KV: %d",
                      static_cast<int>(kv.value_case())));
  return error;
}

ExecutionResult ConvertKeyValue(const proto_api::KeyValue& kv,
                                proto_core::KeyValue& out) {
  *out.mutable_key() = kv.key();

  switch (kv.value_case()) {
    case proto_api::KeyValue::kStringValue:
      *out.mutable_string_value() = kv.string_value();
      return SuccessExecutionResult();

    case proto_api::KeyValue::kIntValue:
      out.set_int_value(kv.int_value());
      return SuccessExecutionResult();

    case proto_api::KeyValue::kDoubleValue:
      out.set_double_value(kv.double_value());
      return SuccessExecutionResult();

    case proto_api::KeyValue::kBoolValue:
      out.set_bool_value(kv.bool_value());
      return SuccessExecutionResult();

    case proto_api::KeyValue::kBytesValue:
      *out.mutable_bytes_value() = kv.bytes_value();
      return SuccessExecutionResult();

    case proto_api::KeyValue::VALUE_NOT_SET:
      return SuccessExecutionResult();
  }

  // Exhaustive switch, should never reach here
  ExecutionResult error = FailureExecutionResult(CONVERTER_PARSE_ERROR);
  SCP_ERROR(kComponentName, kZeroUuid, error,
            absl::StrFormat("Got an unknown value case for API KV: %d",
                            static_cast<int>(kv.value_case())));
  return error;
}

ExecutionResult ConvertKeyValue(const proto_core::KeyValue& kv,
                                proto_api::KeyValue& out) {
  *out.mutable_key() = kv.key();

  switch (kv.value_case()) {
    case proto_core::KeyValue::kStringValue:
      *out.mutable_string_value() = kv.string_value();
      return SuccessExecutionResult();

    case proto_core::KeyValue::kIntValue:
      out.set_int_value(kv.int_value());
      return SuccessExecutionResult();

    case proto_core::KeyValue::kDoubleValue:
      out.set_double_value(kv.double_value());
      return SuccessExecutionResult();

    case proto_core::KeyValue::kBoolValue:
      out.set_bool_value(kv.bool_value());
      return SuccessExecutionResult();

    case proto_core::KeyValue::kBytesValue:
      *out.mutable_bytes_value() = kv.bytes_value();
      return SuccessExecutionResult();

    case proto_core::KeyValue::VALUE_NOT_SET:
      return SuccessExecutionResult();
  }

  // Exhaustive switch, should never reach here
  ExecutionResult error = FailureExecutionResult(CONVERTER_PARSE_ERROR);
  SCP_ERROR(kComponentName, kZeroUuid, error,
            absl::StrFormat("Got an unknown value case for core backend KV: %d",
                            static_cast<int>(kv.value_case())));
  return error;
}

}  // namespace google::confidential_match::lookup_server
