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

#include "cc/lookup_server/parsers/src/export_metadata_parser.h"

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "google/protobuf/util/json_util.h"

#include "cc/lookup_server/parsers/src/error_codes.h"
#include "protos/lookup_server/backend/export_metadata.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_backend::
    ExportMetadata;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

ExecutionResultOr<ExportMetadata> ParseExportMetadata(
    absl::string_view raw_export_metadata) {
  if (raw_export_metadata.empty()) {
    return FailureExecutionResult(PARSER_INVALID_EXPORT_METADATA);
  }

  ExportMetadata export_metadata;
  if (!JsonStringToMessage(raw_export_metadata, &export_metadata).ok()) {
    return FailureExecutionResult(PARSER_INVALID_EXPORT_METADATA);
  }
  return export_metadata;
}

}  // namespace google::confidential_match::lookup_server
