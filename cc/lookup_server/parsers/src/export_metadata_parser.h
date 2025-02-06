/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CC_LOOKUP_SERVER_PARSERS_SRC_EXPORT_METADATA_PARSER_H_
#define CC_LOOKUP_SERVER_PARSERS_SRC_EXPORT_METADATA_PARSER_H_

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/export_metadata.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Parses an export metadata file from its string representation.
 *
 * @param raw_export_metadata the raw string representation of the file
 * @return an ExportMetadata proto or a FailureExecutionResult on error
 */
scp::core::ExecutionResultOr<proto_backend::ExportMetadata> ParseExportMetadata(
    absl::string_view raw_export_metadata);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_PARSERS_SRC_EXPORT_METADATA_PARSER_H_
