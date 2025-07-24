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

#include "cc/lookup_server/parsers/src/match_data_file_parser.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "cc/core/common/global_logger/src/global_logger.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"

#include "cc/lookup_server/converters/src/key_value_converter.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/parsers/src/error_codes.h"
#include "cc/lookup_server/types/match_data_group.h"
#include "protos/lookup_server/backend/exporter_data_row.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    ExporterAssociatedData;
using ::google::confidential_match::lookup_server::proto_backend::
    ExporterAssociatedData_KeyValue;
using ::google::confidential_match::lookup_server::proto_backend::
    ExporterDataRow;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;

constexpr absl::string_view kComponentName = "MatchDataFileParser";
constexpr char kDelimiter = '\n';

// Builds a MatchDataGroup from an ExporterDataRow, appending to the output
// vector. Returns a result indicating whether the operation was successful.
ExecutionResult BuildMatchDataGroup(const ExporterDataRow& exporter_data_row,
                                    std::vector<MatchDataGroup>& out) {
  MatchDataGroup match_data_group;

  for (const ExporterAssociatedData& associated_data :
       exporter_data_row.associated_data()) {
    MatchDataRow match_data_row;
    match_data_row.set_key(exporter_data_row.data_key());
    for (const ExporterAssociatedData_KeyValue& kv :
         associated_data.metadata()) {
      RETURN_IF_FAILURE(
          ConvertKeyValue(kv, *match_data_row.add_associated_data()));
    }
    match_data_group.push_back(std::move(match_data_row));
  }

  out.push_back(std::move(match_data_group));
  return SuccessExecutionResult();
}

// Parses raw encrypted group data into a match data group object, appending
// to the output vector.
ExecutionResult ParseMatchDataGroup(
    absl::string_view raw_group_data,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key,
    std::vector<MatchDataGroup>& out) noexcept {
  std::string encrypted_bytes;
  if (!absl::Base64Unescape(raw_group_data, &encrypted_bytes)) {
    return FailureExecutionResult(PARSER_INVALID_BASE64_DATA);
  }

  ExecutionResultOr<std::string> decrypted_string_or =
      data_encryption_key->Decrypt(encrypted_bytes);
  if (!decrypted_string_or.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, decrypted_string_or.result(),
              "Failed to decrypt match data row.");
    return FailureExecutionResult(PARSER_DATA_DECRYPTION_FAILED);
  }

  ExporterDataRow exporter_data_row;
  if (!exporter_data_row.ParseFromString(*decrypted_string_or)) {
    return FailureExecutionResult(PARSER_INVALID_PROTO_DATA);
  }

  return BuildMatchDataGroup(exporter_data_row, out);
}

}  // namespace

ExecutionResult ParseMatchDataFile(
    absl::string_view file_contents,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key,
    std::vector<MatchDataGroup>& out) noexcept {
  size_t start = 0;
  while (start < file_contents.length()) {
    size_t delimiter_pos = file_contents.find(kDelimiter, start);
    if (delimiter_pos == std::string::npos) {
      // Process the final line, which may not be terminated with a delimiter
      RETURN_IF_FAILURE(ParseMatchDataGroup(file_contents.substr(start),
                                            data_encryption_key, out));
      break;
    }

    RETURN_IF_FAILURE(
        ParseMatchDataGroup(file_contents.substr(start, delimiter_pos - start),
                            data_encryption_key, out));
    start = delimiter_pos + 1;
  }

  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
