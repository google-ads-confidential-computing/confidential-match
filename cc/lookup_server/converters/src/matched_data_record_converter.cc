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

#include "cc/lookup_server/converters/src/matched_data_record_converter.h"

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/types/span.h"
#include "cc/public/core/interface/execution_result_macros.h"
#include "cc/public/core/interface/execution_result_or_macros.h"

#include "cc/lookup_server/converters/src/error_codes.h"
#include "cc/lookup_server/converters/src/key_value_converter.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_api::MatchedDataRecord;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;

ExecutionResult ConvertToMatchedDataRecord(const MatchDataRow& match_data_row,
                                           MatchedDataRecord& out) {
  *out.mutable_lookup_key()->mutable_key() = match_data_row.key();
  for (const KeyValue& kv : match_data_row.associated_data()) {
    RETURN_IF_FAILURE(ConvertKeyValue(kv, *out.add_associated_data()));
  }
  return SuccessExecutionResult();
}

ExecutionResult ConvertToMatchedDataRecord(
    const MatchDataRow& match_data_row,
    absl::Span<const std::string* const> associated_data_keys,
    MatchedDataRecord& out) {
  absl::flat_hash_map<absl::string_view, std::vector<const KeyValue*>>
      keyed_associated_data;
  for (const KeyValue& kv : match_data_row.associated_data()) {
    keyed_associated_data[kv.key()].push_back(&kv);
  }

  *out.mutable_lookup_key()->mutable_key() = match_data_row.key();
  for (const auto* key : associated_data_keys) {
    for (const KeyValue* kv : keyed_associated_data[*key]) {
      RETURN_IF_FAILURE(ConvertKeyValue(*kv, *out.add_associated_data()));
    }
  }
  return SuccessExecutionResult();
}

}  // namespace google::confidential_match::lookup_server
