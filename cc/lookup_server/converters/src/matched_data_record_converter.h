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

#ifndef CC_LOOKUP_SERVER_CONVERTERS_SRC_MATCHED_DATA_RECORD_CONVERTER_H_
#define CC_LOOKUP_SERVER_CONVERTERS_SRC_MATCHED_DATA_RECORD_CONVERTER_H_

#include <string>

#include "absl/types/span.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Converts a MatchDataRow backend proto to a MatchedDataRecord API
 * proto.
 *
 * @param match_data_row the MatchDataRow to convert
 * @param out the MatchedDataRecord to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertToMatchedDataRecord(
    const proto_backend::MatchDataRow& match_data_row,
    proto_api::MatchedDataRecord& out);

/**
 * @brief Converts a MatchDataRow backend proto to a MatchedDataRecord API
 * proto, copying over only the provided associated data to the record.
 *
 * @param match_data_row the MatchDataRow to convert
 * @param associated_data_keys a list of associated data keys to be included
 * in the MatchDataRecord if present in the MatchDataRow
 * @param out the MatchedDataRecord to be written to
 * @return an ExecutionResult indicating whether the operation was successful
 */
scp::core::ExecutionResult ConvertToMatchedDataRecord(
    const proto_backend::MatchDataRow& match_data_row,
    absl::Span<const std::string* const> associated_data_keys,
    proto_api::MatchedDataRecord& out);

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CONVERTERS_SRC_MATCHED_DATA_RECORD_CONVERTER_H_
