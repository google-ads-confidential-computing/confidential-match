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

#ifndef CC_LOOKUP_SERVER_PARSERS_SRC_MATCH_DATA_FILE_PARSER_H_
#define CC_LOOKUP_SERVER_PARSERS_SRC_MATCH_DATA_FILE_PARSER_H_

#include <memory>
#include <vector>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/types/match_data_group.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Parses a match data file into MatchDataRow objects.
 *
 * @param file_contents the raw file contents to be parsed
 * @param data_encryption_key the key used to decrypt the encrypted match rows
 * @param out the vector to which to append MatchDataGroup objects
 * @return an ExecutionResult indicating the result of the parse operation
 */
scp::core::ExecutionResult ParseMatchDataFile(
    absl::string_view file_contents,
    std::shared_ptr<CryptoKeyInterface> data_encryption_key,
    std::vector<MatchDataGroup>& out) noexcept;

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_PARSERS_SRC_MATCH_DATA_FILE_PARSER_H_
