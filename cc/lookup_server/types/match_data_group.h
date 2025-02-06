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

#ifndef CC_LOOKUP_SERVER_TYPES_MATCH_DATA_GROUP_H_
#define CC_LOOKUP_SERVER_TYPES_MATCH_DATA_GROUP_H_

#include <vector>

#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

// A collection of match data rows containing all possible matches for a
// particular hash key.
using MatchDataGroup = std::vector<lookup_server::proto_backend::MatchDataRow>;

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_TYPES_MATCH_DATA_GROUP_H_
