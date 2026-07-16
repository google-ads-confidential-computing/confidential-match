/*
 * Copyright 2026 Google LLC
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

#ifndef CC_MATCH_SERVICE_CONVERTERS_LOOKUP_ASSOCIATED_DATA_CONVERTER_H_
#define CC_MATCH_SERVICE_CONVERTERS_LOOKUP_ASSOCIATED_DATA_CONVERTER_H_

#include <string>

#include "absl/status/status.h"
#include "google/protobuf/repeated_field.h"

#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Converts a Match Service AssociatedDataType to the Lookup Service request
// associated data key.
absl::Status ToLookup(const backend::AssociatedDataType& in, std::string& out);

// Converts Lookup Service associated data repeated key value to a Match
// Service associated data key.
absl::Status ToMatchService(
    const protobuf::RepeatedPtrField<backend::KeyValue>& in,
    backend::AssociatedData& out);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CONVERTERS_LOOKUP_ASSOCIATED_DATA_CONVERTER_H_
