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

#ifndef CC_MATCH_SERVICE_CONVERTERS_MATCHED_FIELD_CONVERTERS_H_
#define CC_MATCH_SERVICE_CONVERTERS_MATCHED_FIELD_CONVERTERS_H_

#include "absl/status/status.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Converts an API MatchedField to the backend format.
absl::Status ToBackend(const api::v1::MatchedField& in,
                       backend::MatchedField& out);

// Converts a backend MatchedField to the API format.
absl::Status ToApi(const backend::MatchedField& in, api::v1::MatchedField& out);

// Converts an API MatchedCompositeField to the backend format.
absl::Status ToBackend(const api::v1::MatchedCompositeField& in,
                       backend::MatchedCompositeField& out);

// Converts a backend MatchedCompositeField to the API format.
absl::Status ToApi(const backend::MatchedCompositeField& in,
                   api::v1::MatchedCompositeField& out);

// Converts an API MatchedFanOutField to the backend format.
absl::Status ToBackend(const api::v1::MatchedFanOutField& in,
                       backend::MatchedFanOutField& out);

// Converts a backend MatchedFanOutField to the API format.
absl::Status ToApi(const backend::MatchedFanOutField& in,
                   api::v1::MatchedFanOutField& out);

// Converts an API FannedOutMatchedKey to the backend format.
absl::Status ToBackend(const api::v1::FannedOutMatchedKey& in,
                       backend::FannedOutMatchedKey& out);

// Converts a backend FannedOutMatchedKey to the API format.
absl::Status ToApi(const backend::FannedOutMatchedKey& in,
                   api::v1::FannedOutMatchedKey& out);

// Converts an API MatchedFieldInfo to the backend format.
absl::Status ToBackend(const api::v1::MatchedFieldInfo& in,
                       backend::MatchedFieldInfo& out);

// Converts a backend MatchedFieldInfo to the API format.
absl::Status ToApi(const backend::MatchedFieldInfo& in,
                   api::v1::MatchedFieldInfo& out);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CONVERTERS_MATCHED_FIELD_CONVERTERS_H_
