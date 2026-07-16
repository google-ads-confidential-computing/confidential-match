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

#ifndef CC_MATCH_SERVICE_CONVERTERS_FIELD_CONVERTERS_H_
#define CC_MATCH_SERVICE_CONVERTERS_FIELD_CONVERTERS_H_

#include "absl/status/status.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Converts an API Field to the backend format.
absl::Status ToBackend(const api::v1::Field& in, backend::Field& out);

// Converts a backend Field to the API format.
absl::Status ToApi(const backend::Field& in, api::v1::Field& out);

// Converts an API CompositeField to the backend format.
absl::Status ToBackend(const api::v1::CompositeField& in,
                       backend::CompositeField& out);

// Converts a backend CompositeField to the API format.
absl::Status ToApi(const backend::CompositeField& in,
                   api::v1::CompositeField& out);

// Converts an API FanOutField to the backend format.
absl::Status ToBackend(const api::v1::FanOutField& in,
                       backend::FanOutField& out);

// Converts a backend FanOutField to the API format.
absl::Status ToApi(const backend::FanOutField& in, api::v1::FanOutField& out);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CONVERTERS_FIELD_CONVERTERS_H_
