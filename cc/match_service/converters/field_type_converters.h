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

#ifndef CC_MATCH_SERVICE_CONVERTERS_FIELD_TYPE_CONVERTERS_H_
#define CC_MATCH_SERVICE_CONVERTERS_FIELD_TYPE_CONVERTERS_H_

#include "absl/status/status.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Converts an API FieldType to the backend format.
absl::Status ToBackend(const api::v1::FieldType& in, backend::FieldType& out);

// Converts a backend FieldType to the API format.
absl::Status ToApi(const backend::FieldType& in, api::v1::FieldType& out);

// Converts an API CompositeFieldType to the backend format.
absl::Status ToBackend(const api::v1::CompositeFieldType& in,
                       backend::CompositeFieldType& out);

// Converts a backend CompositeFieldType to the API format.
absl::Status ToApi(const backend::CompositeFieldType& in,
                   api::v1::CompositeFieldType& out);

// Converts an API FanOutFieldType to the backend format.
absl::Status ToBackend(const api::v1::FanOutFieldType& in,
                       backend::FanOutFieldType& out);

// Converts a backend FanOutFieldType to the API format.
absl::Status ToApi(const backend::FanOutFieldType& in,
                   api::v1::FanOutFieldType& out);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CONVERTERS_FIELD_TYPE_CONVERTERS_H_
