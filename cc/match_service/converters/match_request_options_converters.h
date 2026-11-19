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

#ifndef CC_MATCH_SERVICE_CONVERTERS_MATCH_REQUEST_OPTIONS_CONVERTERS_H_
#define CC_MATCH_SERVICE_CONVERTERS_MATCH_REQUEST_OPTIONS_CONVERTERS_H_

#include "absl/status/status.h"

#include "protos/match_service/api/v1/match_service.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Converts an API MatchRequestOptions to the backend format. Only the fields
// that are set in `in` are copied, so unset fields stay unset in `out`.
absl::Status ToBackend(const api::v1::MatchRequestOptions& in,
                       backend::MatchRequestOptions& out);

// Converts a backend MatchRequestOptions to the API format. Only the fields
// that are set in `in` are copied, so unset fields stay unset in `out`.
absl::Status ToApi(const backend::MatchRequestOptions& in,
                   api::v1::MatchRequestOptions& out);

// Converts an API UserDataNormalizerOptions to the backend format. Only the
// sub-messages that are set in `in` are copied, and within each, only the
// fields that are set, so unset fields stay unset in `out` and read as their
// backend proto defaults.
absl::Status ToBackend(const api::v1::UserDataNormalizerOptions& in,
                       backend::UserDataNormalizerOptions& out);

// Converts a backend UserDataNormalizerOptions to the API format. Only the
// sub-messages that are set in `in` are copied, and within each, only the
// fields that are set, so unset fields stay unset in `out`.
absl::Status ToApi(const backend::UserDataNormalizerOptions& in,
                   api::v1::UserDataNormalizerOptions& out);

// Converts the normalizer sub-messages of UserDataNormalizerOptions between
// the API and backend formats. Only the fields that are set in `in` are copied,
// so unset fields stay unset in `out`.
absl::Status ToBackend(const api::v1::EmailNormalizerOptions& in,
                       backend::EmailNormalizerOptions& out);
absl::Status ToApi(const backend::EmailNormalizerOptions& in,
                   api::v1::EmailNormalizerOptions& out);
absl::Status ToBackend(const api::v1::ZipCodeNormalizerOptions& in,
                       backend::ZipCodeNormalizerOptions& out);
absl::Status ToApi(const backend::ZipCodeNormalizerOptions& in,
                   api::v1::ZipCodeNormalizerOptions& out);
absl::Status ToBackend(const api::v1::NameNormalizerOptions& in,
                       backend::NameNormalizerOptions& out);
absl::Status ToApi(const backend::NameNormalizerOptions& in,
                   api::v1::NameNormalizerOptions& out);
absl::Status ToBackend(const api::v1::PhoneNumberNormalizerOptions& in,
                       backend::PhoneNumberNormalizerOptions& out);
absl::Status ToApi(const backend::PhoneNumberNormalizerOptions& in,
                   api::v1::PhoneNumberNormalizerOptions& out);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CONVERTERS_MATCH_REQUEST_OPTIONS_CONVERTERS_H_
