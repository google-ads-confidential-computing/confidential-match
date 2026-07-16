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

#ifndef CC_MATCH_SERVICE_VALIDATORS_MATCH_KEY_ENCODING_VALIDATOR_H_
#define CC_MATCH_SERVICE_VALIDATORS_MATCH_KEY_ENCODING_VALIDATOR_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// Validates that a match key is encoded using the provided encoding type.
absl::Status ValidateMatchKeyEncoding(backend::MatchKeyEncoding encoding,
                                      absl::string_view match_key);

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_VALIDATORS_MATCH_KEY_ENCODING_VALIDATOR_H_
