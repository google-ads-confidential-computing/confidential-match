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

#ifndef CC_MATCH_SERVICE_TASKS_ASSOCIATED_DATA_COLLECTOR_H_
#define CC_MATCH_SERVICE_TASKS_ASSOCIATED_DATA_COLLECTOR_H_

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"

#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {

// A helper utility to collect associated data and deduplicate equivalent
// entries.
class AssociatedDataCollector {
 public:
  // Constructs an associated data collector.
  AssociatedDataCollector() : associated_data_(), associated_data_indices_() {}

  // Adds associated data to the collector and returns the index of the
  // associated data in the collector.
  int Add(const backend::AssociatedData& associated_data) noexcept;

  // Returns all deduplicated associated data.
  std::vector<backend::AssociatedData> Get() const noexcept;

 private:
  // A vector of deduped associated data.
  std::vector<backend::AssociatedData> associated_data_;
  // All associated data keyed by a unique key for that object, with a value
  // containing an index within associated_data_ for that data.
  absl::flat_hash_map<std::string, int> associated_data_indices_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_TASKS_ASSOCIATED_DATA_COLLECTOR_H_
