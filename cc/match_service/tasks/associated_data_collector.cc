// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cc/match_service/tasks/associated_data_collector.h"

#include <string>
#include <vector>

#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::AssociatedData;

// Returns a unique identifier for the associated data proto.
std::string GetUniqueIdentifier(const AssociatedData& associated_data) {
  // For now, associated data only contains first party identifier, so
  // each ID can uniquely represent the proto efficiently.
  // Proto serialization can be used once needed, ie. when new fields are added.
  return associated_data.first_party_identifier().id();
}

}  // namespace

int AssociatedDataCollector::Add(
    const AssociatedData& associated_data) noexcept {
  std::string key = GetUniqueIdentifier(associated_data);
  auto it = associated_data_indices_.find(key);
  if (it != associated_data_indices_.end()) {
    return it->second;
  }

  int index = associated_data_.size();
  associated_data_indices_[key] = index;
  associated_data_.push_back(associated_data);
  return index;
}

std::vector<AssociatedData> AssociatedDataCollector::Get() const noexcept {
  return associated_data_;
}

}  // namespace google::confidential_match::match_service
