// Copyright 2025 Google LLC
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

#ifndef CC_MATCH_SERVICE_LOOKUP_SERVICE_SHARD_CLIENT_LOOKUP_SERVICE_SHARD_CLIENT_INTERFACE_H_  // NOLINT(whitespace/line_length)
#define CC_MATCH_SERVICE_LOOKUP_SERVICE_SHARD_CLIENT_LOOKUP_SERVICE_SHARD_CLIENT_INTERFACE_H_  // NOLINT(whitespace/line_length)

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "cc/core/async/async_context.h"
#include "protos/lookup_server/api/lookup.pb.h"

namespace google::confidential_match::match_service {

// Interface for the client that communicates with a specific Lookup Service
// Shard.
class LookupServiceShardClientInterface {
 public:
  virtual ~LookupServiceShardClientInterface() = default;

  virtual absl::Status Init() noexcept = 0;
  virtual absl::Status Run() noexcept = 0;
  virtual absl::Status Stop() noexcept = 0;

  // Sends a LookupRequest to the lookup service shard asynchronously.
  virtual void Lookup(
      AsyncContext<lookup_server::proto_api::LookupRequest,
                   lookup_server::proto_api::LookupResponse>& lookup_context,
      absl::string_view lookup_service_shard_address) noexcept = 0;
};

}  // namespace google::confidential_match::match_service

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_MATCH_SERVICE_LOOKUP_SERVICE_SHARD_CLIENT_LOOKUP_SERVICE_SHARD_CLIENT_INTERFACE_H_
