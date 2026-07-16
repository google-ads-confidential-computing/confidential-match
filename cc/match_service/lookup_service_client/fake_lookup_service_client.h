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

#ifndef CC_MATCH_SERVICE_LOOKUP_SERVICE_CLIENT_FAKE_LOOKUP_SERVICE_CLIENT_H_
#define CC_MATCH_SERVICE_LOOKUP_SERVICE_CLIENT_FAKE_LOOKUP_SERVICE_CLIENT_H_

#include <memory>

#include "absl/status/status.h"
#include "cc/core/async/async_context.h"
#include "cc/match_service/lookup_service_client/lookup_service_client_interface.h"
#include "protos/match_service/backend/lookup.pb.h"

namespace google::confidential_match::match_service {

// A fake implementation of the LookupServiceClientInterface for testing
// purposes. It echoes the request data back in the response.
class FakeLookupServiceClient : public LookupServiceClientInterface {
 public:
  ~FakeLookupServiceClient() override = default;

  absl::Status Init() noexcept override { return absl::OkStatus(); }

  absl::Status Run() noexcept override { return absl::OkStatus(); }

  absl::Status Stop() noexcept override { return absl::OkStatus(); }

  // Process input records and simply populate them into the response.
  void Lookup(AsyncContext<backend::LookupServiceRequest,
                           backend::LookupServiceResponse>&
                  lookup_service_context) noexcept override {
    lookup_service_context.response =
        std::make_shared<backend::LookupServiceResponse>();
    for (const auto& input_record :
         lookup_service_context.request->data_records()) {
      auto* result = lookup_service_context.response->add_lookup_results();
      result->set_status(backend::LookupResult::STATUS_SUCCESS);
      *result->mutable_client_data_record() = input_record;
      auto* matched_record = result->add_matched_data_records();
      *matched_record->mutable_lookup_key() = input_record.lookup_key();
    }
    lookup_service_context.Finish();
  }
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_LOOKUP_SERVICE_CLIENT_FAKE_LOOKUP_SERVICE_CLIENT_H_
