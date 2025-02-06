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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_MOCK_FAKE_STREAMED_MATCH_DATA_PROVIDER_H_  // NOLINT(whitespace/line_length)
#define CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_MOCK_FAKE_STREAMED_MATCH_DATA_PROVIDER_H_  // NOLINT(whitespace/line_length)

#include <memory>
#include <vector>

#include "absl/strings/string_view.h"
#include "cc/core/interface/service_interface.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/public/core/interface/execution_result.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/interface/streamed_match_data_provider_interface.h"
#include "protos/lookup_server/backend/location.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

class FakeStreamedMatchDataProvider
    : public StreamedMatchDataProviderInterface {
 public:
  scp::core::ExecutionResult Init() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Run() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult Stop() noexcept override {
    return scp::core::SuccessExecutionResult();
  }

  scp::core::ExecutionResult GetMatchData(
      google::scp::core::ConsumerStreamingContext<
          lookup_server::proto_backend::Location, MatchDataBatch>
          context,
      std::shared_ptr<CryptoKeyInterface> data_encryption_key) noexcept
      override {
    return scp::core::SuccessExecutionResult();
  }
};

}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_MOCK_FAKE_STREAMED_MATCH_DATA_PROVIDER_H_
