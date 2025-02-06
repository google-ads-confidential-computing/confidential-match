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

#ifndef CC_LOOKUP_SERVER_INTERFACE_STREAMED_MATCH_DATA_PROVIDER_INTERFACE_H_
#define CC_LOOKUP_SERVER_INTERFACE_STREAMED_MATCH_DATA_PROVIDER_INTERFACE_H_

#include <memory>
#include <vector>

#include "cc/core/interface/service_interface.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/public/core/interface/execution_result.h"
#include "tink/aead.h"

#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "cc/lookup_server/types/match_data_group.h"
#include "protos/lookup_server/backend/location.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {

// A batch of MatchDataGroup objects. Used as an optimization to reduce the
// number of callbacks needed when returning match data upstream.
using MatchDataBatch = std::vector<MatchDataGroup>;

/** @brief Interface to provide streamed data for use in matching. */
class StreamedMatchDataProviderInterface : public scp::core::ServiceInterface {
 public:
  virtual ~StreamedMatchDataProviderInterface() = default;

  /**
   * @brief Retrieves match data from the provided location.
   *
   * @param context the context supplying the match data location and
   * a callback to process the match data
   * @param crypto_key the key to be used for decrypting match data
   * @return an ExecutionResult indicating whether the stream operation
   * was successfully scheduled
   */
  virtual scp::core::ExecutionResult GetMatchData(
      google::scp::core::ConsumerStreamingContext<
          lookup_server::proto_backend::Location, MatchDataBatch>
          context,
      std::shared_ptr<CryptoKeyInterface> crypto_key) noexcept = 0;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_INTERFACE_STREAMED_MATCH_DATA_PROVIDER_INTERFACE_H_
