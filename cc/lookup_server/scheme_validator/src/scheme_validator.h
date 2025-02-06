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

#ifndef CC_LOOKUP_SERVER_SCHEME_VALIDATOR_SRC_SCHEME_VALIDATOR_H_
#define CC_LOOKUP_SERVER_SCHEME_VALIDATOR_SRC_SCHEME_VALIDATOR_H_

#include <atomic>

#include "absl/synchronization/mutex.h"
#include "cc/public/core/interface/execution_result.h"

#include "protos/lookup_server/backend/sharding_scheme.pb.h"

namespace google::confidential_match::lookup_server {

/**
 * @brief Utility used to validate whether a scheme for an incoming request
 * is valid for the scheme currently in use by Lookup Server.
 */
class SchemeValidator {
 public:
  SchemeValidator() : num_shards_(0), pending_num_shards_(0) {}

  /**
   * @brief Sets the provided sharding scheme as the currently active scheme.
   *
   * @param sharding_scheme the scheme for the currently stored data
   * @return an ExecutionResult indicating whether the operation was successful
   */
  scp::core::ExecutionResult SetCurrentScheme(
      const lookup_server::proto_backend::ShardingScheme&
          sharding_scheme) noexcept;

  /**
   * @brief Sets the provided sharding scheme as the pending sharding scheme
   * for the dataset currently being loaded.
   *
   * @param sharding_scheme the scheme for the dataset currently being loaded
   * @return an ExecutionResult indicating whether the operation was successful
   */
  scp::core::ExecutionResult SetPendingScheme(
      const lookup_server::proto_backend::ShardingScheme&
          sharding_scheme) noexcept;

  /**
   * @brief Saves the pending scheme as the new current scheme.
   *
   * This is called after once the dataset loading operation has finished,
   * and should only be called after `SetPendingScheme()` is called.
   *
   * @return an ExecutionResult indicating whether the operation was successful
   */
  scp::core::ExecutionResult ApplyPendingScheme() noexcept;

  /**
   * @brief Checks if the scheme for the incoming request is compatible with
   * the current scheme in use by the server.
   *
   * @param sharding_scheme the scheme used by the incoming request
   * @return true if the request sharding scheme is valid, false otherwise
   */
  bool IsValidRequestScheme(const lookup_server::proto_backend::ShardingScheme&
                                request_sharding_scheme) noexcept;

 private:
  // The number of shards in the scheme for the currently stored dataset.
  // Using atomic to avoid locks and ensure the validation path is performant,
  // since this is used on every lookup request
  std::atomic<int64_t> num_shards_;
  // Lock used to ensure that only one pending update is set or applied at any
  // given time. Required since multiple variables are updated on apply
  absl::Mutex pending_num_shards_mutex_;
  // The number of shards in the scheme for the dataset that is currently being
  // loaded, if any
  int64_t pending_num_shards_ ABSL_GUARDED_BY(pending_num_shards_mutex_);
};

}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_SCHEME_VALIDATOR_SRC_SCHEME_VALIDATOR_H_
