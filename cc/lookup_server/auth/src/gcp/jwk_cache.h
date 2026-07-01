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

#ifndef CC_LOOKUP_SERVER_AUTH_SRC_GCP_JWK_CACHE_H_
#define CC_LOOKUP_SERVER_AUTH_SRC_GCP_JWK_CACHE_H_

#include <memory>
#include <mutex>
#include <string>

namespace google::confidential_match::lookup_server::auth::gcp {

/**
 *  JwkCache manages the active set of JWK keys for validating JWTs.
 */
class JwkCache {
 public:
  JwkCache() = default;

  // Get active keys.
  std::shared_ptr<const std::string> Get() const;

  // Update active keys.
  void Update(const std::string& new_value);

  // Checks if the keys cache has been initialized with a value.
  bool IsInitialized() const;

 private:
  // Const to make sure that readers don't mistakenly try to mutate the string.
  // Once we migrate to c++20, we can make active_string_ an atomic<shared_ptr>,
  // and remove the usages of atomic_load.
  std::shared_ptr<const std::string> active_string_;
  std::mutex write_mtx_;
};

}  // namespace google::confidential_match::lookup_server::auth::gcp

#endif  // CC_LOOKUP_SERVER_AUTH_SRC_GCP_JWK_CACHE_H_
