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

#include "cc/lookup_server/auth/src/gcp/jwk_cache.h"

#include <memory>
#include <mutex>
#include <string>

/**
 * This implements a lockless read, single writer cache for the JWK set.
 * The use case is that we want many threads to be able to read the cached
 * keys without blocking, but we want to protect the writing of the keys.
 * Locking for writing is acceptable, since only one thread is supposed to
 * write to the cache at a time, and this is an infrequent operation.
 *
 * The approach implemented is that the cache is implemented as an
 * atomic pointer to a string, with a shared_ptr to manage the memory.
 *
 * When writing, we create a new string, and then atomically swap the pointer
 * to point to the new string. Because we are using a shared_ptr, the old
 * string will not be deallocated until all threads that were reading it have
 * finished.
 */

namespace google::confidential_match::lookup_server::auth::gcp {

std::shared_ptr<const std::string> JwkCache::Get() const {
  return std::atomic_load(&active_string_);
}

void JwkCache::Update(const std::string& new_value) {
  std::lock_guard<std::mutex> lock(write_mtx_);
  auto new_ptr = std::make_shared<const std::string>(new_value);
  std::atomic_store(&active_string_, new_ptr);
}

bool JwkCache::IsInitialized() const {
  return std::atomic_load(&active_string_) != nullptr;
}

}  // namespace google::confidential_match::lookup_server::auth::gcp
