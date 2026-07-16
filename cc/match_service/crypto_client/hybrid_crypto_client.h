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

#ifndef CONFIDENTIAL_MATCH_HPKE_CRYPTO_CLIENT_H
#define CONFIDENTIAL_MATCH_HPKE_CRYPTO_CLIENT_H

#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/public/cpio/utils/key_fetching/interface/key_fetcher_with_cache_interface.h"

namespace google::confidential_match::match_service {

// There is meant to be an instance of this class per-application to support
// caching per application.
class HybridCryptoClient : public CryptoClientInterface {
 public:
  explicit HybridCryptoClient(
      google::scp::cpio::KeyFetcherWithCacheInterface* key_fetcher);

  absl::Status Init() noexcept override;
  absl::Status Run() noexcept override;
  absl::Status Stop() noexcept override;

  void GetCryptoKey(AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
                        key_context) noexcept override;

 private:
  google::scp::cpio::KeyFetcherWithCacheInterface* key_fetcher_;
};

}  // namespace google::confidential_match::match_service

#endif  // CONFIDENTIAL_MATCH_HPKE_CRYPTO_CLIENT_H
