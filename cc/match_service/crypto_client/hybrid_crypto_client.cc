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

#include "cc/match_service/crypto_client/hybrid_crypto_client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/strings/escaping.h"
#include "cc/core/interface/errors.h"
#include "cc/match_service/crypto_client/hybrid_decrypt_crypto_key.h"
#include "cc/match_service/crypto_client/tink_utils.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"
#include "tink/hybrid/hpke_config.h"
#include "tink/hybrid_config.h"
#include "tink/hybrid_decrypt.h"
#include "tink/util/status.h"

namespace google::confidential_match::match_service {

using ::crypto::tink::HybridConfig;
using ::crypto::tink::HybridDecrypt;
using ::crypto::tink::RegisterHpke;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::cpio::KeyFetcherWithCacheInterface;
using TinkStatus = ::crypto::tink::util::Status;

HybridCryptoClient::HybridCryptoClient(
    KeyFetcherWithCacheInterface* key_fetcher)
    : key_fetcher_(key_fetcher) {}

absl::Status HybridCryptoClient::Init() noexcept {
  return absl::OkStatus();
}

absl::Status HybridCryptoClient::Run() noexcept {
  TinkStatus register_result = HybridConfig::Register();
  if (!register_result.ok()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to initialize HybridConfig: ",
                               register_result.message()));
  }
  register_result = RegisterHpke();
  if (!register_result.ok()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to initialize Hpke config: ",
                               register_result.message()));
  }
  return absl::OkStatus();
}

absl::Status HybridCryptoClient::Stop() noexcept {
  return absl::OkStatus();
}

void HybridCryptoClient::GetCryptoKey(
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context) noexcept {
  auto key_or =
      key_fetcher_->GetKey(context.request->coordinator_key_info().key_id());

  if (!key_or.Successful()) {
    // TODO(b/492229483) will use a different error code after the error mapping
    // is determined.
    context.status =
        Status(Error::KEY_FETCHING_ERROR,
               absl::StrFormat("Failed to get private key for key_id %s: %s",
                               context.request->coordinator_key_info().key_id(),
                               GetErrorMessage(key_or.result().status_code)));

    LOG_ERROR(*context.logger, "%v", context.status);
    context.Finish();
    return;
  }

  std::string decoded_key;
  if (!absl::Base64Unescape(key_or->private_key, &decoded_key)) {
    context.status =
        Status(Error::DECODING_ERROR,
               absl::StrCat(
                   "Failed to Base64 unescape private key material for key ID ",
                   key_or->key_id));
    LOG_ERROR(*context.logger, "%v", context.status);
    context.Finish();
    return;
  }
  auto decrypt_or = GetTinkPrimitive<HybridDecrypt>(decoded_key);
  if (!decrypt_or.ok()) {
    context.status = decrypt_or.status();
    LOG_ERROR(*context.logger, "%v", context.status);
    context.Finish();
    return;
  }

  context.response =
      std::make_shared<HybridDecryptCryptoKey>(std::move(*decrypt_or));
  context.Finish();
}

}  // namespace google::confidential_match::match_service
