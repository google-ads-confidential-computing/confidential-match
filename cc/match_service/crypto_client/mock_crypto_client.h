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

#ifndef CC_MATCH_SERVICE_CRYPTO_CLIENT_MOCK_CRYPTO_CLIENT_H_
#define CC_MATCH_SERVICE_CRYPTO_CLIENT_MOCK_CRYPTO_CLIENT_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"

#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"

namespace google::confidential_match::match_service {

class MockCryptoClient : public CryptoClientInterface {
 public:
  MOCK_METHOD(absl::Status, Init, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Run, (), (noexcept, override));
  MOCK_METHOD(absl::Status, Stop, (), (noexcept, override));

  // Redirects to GetCryptoKeyAsync to allow easier capturing of the context in
  // tests.
  void GetCryptoKey(AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
                        key_context) noexcept override {
    GetCryptoKeyAsync(key_context);
  }

  MOCK_METHOD(void, GetCryptoKeyAsync,
              ((AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>)),
              (noexcept));
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_CRYPTO_CLIENT_MOCK_CRYPTO_CLIENT_H_
