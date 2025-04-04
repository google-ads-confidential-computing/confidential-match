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

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_MOCK_MOCK_AEAD_CRYPTO_CLIENT_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_MOCK_MOCK_AEAD_CRYPTO_CLIENT_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "gmock/gmock.h"
#include "tink/aead.h"

#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"

namespace google::confidential_match::lookup_server {

class MockCryptoClient : public CryptoClientInterface {
 public:
  MOCK_METHOD(scp::core::ExecutionResult, Init, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Run, (), (noexcept, override));
  MOCK_METHOD(scp::core::ExecutionResult, Stop, (), (noexcept, override));

  MOCK_METHOD(void, GetCryptoKey,
              ((scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                                        CryptoKeyInterface>)),
              (noexcept, override));
};
}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_MOCK_MOCK_AEAD_CRYPTO_CLIENT_H_
