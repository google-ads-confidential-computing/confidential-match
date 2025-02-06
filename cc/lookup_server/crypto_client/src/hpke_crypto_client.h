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

#ifndef CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_HPKE_CRYPTO_CLIENT_H_
#define CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_HPKE_CRYPTO_CLIENT_H_

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/crypto_client/crypto_client_interface.h"

#include "cc/lookup_server/interface/coordinator_client_interface.h"
#include "cc/lookup_server/interface/crypto_client_interface.h"
#include "cc/lookup_server/interface/crypto_key_interface.h"
#include "protos/lookup_server/backend/coordinator_client.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Provides a CryptoKey using hybrid public key encryption. */
class HpkeCryptoClient : public CryptoClientInterface,
                         public std::enable_shared_from_this<HpkeCryptoClient> {
 public:
  /**
   * @brief Constructs a hybrid public-key encryption crypto client.
   */
  static std::shared_ptr<HpkeCryptoClient> Create(
      std::shared_ptr<CoordinatorClientInterface> coordinator_client) noexcept;

  HpkeCryptoClient() = delete;

  scp::core::ExecutionResult Init() noexcept override;
  scp::core::ExecutionResult Run() noexcept override;
  scp::core::ExecutionResult Stop() noexcept override;

  /**
   * @brief Gets Tink keyset text from the coordinator client and returns a key
   * that can be used for encryption and decryption.
   *
   * @param key_context the crypto key context of crypto client request
   */
  void GetCryptoKey(scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                                            CryptoKeyInterface>
                        key_context) noexcept override;

  friend class HpkeCryptoKey;

 protected:
  /**
   * @brief Constructs a hybrid public-key encryption crypto client.
   *
   * This is exposed for mocking in tests only.
   *
   * This HpkeCryptoClient will call Init()/Run()/Stop() on the CPIO client.
   */
  explicit HpkeCryptoClient(
      std::shared_ptr<CoordinatorClientInterface> coordinator_client,
      std::shared_ptr<scp::cpio::CryptoClientInterface> cpio_crypto_client);

 private:
  /**
   * @brief Encrypts plaintext using hybrid encryption.
   *
   * @param plaintext the plaintext data to be encrypted
   * @param public_key the base-64 encoded raw public key.
   * @return the encrypted ciphertext, or a failure on error
   */
  scp::core::ExecutionResultOr<std::string> Encrypt(
      absl::string_view plaintext, absl::string_view public_key) noexcept;

  /**
   * @brief Decrypts ciphertext using hybrid decryption.
   *
   * @param ciphertext the data to be decrypted
   * @param private_key the base-64 encoded keyset for a Tink HpkePrivateKey
   * @return the decrypted plaintext, or a failure on error
   */
  scp::core::ExecutionResultOr<std::string> Decrypt(
      absl::string_view ciphertext, absl::string_view private_key) noexcept;

  /**
   * @brief Handles the callback after hybrid key request to the coordinator
   * client is completed.
   *
   * @param coordinator_context the context of the coordinator client request
   * @param key_context the context of the crypto client request
   */
  void OnGetHybridKeyCallback(
      const scp::core::AsyncContext<proto_backend::GetHybridKeyRequest,
                                    proto_backend::GetHybridKeyResponse>&
          coordinator_context,
      scp::core::AsyncContext<proto_backend::EncryptionKeyInfo,
                              CryptoKeyInterface>
          key_context) noexcept;

  // The client that fetches hybrid keys from a coordinator.
  std::shared_ptr<CoordinatorClientInterface> coordinator_client_;
  // The CPIO crypto client used for cryptographic operations.
  std::shared_ptr<scp::cpio::CryptoClientInterface> cpio_crypto_client_;
  // Keeps track of whether the service is running or not.
  std::atomic_bool is_running_;
};

}  // namespace google::confidential_match::lookup_server

#endif  // CC_LOOKUP_SERVER_CRYPTO_CLIENT_SRC_HPKE_CRYPTO_CLIENT_H_
