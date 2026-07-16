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

#include "cc/match_service/crypto_client/tink_utils.h"

#include <utility>

#include "tink/aead.h"
#include "tink/binary_keyset_reader.h"
#include "tink/cleartext_keyset_handle.h"
#include "tink/hybrid_decrypt.h"
#include "tink/hybrid_encrypt.h"
#include "tink/keyset_handle.h"
#include "tink/keyset_reader.h"

#include "cc/match_service/error/error.h"

namespace google::confidential_match::match_service {

using ::crypto::tink::Aead;
using ::crypto::tink::BinaryKeysetReader;
using ::crypto::tink::CleartextKeysetHandle;
using ::crypto::tink::HybridDecrypt;
using ::crypto::tink::HybridEncrypt;
using ::crypto::tink::KeysetHandle;
using ::crypto::tink::KeysetReader;
using ::google::confidential_match::match_service::backend::Error;
using TinkStatus = ::crypto::tink::util::Status;
template <typename T>
using TinkStatusOr = ::crypto::tink::util::StatusOr<T>;

template <typename TinkPrimitive>
absl::StatusOr<std::unique_ptr<TinkPrimitive>> GetTinkPrimitive(
    absl::string_view key_material) {
  TinkStatusOr<std::unique_ptr<KeysetReader>> keyset_reader_or =
      BinaryKeysetReader::New(key_material);
  if (!keyset_reader_or.ok()) {
    return absl::InternalError(
        absl::StrCat("Failed to read keyset from binary: ",
                     keyset_reader_or.status().message()));
  }

  TinkStatusOr<std::unique_ptr<KeysetHandle>> keyset_handle_or =
      CleartextKeysetHandle::Read(std::move(*keyset_reader_or));
  if (!keyset_handle_or.ok()) {
    return absl::InternalError(
        absl::StrCat("Failed to read keyset handle from keyset reader: ",
                     keyset_handle_or.status().message()));
  }

  // Get the primitive.
  TinkStatusOr<std::unique_ptr<TinkPrimitive>> key_type_or =
      (*keyset_handle_or)->GetPrimitive<TinkPrimitive>();
  if (!key_type_or.ok()) {
    return absl::InternalError(absl::StrFormat(
        "Failed to get %s from keyset handle: %s", typeid(TinkPrimitive).name(),
        key_type_or.status().message()));
  }
  return std::move(*key_type_or);
}

template absl::StatusOr<std::unique_ptr<Aead>> GetTinkPrimitive<Aead>(
    absl::string_view key_material);

template absl::StatusOr<std::unique_ptr<HybridDecrypt>>
GetTinkPrimitive<HybridDecrypt>(absl::string_view key_material);

template absl::StatusOr<std::unique_ptr<HybridEncrypt>>
GetTinkPrimitive<HybridEncrypt>(absl::string_view key_material);

}  // namespace google::confidential_match::match_service
