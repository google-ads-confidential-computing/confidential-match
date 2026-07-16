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

#include "cc/match_service/converters/lookup_associated_data_converter.h"

#include <string>

#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

#include "cc/core/error/status_macros.h"
#include "cc/match_service/error/error.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::google::confidential_match::match_service::backend::AssociatedData;
using ::google::confidential_match::match_service::backend::AssociatedDataType;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::KeyValue;
using ::google::protobuf::RepeatedPtrField;

// The Lookup Service associated data key to use for encrypted Gaia IDs.
constexpr absl::string_view kEncryptedGaiaAssociatedDataKey =
    "encrypted_gaia_id";

}  // namespace

absl::Status ToLookup(const AssociatedDataType& in, std::string& out) {
  switch (in) {
    case AssociatedDataType::ASSOCIATED_DATA_TYPE_UNSPECIFIED:
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          "Unspecified associated data has no corresponding Lookup value.");

    case AssociatedDataType::ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER:
      out = kEncryptedGaiaAssociatedDataKey;
      return absl::OkStatus();

    case AssociatedDataType::ASSOCIATED_DATA_TYPE_PRIVACY_INFO:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    "Privacy info associated data is not supported.");

    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Got unknown AssociatedDataType: ", in));
  }
}

absl::Status ToMatchService(
    const RepeatedPtrField<KeyValue>& in_associated_data, AssociatedData& out) {
  out.Clear();

  for (const auto& key_value : in_associated_data) {
    if (key_value.key() == kEncryptedGaiaAssociatedDataKey) {
      if (out.has_first_party_identifier()) {
        return Status(Error::CONVERTER_PARSE_ERROR,
                      "Attempted to set first party identifier more than once, "
                      "data is invalid.");
      }
      out.mutable_first_party_identifier()->set_id(
          absl::Base64Escape(key_value.bytes_value()));

    } else {
      return Status(
          Error::CONVERTER_PARSE_ERROR,
          absl::StrCat("Got unknown AssociatedData key from Lookup Service: ",
                       key_value.key()));
    }
  }

  return absl::OkStatus();
}

}  // namespace google::confidential_match::match_service
