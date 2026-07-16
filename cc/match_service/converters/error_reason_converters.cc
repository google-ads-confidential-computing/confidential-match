// Copyright 2025 Google LLC
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

#include "cc/match_service/converters/error_reason_converters.h"

#include "absl/status/status.h"
#include "cc/match_service/error/error.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

using ::google::confidential_match::match_service::backend::Error;

absl::Status ToBackend(const api::v1::ErrorReason& in,
                       backend::ErrorReason& out) {
  switch (in) {
    case api::v1::ERROR_REASON_UNKNOWN:
      out = backend::ERROR_REASON_UNKNOWN;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_INTERNAL_ERROR:
      out = backend::ERROR_REASON_INTERNAL_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_DECRYPTION_ERROR:
      out = backend::ERROR_REASON_DECRYPTION_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_DEK_DECRYPTION_ERROR:
      out = backend::ERROR_REASON_DEK_DECRYPTION_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_INVALID_DEK:
      out = backend::ERROR_REASON_INVALID_DEK;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_WRONG_NUMBER_OF_KEYS:
      out = backend::ERROR_REASON_WRONG_NUMBER_OF_KEYS;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_BASE64_DECODING_ERROR:
      out = backend::ERROR_REASON_BASE64_DECODING_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_KEY_FETCHING_ERROR:
      out = backend::ERROR_REASON_KEY_FETCHING_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_INVALID_MATCH_KEY_FIELD:
      out = backend::ERROR_REASON_INVALID_MATCH_KEY_FIELD;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_CUSTOMER_KEY_PERMISSION_DENIED:
      out = backend::ERROR_REASON_CUSTOMER_KEY_PERMISSION_DENIED;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_CUSTOMER_QUOTA_EXCEEDED:
      out = backend::ERROR_REASON_CUSTOMER_QUOTA_EXCEEDED;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR:
      out = backend::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR:
      out = backend::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_ENCRYPTION_ERROR:
      out = backend::ERROR_REASON_ENCRYPTION_ERROR;
      return absl::OkStatus();
    case api::v1::ERROR_REASON_DECODING_ERROR:
      out = backend::ERROR_REASON_DECODING_ERROR;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse ErrorReason: ", in));
  }
}

absl::Status ToApi(const backend::ErrorReason& in, api::v1::ErrorReason& out) {
  switch (in) {
    case backend::ERROR_REASON_UNKNOWN:
      out = api::v1::ERROR_REASON_UNKNOWN;
      return absl::OkStatus();
    case backend::ERROR_REASON_INTERNAL_ERROR:
      out = api::v1::ERROR_REASON_INTERNAL_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_DECRYPTION_ERROR:
      out = api::v1::ERROR_REASON_DECRYPTION_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_DEK_DECRYPTION_ERROR:
      out = api::v1::ERROR_REASON_DEK_DECRYPTION_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_INVALID_DEK:
      out = api::v1::ERROR_REASON_INVALID_DEK;
      return absl::OkStatus();
    case backend::ERROR_REASON_WRONG_NUMBER_OF_KEYS:
      out = api::v1::ERROR_REASON_WRONG_NUMBER_OF_KEYS;
      return absl::OkStatus();
    case backend::ERROR_REASON_BASE64_DECODING_ERROR:
      out = api::v1::ERROR_REASON_BASE64_DECODING_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_KEY_FETCHING_ERROR:
      out = api::v1::ERROR_REASON_KEY_FETCHING_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_INVALID_MATCH_KEY_FIELD:
      out = api::v1::ERROR_REASON_INVALID_MATCH_KEY_FIELD;
      return absl::OkStatus();
    case backend::ERROR_REASON_CUSTOMER_KEY_PERMISSION_DENIED:
      out = api::v1::ERROR_REASON_CUSTOMER_KEY_PERMISSION_DENIED;
      return absl::OkStatus();
    case backend::ERROR_REASON_CUSTOMER_QUOTA_EXCEEDED:
      out = api::v1::ERROR_REASON_CUSTOMER_QUOTA_EXCEEDED;
      return absl::OkStatus();
    case backend::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR:
      out = api::v1::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR:
      out = api::v1::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_ENCRYPTION_ERROR:
      out = api::v1::ERROR_REASON_ENCRYPTION_ERROR;
      return absl::OkStatus();
    case backend::ERROR_REASON_DECODING_ERROR:
      out = api::v1::ERROR_REASON_DECODING_ERROR;
      return absl::OkStatus();
    default:
      return Status(Error::CONVERTER_PARSE_ERROR,
                    absl::StrCat("Failed to parse ErrorReason: ", in));
  }
}

}  // namespace google::confidential_match::match_service
