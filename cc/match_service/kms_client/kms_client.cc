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

#include "cc/match_service/kms_client/kms_client.h"

#include <memory>
#include <string>
#include <cstdint>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "cc/core/interface/errors.h"
#include "cc/public/cpio/interface/error_codes.h"
#include "cc/public/cpio/interface/kms_client/kms_client_interface.h"

#include "cc/core/async/async_context.h"
#include "cc/core/error/status_macros.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

namespace {

using ::google::cmrt::sdk::kms_service::v1::DecryptRequest;
using ::google::cmrt::sdk::kms_service::v1::DecryptResponse;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::errors::GetErrorMessage;

}  // namespace

KmsClient::KmsClient()
    : kms_client_(
          scp::cpio::KmsClientFactory::Create(scp::cpio::KmsClientOptions())) {}

KmsClient::KmsClient(
    std::shared_ptr<scp::cpio::KmsClientInterface> cpio_kms_client)
    : kms_client_(std::move(cpio_kms_client)) {}

absl::Status KmsClient::Init() noexcept {
  ExecutionResult result = kms_client_->Init();
  if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to init KMS client: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status KmsClient::Run() noexcept {
  ExecutionResult result = kms_client_->Run();
  if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to run KMS client: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status KmsClient::Stop() noexcept {
  ExecutionResult result = kms_client_->Stop();
  if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to stop KMS client: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

void KmsClient::Decrypt(
    google::confidential_match::AsyncContext<DecryptRequest, std::string>
        decrypt_context) noexcept {
  // Create a new AsyncContext to pass to the underlying CPIO client.
  auto cpio_context =
      decrypt_context
          .CreateExecutionResultAsyncContext<DecryptRequest, DecryptResponse>();
  cpio_context.request = decrypt_context.request;
  cpio_context.callback =
      absl::bind_front(&KmsClient::OnDecryptCallback, this, decrypt_context);

  auto scp_context = cpio_context.CreateScpAsyncContext();
  kms_client_->Decrypt(scp_context);
}

void KmsClient::OnDecryptCallback(
    AsyncContext<DecryptRequest, std::string> decrypt_context,
    ExecutionResultAsyncContext<DecryptRequest, DecryptResponse>&
        cpio_context) noexcept {
  if (!cpio_context.result.Successful()) {
    Error::Reason reason =
        MapWrappedKeyFetchingError(cpio_context.result.status_code);

    decrypt_context.status = Status(
        reason, absl::StrCat("SCP KMS client failed to decrypt, error: ",
                             GetErrorMessage(cpio_context.result.status_code)));
    decrypt_context.Finish();
    return;
  }

  decrypt_context.response =
      std::make_shared<std::string>(cpio_context.response->plaintext());
  decrypt_context.Finish();
}

// Method to map CPIO error codes to error reasons for wrapped keys.
Error::Reason KmsClient::MapWrappedKeyFetchingError(uint64_t cpio_error_code) {
  if (cpio_error_code == scp::core::errors::SC_CPIO_INVALID_CREDENTIALS) {
    return Error::CUSTOMER_KEY_PERMISSION_DENIED;
  }
  if (cpio_error_code == scp::core::errors::SC_CPIO_REQUEST_LIMIT_REACHED) {
    return Error::CUSTOMER_QUOTA_EXCEEDED;
  }
  if (cpio_error_code == scp::core::errors::SC_CPIO_INTERNAL_ERROR ||
      cpio_error_code == scp::core::errors::SC_CPIO_INVALID_ARGUMENT ||
      cpio_error_code == scp::core::errors::SC_CPIO_ENTITY_NOT_FOUND) {
    return Error::INVALID_DEK;
  }
  return Error::WRAPPED_KEY_FETCHING_ERROR;
}

}  // namespace google::confidential_match::match_service
