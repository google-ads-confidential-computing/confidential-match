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

#include "cc/lookup_server/kms_client/src/kms_client.h"

#include <utility>

#include "absl/strings/string_view.h"
#include "cc/core/common/concurrent_map/src/error_codes.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/kms_client/kms_client_interface.h"
#include "cc/public/cpio/utils/sync_utils/src/sync_utils.h"

#include "cc/lookup_server/kms_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::kms_service::v1::DecryptResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::RetryExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::cpio::SyncUtils;
using ::std::placeholders::_1;

constexpr absl::string_view kComponentName = "KmsClient";

}  // namespace

ExecutionResult KmsClient::Init() noexcept {
  return kms_client_->Init();
}

ExecutionResult KmsClient::Run() noexcept {
  return kms_client_->Run();
}

ExecutionResult KmsClient::Stop() noexcept {
  return kms_client_->Stop();
}

ExecutionResult KmsClient::Decrypt(
    AsyncContext<DecryptRequest, std::string> decrypt_context) noexcept {
  AsyncContext<DecryptRequest, DecryptResponse> cpio_context;
  cpio_context.request = decrypt_context.request;
  cpio_context.callback = [decrypt_context](
                              AsyncContext<DecryptRequest, DecryptResponse>&
                                  callback_context) mutable {
    if (!callback_context.result.Successful()) {
      SCP_ERROR_CONTEXT(kComponentName, callback_context,
                        callback_context.result,
                        "CPIO KMS client failed to decrypt.");
      decrypt_context.result =
          FailureExecutionResult(KMS_CLIENT_DECRYPTION_ERROR);
      decrypt_context.Finish();
      return;
    }

    decrypt_context.result = SuccessExecutionResult();
    decrypt_context.response = std::make_shared<std::string>(
        std::move(*callback_context.response->mutable_plaintext()));
    decrypt_context.Finish();
  };

  kms_client_->Decrypt(cpio_context);
  return SuccessExecutionResult();
}

ExecutionResultOr<std::string> KmsClient::Decrypt(
    const DecryptRequest& decrypt_request) noexcept {
  std::string response;
  ExecutionResult result = SyncUtils::AsyncToSync<DecryptRequest, std::string>(
      std::bind(
          // NOLINTNEXTLINE(whitespace/parens)
          static_cast<ExecutionResult (KmsClient::*)(
              AsyncContext<DecryptRequest, std::string>)>(&KmsClient::Decrypt),
          this, _1),
      decrypt_request, response);
  RETURN_AND_LOG_IF_FAILURE(result, kComponentName, kZeroUuid,
                            "KMS client failed to decrypt.");
  return response;
}

}  // namespace google::confidential_match::lookup_server
