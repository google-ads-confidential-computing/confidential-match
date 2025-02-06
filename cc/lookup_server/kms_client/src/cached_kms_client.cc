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

#include "cc/lookup_server/kms_client/src/cached_kms_client.h"

#include <utility>

#include "absl/strings/string_view.h"

#include "cc/core/common/auto_expiry_concurrent_map/src/auto_expiry_concurrent_map.h"
#include "cc/core/common/auto_expiry_concurrent_map/src/error_codes.h"
#include "cc/core/common/concurrent_map/src/error_codes.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/errors.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/utils/sync_utils/src/sync_utils.h"

#include "cc/lookup_server/kms_client/src/error_codes.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::RetryExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::
    SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_ALREADY_EXISTS;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST;
using ::google::scp::cpio::SyncUtils;
using ::std::placeholders::_1;
using ::std::placeholders::_2;
using ::std::placeholders::_3;

constexpr absl::string_view kComponentName = "CachedKmsClient";
constexpr int kDecryptionCacheEntryLifetimeSec = 3600;

// Callback invoked by the cache before removing stale entries.
void OnBeforeCacheGarbageCollection(
    std::string& key, std::string& value,
    std::function<void(bool)> should_delete_entry) {
  should_delete_entry(true);
}

// Converts a decryption request into a key used to cache a response value.
std::string GetCacheKey(const DecryptRequest& decrypt_request) {
  return decrypt_request.SerializeAsString();
}

}  // namespace

CachedKmsClient::CachedKmsClient(
    std::shared_ptr<AsyncExecutorInterface> async_executor,
    std::shared_ptr<KmsClientInterface> kms_client)
    : async_executor_(async_executor),
      kms_client_(kms_client),
      cache_(kDecryptionCacheEntryLifetimeSec,
             /* extend_entry_lifetime_on_access= */ true,
             /* block_entry_while_eviction= */ true,
             std::bind(&OnBeforeCacheGarbageCollection, _1, _2, _3),
             async_executor) {}

ExecutionResult CachedKmsClient::Init() noexcept {
  return cache_.Init();
}

ExecutionResult CachedKmsClient::Run() noexcept {
  return cache_.Run();
}

ExecutionResult CachedKmsClient::Stop() noexcept {
  return cache_.Stop();
}

ExecutionResultOr<std::string> CachedKmsClient::Decrypt(
    const DecryptRequest& decrypt_request) noexcept {
  std::string response;
  ExecutionResult result = SyncUtils::AsyncToSync<DecryptRequest, std::string>(
      // NOLINTNEXTLINE(whitespace/parens)
      std::bind(static_cast<ExecutionResult (CachedKmsClient::*)(
                    AsyncContext<DecryptRequest, std::string>)>(
                    &CachedKmsClient::Decrypt),
                this, _1),
      decrypt_request, response);
  RETURN_AND_LOG_IF_FAILURE(result, kComponentName, kZeroUuid,
                            "Cached KMS client failed to decrypt.");
  return response;
}

ExecutionResult CachedKmsClient::Decrypt(
    AsyncContext<DecryptRequest, std::string> decrypt_context) noexcept {
  std::string cache_key = GetCacheKey(*decrypt_context.request);

  ExecutionResultOr<std::string> response_or = GetFromCache(cache_key);
  if (response_or.result().Successful()) {
    decrypt_context.result = SuccessExecutionResult();
    decrypt_context.response =
        std::make_shared<std::string>(std::move(*response_or));
    decrypt_context.Finish();
    return SuccessExecutionResult();
  }

  // No entry fetched from the cache. Decrypt using the Cloud KMS
  AsyncContext<DecryptRequest, std::string> kms_decrypt_context;
  kms_decrypt_context.request =
      std::make_shared<DecryptRequest>(*decrypt_context.request);
  kms_decrypt_context.callback = std::bind(
      &CachedKmsClient::HandleKmsDecryptionCallback, this, _1, decrypt_context);
  return kms_client_->Decrypt(kms_decrypt_context);
}

void CachedKmsClient::HandleKmsDecryptionCallback(
    AsyncContext<DecryptRequest, std::string> kms_decrypt_context,
    AsyncContext<DecryptRequest, std::string> output_context) noexcept {
  if (!kms_decrypt_context.result.Successful()) {
    output_context.result = kms_decrypt_context.result;
    output_context.Finish();
    return;
  }

  std::string cache_key = GetCacheKey(*output_context.request);
  ExecutionResult cache_result =
      InsertToCache(cache_key, *kms_decrypt_context.response);
  if (!cache_result.Successful()) {
    SCP_WARNING_CONTEXT(
        kComponentName, kms_decrypt_context,
        absl::StrFormat(
            "Successfully retrieved a decryption response from the KMS, "
            "but unable to cache the response. Error: %s",
            GetErrorMessage(cache_result.status_code)));
  }

  output_context.result = SuccessExecutionResult();
  output_context.response = kms_decrypt_context.response;
  output_context.Finish();
}

ExecutionResult CachedKmsClient::InsertToCache(
    const std::string& cache_key,
    const std::string& decrypt_response) noexcept {
  std::pair<std::string, std::string> key_value_pair =
      std::make_pair(cache_key, decrypt_response);
  std::string existing_response_value;
  ExecutionResult result =
      cache_.Insert(key_value_pair, existing_response_value);
  if (result.status_code == SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED) {
    return RetryExecutionResult(KMS_CLIENT_CACHE_ENTRY_BEING_DELETED);
  } else if (result.status_code == SC_CONCURRENT_MAP_ENTRY_ALREADY_EXISTS) {
    return FailureExecutionResult(KMS_CLIENT_CACHE_ENTRY_ALREADY_EXISTS);
  } else if (!result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, result,
              "Failed to cache the decryption response.");
    return FailureExecutionResult(KMS_CLIENT_CACHE_ENTRY_INSERT_ERROR);
  }

  return SuccessExecutionResult();
}

ExecutionResultOr<std::string> CachedKmsClient::GetFromCache(
    const std::string& cache_key) noexcept {
  std::string decrypt_response;
  ExecutionResult find_result = cache_.Find(cache_key, decrypt_response);
  if (find_result.status_code == SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST) {
    return FailureExecutionResult(SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST);
  } else if (find_result.status_code ==
             SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED) {
    return RetryExecutionResult(KMS_CLIENT_CACHE_ENTRY_BEING_DELETED);
  } else if (!find_result.Successful()) {
    SCP_ERROR(kComponentName, kZeroUuid, find_result,
              "Failed to fetch the cached decryption response.");
    return FailureExecutionResult(KMS_CLIENT_CACHE_ENTRY_FETCH_ERROR);
  }

  return decrypt_response;
}

}  // namespace google::confidential_match::lookup_server
