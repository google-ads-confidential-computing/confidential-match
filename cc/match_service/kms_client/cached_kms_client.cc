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

#include "cc/match_service/kms_client/cached_kms_client.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "cc/core/async/async_context.h"
#include "cc/core/common/auto_expiry_concurrent_map/src/error_codes.h"
#include "cc/core/common/concurrent_map/src/error_codes.h"
#include "cc/core/common/uuid/src/uuid.h"
#include "cc/core/error/status_macros.h"
#include "cc/core/interface/errors.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/kms_client/kms_client_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/proto/kms_service/v1/kms_service.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {

namespace {

using ::google::cmrt::sdk::kms_service::v1::DecryptRequest;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::common::kZeroUuid;
using ::google::scp::core::errors::GetErrorMessage;
using ::google::scp::core::errors::
    SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_ALREADY_EXISTS;
using ::google::scp::core::errors::SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST;

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
    std::shared_ptr<scp::core::AsyncExecutorInterface> async_executor,
    KmsClientInterface* kms_client)
    : async_executor_(async_executor),
      kms_client_(kms_client),
      cache_(kDecryptionCacheEntryLifetimeSec,
             /* extend_entry_lifetime_on_access= */ true,
             /* block_entry_while_eviction= */ true,
             absl::bind_front(&OnBeforeCacheGarbageCollection),
             async_executor_) {}

absl::Status CachedKmsClient::Init() noexcept {
  ExecutionResult result = cache_.Init();
  if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to init KMS client cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status CachedKmsClient::Run() noexcept {
  ExecutionResult result = cache_.Run();
  if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to run KMS client cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

absl::Status CachedKmsClient::Stop() noexcept {
  ExecutionResult result = cache_.Stop();
  if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to stop KMS client cache: ",
                               GetErrorMessage(result.status_code)));
  }
  return absl::OkStatus();
}

void CachedKmsClient::Decrypt(
    google::confidential_match::AsyncContext<DecryptRequest, std::string>
        decrypt_context) noexcept {
  if (!decrypt_context.request) {
    decrypt_context.status =
        Status(Error::INTERNAL_ERROR, "Request must not be null.");
    decrypt_context.Finish();
    return;
  }

  std::string cache_key = GetCacheKey(*decrypt_context.request);

  absl::StatusOr<std::string> response_or = GetFromCache(cache_key);
  if (response_or.ok()) {
    decrypt_context.response =
        std::make_shared<std::string>(std::move(response_or.value()));
    decrypt_context.Finish();
    return;
  } else if (response_or.status().code() != absl::StatusCode::kNotFound) {
    LOG_WARNING(*decrypt_context.logger,
                "KMS client failed to fetch cached decryption result, "
                "performing decryption. Error: %v",
                response_or.status());
  }

  // No entry fetched from the cache. Decrypt using Cloud KMS
  auto kms_decrypt_context =
      decrypt_context.CreateAsyncContext<DecryptRequest, std::string>();
  kms_decrypt_context.request = decrypt_context.request;
  kms_decrypt_context.callback = absl::bind_front(
      &CachedKmsClient::HandleKmsDecryptionCallback, this, decrypt_context);

  kms_client_->Decrypt(kms_decrypt_context);
}

void CachedKmsClient::HandleKmsDecryptionCallback(
    google::confidential_match::AsyncContext<DecryptRequest, std::string>
        output_context,
    google::confidential_match::AsyncContext<DecryptRequest, std::string>
        kms_decrypt_context) noexcept {
  if (!kms_decrypt_context.status.ok()) {
    output_context.status = Annotate(kms_decrypt_context.status,
                                     "Cached KMS client unable to decrypt.");
    output_context.Finish();
    return;
  }

  std::string cache_key = GetCacheKey(*output_context.request);
  absl::Status cache_result =
      InsertToCache(cache_key, *kms_decrypt_context.response);

  // We log cache failures but do not fail the request because the decryption
  // itself succeeded.
  if (!cache_result.ok() && !absl::IsAlreadyExists(cache_result)) {
    LOG_WARNING(*kms_decrypt_context.logger,
                "Successfully retrieved a decryption response from the KMS, "
                "but unable to cache the response. Error: %v",
                cache_result);
  }
  output_context.response = std::move(kms_decrypt_context.response);
  output_context.Finish();
}

absl::Status CachedKmsClient::InsertToCache(
    const std::string& cache_key,
    const std::string& decrypt_response) noexcept {
  std::pair<std::string, std::string> key_value_pair = {cache_key,
                                                        decrypt_response};
  std::string existing_response_value;
  ExecutionResult result =
      cache_.Insert(key_value_pair, existing_response_value);

  if (result.status_code == SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED) {
    return Status(Error::DELETION_IN_PROGRESS,
                  "Entry is currently being deleted.");
  } else if (result.status_code == SC_CONCURRENT_MAP_ENTRY_ALREADY_EXISTS) {
    return Status(Error::ALREADY_EXISTS, "Entry already exists.");
  } else if (!result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to insert into cache: ",
                               GetErrorMessage(result.status_code)));
  }

  return absl::OkStatus();
}

absl::StatusOr<std::string> CachedKmsClient::GetFromCache(
    const std::string& cache_key) noexcept {
  std::string decrypt_response;
  ExecutionResult find_result = cache_.Find(cache_key, decrypt_response);

  if (find_result.status_code == SC_CONCURRENT_MAP_ENTRY_DOES_NOT_EXIST) {
    return Status(Error::NOT_FOUND, "Entry not found in cache.");
  } else if (find_result.status_code ==
             SC_AUTO_EXPIRY_CONCURRENT_MAP_ENTRY_BEING_DELETED) {
    return Status(Error::DELETION_IN_PROGRESS,
                  "Entry is currently being deleted.");
  } else if (!find_result.Successful()) {
    return Status(Error::INTERNAL_ERROR,
                  absl::StrCat("Failed to fetch from cache: ",
                               GetErrorMessage(find_result.status_code)));
  }

  return decrypt_response;
}

}  // namespace google::confidential_match::match_service
