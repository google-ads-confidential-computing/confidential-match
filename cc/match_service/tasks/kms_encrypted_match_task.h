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

#ifndef CC_MATCH_SERVICE_TASKS_KMS_ENCRYPTED_MATCH_TASK_H_
#define CC_MATCH_SERVICE_TASKS_KMS_ENCRYPTED_MATCH_TASK_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "cc/core/async/async_context.h"
#include "cc/core/hash/hasher_interface.h"
#include "cc/match_service/crypto_client/crypto_client_interface.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/lookup_service_client/lookup_service_client_interface.h"
#include "cc/match_service/tasks/match_task_interface.h"
#include "cc/public/cpio/interface/metric_client/metric_client_interface.h"
#include "protos/core/encryption_key_info.pb.h"
#include "protos/match_service/backend/lookup.pb.h"
#include "protos/match_service/backend/match_service.pb.h"
#include "protos/match_service/backend/private_key_endpoints.pb.h"

namespace google::confidential_match::match_service {

namespace encrypted_match_task_internal {

// Stores the indices of a specific match key in the original request.
struct KeyIndex {
  // The index of the data record containing the key.
  int data_records_index;
  // The index of the match key containing the key.
  int match_keys_index;
};

// Represents an encrypted match key and associated metadata.
struct EncryptedMatchKey {
  // The encrypted match key, encoded in base-64, regardless of the original
  // encoding in the request.
  std::string encrypted_match_key;
  // The type of match key being stored.
  backend::FieldType field_type = backend::FieldType::FIELD_TYPE_UNSPECIFIED;
  // The position of the encrypted match key within the original request.
  KeyIndex key_index;
};

// A group of match keys that share the same encryption configuration.
struct KeyGroup {
  // The specific key info proto required for the LookupServiceRequest.
  std::shared_ptr<backend::EncryptionKey> key_info;
  // A list of match keys encrypted with the key in this group.
  // Any fields sharing the same key index (eg. composite fields like first and
  // last name) must be stored contiguously.
  std::vector<EncryptedMatchKey> keys;
};

}  // namespace encrypted_match_task_internal

// A task to perform a matching operation for KMS-encrypted data.
class KmsEncryptedMatchTask : public MatchTaskInterface {
 public:
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the KmsEncryptedMatchTask object.
  explicit KmsEncryptedMatchTask(
      LookupServiceClientInterface* lookup_service_client,
      CryptoClientInterface* aead_crypto_client, HasherInterface* sha256_hasher,
      CryptoClientInterface* hybrid_crypto_client,
      const backend::PrivateKeyEndpoints& private_key_endpoints,
      scp::cpio::MetricClientInterface* metric_client = nullptr,
      absl::string_view metric_namespace = "")
      : lookup_service_client_(lookup_service_client),
        aead_crypto_client_(aead_crypto_client),
        sha256_hasher_(sha256_hasher),
        hybrid_crypto_client_(hybrid_crypto_client),
        private_key_endpoints_(private_key_endpoints),
        metric_client_(metric_client),
        metric_namespace_(metric_namespace) {}

  // Performs the match operation.
  void Match(AsyncContext<backend::MatchRequest, backend::MatchResponse>
                 context) noexcept override;

 private:
  // A structure to hold decrypted values corresponding to the original request
  // structure, of the form: [data_record_index][match_key_index][field_index]
  // The data record and key indices map 1:1 with the indices in the request.
  // The field index represents each decrypted value within a composite
  // field in the concatenation ordering required by Lookup Service, or the
  // sole decrypted value for a non-composite field.
  using DecryptedValues = std::vector<std::vector<std::vector<std::string>>>;

  // Shared state to manage synchronization of multiple concurrent groups.
  struct MatchRequestContext {
    explicit MatchRequestContext(
        AsyncContext<backend::MatchRequest, backend::MatchResponse> ctx)
        : match_context(ctx) {}

    // Records a failed status to the context, or appends if one exists.
    // IMPORTANT: The caller is required to hold the mutex before calling this.
    void RecordFailureHoldingLock(absl::Status failed_status) noexcept;

    // The original context to eventually finish.
    AsyncContext<backend::MatchRequest, backend::MatchResponse> match_context;

    // Grid storing decrypted values: [record_index][key_index][field_index].
    // Composite fields are stored in the same concatenation order required
    // by Lookup Service, while regular fields have a single decrypted value
    // at field_index = 0.
    DecryptedValues decrypted_values;

    // Start times for each key group fetch operation.
    std::vector<absl::Time> key_group_start_times;

    // The response object, which is built up over the course of the request.
    // Any field-level errors (eg. decryption errors) are populated onto the
    // match response throughout the processing of the request.
    std::shared_ptr<backend::MatchResponse> match_response;

    // Collected responses from separate LookupService calls;
    std::vector<std::shared_ptr<backend::LookupServiceResponse>>
        lookup_responses;

    // Synchronization primitives.
    absl::Mutex mutex;
    // Indicates the current number of lookup threads that haven't finished yet.
    int pending_groups = 0;
    // The status of the operation. The first failed status code is used in the
    // event of multiple failures.
    absl::Status status = absl::OkStatus();
  };

  // Handles the processing for a group of keys: Decrypts them, then calls
  // LookupService.
  void OnGroupCryptoKeyReady(
      std::shared_ptr<MatchRequestContext> state,
      encrypted_match_task_internal::KeyGroup group, size_t group_index,
      AsyncContext<google::confidential_match::EncryptionKeyInfo,
                   CryptoKeyInterface>
          key_context) noexcept;

  // Handles the response from a Lookup Service match operation.
  void OnLookupCallback(
      std::shared_ptr<MatchRequestContext> state,
      AsyncContext<backend::LookupServiceRequest,
                   backend::LookupServiceResponse>& lookup_context) noexcept;

  // Helper method to group keys by their encryption configuration.
  absl::StatusOr<std::vector<encrypted_match_task_internal::KeyGroup>>
  GroupByEncryptionKey(const backend::MatchRequest& request,
                      backend::MatchResponse& match_response);

  // Helper to create the LookupServiceRequest and populate decrypted values.
  absl::StatusOr<std::shared_ptr<backend::LookupServiceRequest>>
  CreateLookupRequest(MatchRequestContext& state,
                      const encrypted_match_task_internal::KeyGroup& group,
                      const CryptoKeyInterface& crypto_key);

  // Helper to batch decrypt all encrypted items for this group.
  std::vector<absl::StatusOr<std::string>> DecryptKeysInGroup(
      const encrypted_match_task_internal::KeyGroup& group,
      const CryptoKeyInterface& crypto_key);

  // Helper to handle processing of single field types.
  absl::Status ProcessSingleField(
      const encrypted_match_task_internal::EncryptedMatchKey& key,
      absl::string_view decrypted_val, MatchRequestContext& state,
      backend::LookupServiceRequest& lookup_request);

  // Helper to handle reconstruction, hashing, and encryption of composite
  // fields. Requires decrypted First Name/Last Name pairs.
  absl::Status ProcessCompositeField(
      absl::Span<const std::pair<backend::FieldType, absl::string_view>>
          decrypted_fields,
      const encrypted_match_task_internal::KeyIndex& key_index,
      const CryptoKeyInterface& crypto_key, MatchRequestContext& state,
      backend::LookupServiceRequest& lookup_request);

  // Helper to populate the coordinator info for an encryption key.
  template <typename T>
  void PopulateCoordinatorInfo(T* coord_key) const {
    coord_key->clear_coordinator_info();
    for (const auto& endpoint : private_key_endpoints_.endpoints()) {
      auto* info = coord_key->add_coordinator_info();
      info->set_key_service_endpoint(endpoint.endpoint());
      info->set_kms_wip_provider(endpoint.gcp_wip_provider());
      info->set_key_service_audience_url(endpoint.gcp_cloud_function_url());
    }
  }

  // The client used to call Lookup Service.
  LookupServiceClientInterface* lookup_service_client_;
  // The client used to decrypt wrapped keys.
  CryptoClientInterface* aead_crypto_client_;
  // The hasher used to generate SHA-256 hashes.
  HasherInterface* sha256_hasher_;
  // The client used to acquire coordinator keys.
  CryptoClientInterface* hybrid_crypto_client_;
  // The endpoints for the private key service.
  backend::PrivateKeyEndpoints private_key_endpoints_;
  // The client responsible for recording metrics.
  scp::cpio::MetricClientInterface* metric_client_;
  // The namespace that metrics will be written to.
  std::string metric_namespace_;
};

}  // namespace google::confidential_match::match_service

#endif  // CC_MATCH_SERVICE_TASKS_KMS_ENCRYPTED_MATCH_TASK_H_
