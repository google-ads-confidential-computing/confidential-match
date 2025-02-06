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

#ifndef CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_BLOB_STORAGE_DATA_PROVIDER_H_
#define CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_BLOB_STORAGE_DATA_PROVIDER_H_

#include <memory>
#include <string>

#include "cc/core/interface/async_context.h"
#include "cc/core/interface/service_interface.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"

#include "cc/lookup_server/interface/data_provider_interface.h"
#include "protos/lookup_server/backend/location.pb.h"

namespace google::confidential_match::lookup_server {

/** @brief Provides data from object storage asynchronously. */
class BlobStorageDataProvider : public DataProviderInterface {
 public:
  explicit BlobStorageDataProvider(
      std::shared_ptr<scp::cpio::BlobStorageClientInterface>
          blob_storage_client)
      : blob_storage_client_(blob_storage_client) {}

  scp::core::ExecutionResult Init() noexcept override;

  scp::core::ExecutionResult Run() noexcept override;

  scp::core::ExecutionResult Stop() noexcept override;

  /**
   * @brief Retrieves data from the provided location.
   *
   * Uses streaming to fetch the data asynchronously and assembles it into
   * its full contents before returning to the caller.
   *
   * @param context the context supplying the data location and
   * a callback to process the data
   * @return an ExecutionResult indicating whether the fetch operation
   * was successfully scheduled
   */
  scp::core::ExecutionResult Get(
      google::scp::core::AsyncContext<lookup_server::proto_backend::Location,
                                      std::string>
          context) noexcept override;

 protected:
  // Helper to handle the callback for retrieving data from blob storage.
  void OnGetBlobCallback(
      scp::core::AsyncContext<
          google::cmrt::sdk::blob_storage_service::v1::GetBlobRequest,
          google::cmrt::sdk::blob_storage_service::v1::GetBlobResponse>&
          get_blob_context,
      scp::core::AsyncContext<proto_backend::Location, std::string>
          parent_context) noexcept;

  std::shared_ptr<scp::cpio::BlobStorageClientInterface> blob_storage_client_;
};
}  // namespace google::confidential_match::lookup_server

// NOLINTNEXTLINE(whitespace/line_length)
#endif  // CC_LOOKUP_SERVER_MATCH_DATA_PROVIDER_SRC_BLOB_STORAGE_DATA_PROVIDER_H_
