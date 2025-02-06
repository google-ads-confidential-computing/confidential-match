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

#include "cc/lookup_server/match_data_provider/src/blob_storage_data_provider.h"

#include <future>
#include <memory>

#include "absl/strings/string_view.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"
#include "cc/public/cpio/mock/blob_storage_client/mock_blob_storage_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/interface/data_provider_interface.h"
#include "cc/lookup_server/match_data_provider/src/error_codes.h"
#include "protos/lookup_server/backend/location.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobRequest;
using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobResponse;
using ::google::confidential_match::lookup_server::proto_backend::Location;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::MockBlobStorageClient;
using ::testing::_;
using ::testing::Return;

constexpr absl::string_view kBucketName = "test-bucket";
constexpr absl::string_view kPath = "test/path.txt";
constexpr absl::string_view kSampleData = R"({"test":"data"})";

class BlobStorageDataProviderTest : public testing::Test {
 protected:
  BlobStorageDataProviderTest() {
    mock_blob_storage_client_ = std::make_shared<MockBlobStorageClient>();
    match_data_provider_ =
        std::make_unique<BlobStorageDataProvider>(mock_blob_storage_client_);
  }

  std::shared_ptr<MockBlobStorageClient> mock_blob_storage_client_;
  std::unique_ptr<DataProviderInterface> match_data_provider_;
};

// Helper mock to simulate fetching match data from object storage.
// Uses the raw data in kSampleData.
ExecutionResult MockGetBlobWithData(
    AsyncContext<GetBlobRequest, GetBlobResponse> context) {
  context.result = SuccessExecutionResult();
  context.response = std::make_shared<GetBlobResponse>();
  *context.response->mutable_blob()->mutable_data() = kSampleData;
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate an error on fetching data from object storage.
ExecutionResult MockGetBlobWithContextError(
    AsyncContext<GetBlobRequest, GetBlobResponse> context) {
  context.result = FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate an error on the response of an object storage fetch.
ExecutionResult MockGetBlobWithResponseProtoError(
    AsyncContext<GetBlobRequest, GetBlobResponse> context) {
  context.result = SuccessExecutionResult();
  context.response = std::make_shared<GetBlobResponse>();
  *context.response->mutable_result() =
      FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR).ToProto();
  context.Finish();
  return SuccessExecutionResult();
}

TEST_F(BlobStorageDataProviderTest, StartStop) {
  BlobStorageDataProvider match_data_provider(mock_blob_storage_client_);

  EXPECT_THAT(match_data_provider.Init(), IsSuccessful());
  EXPECT_THAT(match_data_provider.Run(), IsSuccessful());
  EXPECT_THAT(match_data_provider.Stop(), IsSuccessful());
}

TEST_F(BlobStorageDataProviderTest, GetWithDataYieldsSuccess) {
  std::promise<ExecutionResult> result_promise;
  std::promise<std::string> data_promise;

  AsyncContext<Location, std::string> context;
  context.request = std::make_shared<Location>();
  *context.request->mutable_blob_storage_location()->mutable_bucket_name() =
      kBucketName;
  *context.request->mutable_blob_storage_location()->mutable_path() = kPath;
  context.callback = [&result_promise, &data_promise](auto& context) -> void {
    result_promise.set_value(context.result);
    data_promise.set_value(*context.response);
  };
  EXPECT_CALL(*mock_blob_storage_client_, GetBlob)
      .WillOnce(MockGetBlobWithData);

  ExecutionResult get_result = match_data_provider_->Get(context);

  EXPECT_THAT(get_result, IsSuccessful());
  EXPECT_THAT(result_promise.get_future().get(), IsSuccessful());
  EXPECT_EQ(data_promise.get_future().get(), kSampleData);
}

TEST_F(BlobStorageDataProviderTest, GetWithContextError) {
  std::promise<ExecutionResult> result_promise;

  AsyncContext<Location, std::string> context;
  context.request = std::make_shared<Location>();
  *context.request->mutable_blob_storage_location()->mutable_bucket_name() =
      kBucketName;
  *context.request->mutable_blob_storage_location()->mutable_path() = kPath;
  context.callback = [&result_promise](auto& context) -> void {
    result_promise.set_value(context.result);
  };
  EXPECT_CALL(*mock_blob_storage_client_, GetBlob)
      .WillOnce(MockGetBlobWithContextError);

  ExecutionResult get_result = match_data_provider_->Get(context);

  EXPECT_THAT(get_result, IsSuccessful());
  EXPECT_THAT(
      result_promise.get_future().get(),
      ResultIs(FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR)));
}

TEST_F(BlobStorageDataProviderTest, GetWithResponseProtoError) {
  std::promise<ExecutionResult> result_promise;

  AsyncContext<Location, std::string> context;
  context.request = std::make_shared<Location>();
  *context.request->mutable_blob_storage_location()->mutable_bucket_name() =
      kBucketName;
  *context.request->mutable_blob_storage_location()->mutable_path() = kPath;
  context.callback = [&result_promise](auto& context) -> void {
    result_promise.set_value(context.result);
  };
  EXPECT_CALL(*mock_blob_storage_client_, GetBlob)
      .WillOnce(MockGetBlobWithResponseProtoError);

  ExecutionResult get_result = match_data_provider_->Get(context);

  EXPECT_THAT(get_result, IsSuccessful());
  EXPECT_THAT(
      result_promise.get_future().get(),
      ResultIs(FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR)));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
