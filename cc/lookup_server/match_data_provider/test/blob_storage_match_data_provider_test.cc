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

#include "cc/lookup_server/match_data_provider/src/blob_storage_match_data_provider.h"

#include <memory>
#include <string>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/streaming_context.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/interface/blob_storage_client/blob_storage_client_interface.h"
#include "cc/public/cpio/mock/blob_storage_client/mock_blob_storage_client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/crypto_client/mock/fake_crypto_key.h"
#include "cc/lookup_server/interface/streamed_match_data_provider_interface.h"
#include "cc/lookup_server/match_data_provider/mock/mock_data_provider.h"
#include "cc/lookup_server/match_data_provider/src/error_codes.h"
#include "cc/lookup_server/parsers/src/error_codes.h"
#include "protos/lookup_server/backend/location.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobStreamRequest;
using ::google::cmrt::sdk::blob_storage_service::v1::GetBlobStreamResponse;
using ::google::cmrt::sdk::blob_storage_service::v1::ListBlobsMetadataRequest;
using ::google::cmrt::sdk::blob_storage_service::v1::ListBlobsMetadataResponse;
using ::google::confidential_match::lookup_server::proto_backend::Location;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::ConsumerStreamingContext;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::common::ConcurrentQueue;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::MockBlobStorageClient;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Return;
using ::testing::UnorderedElementsAre;

constexpr absl::string_view kBucketName = "test-bucket";
constexpr absl::string_view kPath = "test/path/";
constexpr absl::string_view kPathWithoutTrailingSlash = "test/path";
constexpr absl::string_view kFile = "test/path/fragment-00000-of-00001.txt";
constexpr absl::string_view kFile2 = "test/path/fragment-00001-of-00002.txt";
constexpr absl::string_view kFileSubFragment =
    "test/path/fragment-3-00001-of-00002.txt";
constexpr absl::string_view kIgnoredFile = "test/path/ignored/";
constexpr uint64_t kMaxConcurrentFileReads = 100;

// Sample raw encoded match data from a match data source.
// Contents:
//
// data_key: "key"
// associated_data {
//   metadata {
//     key: "type"
//     string_value: "P"
//   }
//   metadata {
//     key: "encrypted_user_id"
//     bytes_value: "user_id"
//   }
// }
constexpr absl::string_view kRawMatchData =
    "CgNrZXkSKQoJCgR0eXBlEgFQChwKEWVuY3J5cHRlZF91c2VyX2lkMgd1c2VyX2lk";
// The match data record key contained within kRawMatchData.
constexpr absl::string_view kMatchDataKey = "key";

// Sample raw encoded match data provided by a match data source, containing
// record metadata split into chunks.
// Contents:
//
// data_key: "key2"
// associated_data {
//   metadata {
//     key: "type"
//     string_value: "P"
//   }
//   metadata {
//     key: "encrypted_user_id"
//     bytes_value: "user_id"
//   }
// }

constexpr absl::string_view kRawMatchData2 =
    "CgRrZXkyEikKCQoEdHlwZRIBUAocChFlbmNyeXB0ZWRfdXNlcl9pZDIHdXNlcl9pZA==";
// The match data record key contained within kRawMatchData2 chunks.
constexpr absl::string_view kMatchDataKey2 = "key2";

constexpr absl::string_view kInvalidMatchData = "invalid-base64!";

class BlobStorageMatchDataProviderTest : public testing::Test {
 protected:
  BlobStorageMatchDataProviderTest()
      : mock_blob_storage_client_(std::make_shared<MockBlobStorageClient>()),
        mock_blob_storage_data_provider_(std::make_shared<MockDataProvider>()),
        crypto_key_(std::make_shared<FakeCryptoKey>()),
        match_data_provider_(std::make_unique<BlobStorageMatchDataProvider>(
            mock_blob_storage_client_, mock_blob_storage_data_provider_,
            kMaxConcurrentFileReads)) {}

  std::shared_ptr<MockBlobStorageClient> mock_blob_storage_client_;
  std::shared_ptr<MockDataProvider> mock_blob_storage_data_provider_;
  std::shared_ptr<CryptoKeyInterface> crypto_key_;
  std::unique_ptr<StreamedMatchDataProviderInterface> match_data_provider_;
};

// Helper to set up a server streaming context.
ConsumerStreamingContext<Location, MatchDataBatch>
BuildConsumerStreamingContext(absl::string_view bucket_name,
                              absl::string_view path) {
  ConsumerStreamingContext<Location, MatchDataBatch> context;
  context.request = std::make_shared<Location>();
  *context.request->mutable_blob_storage_location()->mutable_bucket_name() =
      bucket_name;
  *context.request->mutable_blob_storage_location()->mutable_path() = path;
  context.process_callback = [](auto& context, bool finished) {};
  return context;
}

// Helper mock to simulate listing an empty directory.
ExecutionResult MockListBlobsMetadataWithEmptyDirectory(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate listing a directory with one path.
ExecutionResult MockListBlobsMetadataWithSinglePath(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  if (!absl::EndsWith(context.request->blob_metadata().blob_name(), "/")) {
    return FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
  }
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  auto* metadata = context.response->add_blob_metadatas();
  *metadata->mutable_bucket_name() = kBucketName;
  *metadata->mutable_blob_name() = kFile;
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate listing a directory with one path, using the file
// naming format for when the export is split into sub-fragments.
ExecutionResult MockListBlobsMetadataWithSinglePathSubFragment(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  if (!absl::EndsWith(context.request->blob_metadata().blob_name(), "/")) {
    return FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
  }
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  auto* metadata = context.response->add_blob_metadatas();
  *metadata->mutable_bucket_name() = kBucketName;
  *metadata->mutable_blob_name() = kFileSubFragment;
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate a directory with multiple paths.
ExecutionResult MockListBlobsMetadataWithMultiplePaths(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  auto* metadata = context.response->add_blob_metadatas();
  *metadata->mutable_bucket_name() = kBucketName;
  *metadata->mutable_blob_name() = kFile;
  auto* metadata2 = context.response->add_blob_metadatas();
  *metadata2->mutable_bucket_name() = kBucketName;
  *metadata2->mutable_blob_name() = kFile2;
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate listing the first page of a paginated response.
ExecutionResult MockListBlobsMetadataWithFirstPage(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  auto* metadata = context.response->add_blob_metadatas();
  *metadata->mutable_bucket_name() = kBucketName;
  *metadata->mutable_blob_name() = kFile;
  *context.response->mutable_next_page_token() = kFile;
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate listing the second page of a paginated response.
ExecutionResult MockListBlobsMetadataWithSecondPage(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  auto* metadata = context.response->add_blob_metadatas();
  *metadata->mutable_bucket_name() = kBucketName;
  *metadata->mutable_blob_name() = kFile2;
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate a directory with an ignored path.
ExecutionResult MockListBlobsMetadataWithIgnoredPath(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  auto* metadata = context.response->add_blob_metadatas();
  *metadata->mutable_bucket_name() = kBucketName;
  *metadata->mutable_blob_name() = kIgnoredFile;
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate an error when listing the directory.
ExecutionResult MockListBlobsMetadataError(
    AsyncContext<ListBlobsMetadataRequest, ListBlobsMetadataResponse> context) {
  context.response = std::make_shared<ListBlobsMetadataResponse>();
  context.result = FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
  context.Finish();
  return FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR);
}

// Helper mock to simulate fetching raw match data from object storage.
// Uses the raw data in kRawMatchData.
ExecutionResult MockGetBlobWithMatchData(
    AsyncContext<Location, std::string> context) {
  context.response = std::make_shared<std::string>(kRawMatchData);
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate fetching raw match data from object storage.
// Uses the raw data in kRawMatchData2.
ExecutionResult MockGetBlobWithMatchData2(
    AsyncContext<Location, std::string> context) {
  context.response = std::make_shared<std::string>(kRawMatchData2);
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate fetching invalid raw match data from object storage.
ExecutionResult MockGetBlobWithInvalidMatchData(
    AsyncContext<Location, std::string> context) {
  context.response = std::make_shared<std::string>(kInvalidMatchData);
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper mock to simulate an immediate error on fetching match data.
// Uses the raw data in kRawMatchData.
ExecutionResult MockGetBlobImmediateError(
    AsyncContext<Location, std::string> context) {
  return FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
}

// Helper mock to simulate an async error on fetching match data.
// Uses the raw data in kRawMatchData.
ExecutionResult MockGetBlobAsyncError(
    AsyncContext<Location, std::string> context) {
  context.result = FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
  context.Finish();
  return SuccessExecutionResult();
  return FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR);
}

MATCHER_P(HasKey, key, "") {
  return testing::ExplainMatchResult(Eq(key), arg.key(), result_listener);
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithEmptyDirectoryYieldsSuccess) {
  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  std::atomic_bool callback_invoked = false;
  context.process_callback = [&callback_invoked](auto& context, bool finished) {
    EXPECT_EQ(context.TryGetNextResponse(), nullptr);
    EXPECT_THAT(context.result, IsSuccessful());
    if (finished) {
      callback_invoked.store(finished);
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithEmptyDirectory);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&callback_invoked]() { return callback_invoked.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
}

TEST_F(BlobStorageMatchDataProviderTest, GetMatchDataWithListErrorYieldsError) {
  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  std::atomic_bool is_finished = false;
  context.process_callback = [&is_finished](auto& context, bool finished) {
    EXPECT_THAT(
        context.result,
        ResultIs(FailureExecutionResult(MATCH_DATA_PROVIDER_LIST_ERROR)));
    is_finished = true;
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataError);
  EXPECT_SUCCESS(match_data_provider_->GetMatchData(context, crypto_key_));
  WaitUntil([&is_finished]() { return is_finished.load(); });
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithSingleFileYieldsSuccess) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &results,
                              &context_result](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(finished);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithSinglePath);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithMatchData);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result, IsSuccessful());
  EXPECT_THAT(results, ElementsAre(ElementsAre(HasKey(kMatchDataKey))));
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithSubFragmentPathYieldsSuccess) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &results,
                              &context_result](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(finished);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithSinglePathSubFragment);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithMatchData);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result, IsSuccessful());
  EXPECT_THAT(results, ElementsAre(ElementsAre(HasKey(kMatchDataKey))));
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithMultipleFilesYieldsSuccess) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &context_result,
                              &results](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithMultiplePaths);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithMatchData)
      .WillOnce(MockGetBlobWithMatchData2);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result, IsSuccessful());
  EXPECT_THAT(results,
              UnorderedElementsAre(ElementsAre(HasKey(kMatchDataKey)),
                                   ElementsAre(HasKey(kMatchDataKey2))));
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithPaginatedMultipleFilesYieldsSuccess) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &context_result,
                              &results](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithFirstPage)
      .WillOnce(MockListBlobsMetadataWithSecondPage);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithMatchData)
      .WillOnce(MockGetBlobWithMatchData2);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result, IsSuccessful());
  EXPECT_THAT(results,
              UnorderedElementsAre(ElementsAre(HasKey(kMatchDataKey)),
                                   ElementsAre(HasKey(kMatchDataKey2))));
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithIgnoredPathYieldsSuccess) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &context_result,
                              &results](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      EXPECT_THAT(context.result, IsSuccessful());
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithIgnoredPath);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get).Times(0);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result, IsSuccessful());
  EXPECT_TRUE(results.empty());
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithSingleFileInvalidMatchDataYieldsError) {
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&is_finished, &results, &context_result](
                                 auto& context, bool finished) {
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      EXPECT_THAT(context.result, IsSuccessful());
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithSinglePath);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithInvalidMatchData);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result,
              ResultIs(FailureExecutionResult(PARSER_INVALID_BASE64_DATA)));
  EXPECT_THAT(results, IsEmpty());
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithSingleFileImmediateError) {
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&is_finished, &results, &context_result](
                                 auto& context, bool finished) {
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      EXPECT_THAT(context.result, IsSuccessful());
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithSinglePath);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobImmediateError);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(
      context_result,
      ResultIs(FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR)));
  EXPECT_THAT(results, IsEmpty());
}

TEST_F(BlobStorageMatchDataProviderTest, GetMatchDataWithSingleFileAsyncError) {
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&is_finished, &results, &context_result](
                                 auto& context, bool finished) {
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      EXPECT_THAT(context.result, IsSuccessful());
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithSinglePath);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobAsyncError);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(
      context_result,
      ResultIs(FailureExecutionResult(MATCH_DATA_PROVIDER_FETCH_ERROR)));
  EXPECT_THAT(results, IsEmpty());
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithMultipleFilesOneSuccessThenFailureYieldsError) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &context_result,
                              &results](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(true);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithMultiplePaths);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithMatchData)
      .WillOnce(MockGetBlobWithInvalidMatchData);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result,
              ResultIs(FailureExecutionResult(PARSER_INVALID_BASE64_DATA)));
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataWithMultipleFilesOneFailureThenSuccessYieldsError) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPath);
  context.process_callback = [&callback_mutex, &is_finished, &context_result,
                              &results](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(finished);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithMultiplePaths);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithInvalidMatchData)
      .WillOnce(MockGetBlobWithMatchData);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result,
              ResultIs(FailureExecutionResult(PARSER_INVALID_BASE64_DATA)));
}

TEST_F(BlobStorageMatchDataProviderTest,
       GetMatchDataPathWithoutTrailingSlashYieldsSuccess) {
  absl::Mutex callback_mutex;
  std::atomic_bool is_finished = false;
  ExecutionResult context_result;
  MatchDataBatch results;

  ConsumerStreamingContext<Location, MatchDataBatch> context =
      BuildConsumerStreamingContext(kBucketName, kPathWithoutTrailingSlash);
  context.process_callback = [&callback_mutex, &is_finished, &results,
                              &context_result](auto& context, bool finished) {
    absl::MutexLock lock(&callback_mutex);
    if (!context.IsMarkedDone()) {
      std::unique_ptr<MatchDataBatch> data_batch = context.TryGetNextResponse();
      ASSERT_NE(data_batch, nullptr);
      for (const auto& item : *data_batch) {
        results.push_back(item);
      }
    }
    if (finished) {
      is_finished.store(finished);
      context_result = context.result;
    }
  };
  EXPECT_CALL(*mock_blob_storage_client_, ListBlobsMetadata)
      .WillOnce(MockListBlobsMetadataWithSinglePath);
  EXPECT_CALL(*mock_blob_storage_data_provider_, Get)
      .WillOnce(MockGetBlobWithMatchData);

  ExecutionResult result =
      match_data_provider_->GetMatchData(context, crypto_key_);

  EXPECT_THAT(result, IsSuccessful());
  WaitUntil([&is_finished]() { return is_finished.load(); });
  EXPECT_TRUE(context.IsMarkedDone());
  EXPECT_THAT(context_result, IsSuccessful());
  EXPECT_THAT(results, ElementsAre(ElementsAre(HasKey(kMatchDataKey))));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
