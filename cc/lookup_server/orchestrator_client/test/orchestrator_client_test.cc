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

#include "cc/lookup_server/orchestrator_client/src/orchestrator_client.h"

#include <memory>
#include <string>

#include "absl/strings/str_format.h"
#include "cc/core/http2_client/mock/mock_http_client.h"
#include "cc/core/interface/http_types.h"
#include "cc/core/interface/type_def.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"

#include "cc/lookup_server/orchestrator_client/mock/fake_orchestrator_client.h"
#include "cc/lookup_server/orchestrator_client/src/error_codes.h"
#include "protos/lookup_server/backend/data_export_info.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::lookup_server::proto_backend::
    DataExportInfo;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::ExecutionResultOr;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpClientInterface;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::http2_client::mock::MockHttpClient;
using ::google::scp::core::test::IsSuccessful;
using ::google::scp::core::test::ResultIs;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;

constexpr absl::string_view kOrchestratorHostAddress =
    "orchestrator.test.google.com";
constexpr absl::string_view kClusterGroupId = "cluster-group-id";
constexpr absl::string_view kClusterId = "cluster-id";
constexpr absl::string_view kAuthenticationToken = "token";
constexpr absl::string_view kDataExportId = "data-export-id";
constexpr absl::string_view kBucketName = "bucket-name";
constexpr absl::string_view kExportMetadataPath = "export-metadata-filename";
constexpr absl::string_view kShardDirectory = "test/shard-data/";
constexpr absl::string_view kShardingSchemeType = "jch";
constexpr uint64_t kNumShards = 2;

class OrchestratorClientTest : public testing::Test {
 protected:
  OrchestratorClientTest()
      : mock_http1_client_(std::make_shared<MockHttpClient>()),
        mock_http2_client_(std::make_shared<MockHttpClient>()),
        orchestrator_client_(std::make_unique<OrchestratorClient>(
            // std::make_shared<MockHttpClient>(),
            // std::make_shared<MockHttpClient>())) {}
            mock_http1_client_, mock_http2_client_, kOrchestratorHostAddress)) {
  }

  void SetUp() override {
    EXPECT_TRUE(orchestrator_client_->Init().Successful());
  }

  std::shared_ptr<MockHttpClient> mock_http1_client_;
  std::shared_ptr<MockHttpClient> mock_http2_client_;
  std::unique_ptr<OrchestratorClientInterface> orchestrator_client_;
};

TEST_F(OrchestratorClientTest, StartStop) {
  OrchestratorClient orchestrator_client(mock_http1_client_, mock_http2_client_,
                                         kOrchestratorHostAddress);
  EXPECT_TRUE(orchestrator_client.Init().Successful());
  EXPECT_TRUE(orchestrator_client.Run().Successful());
  EXPECT_TRUE(orchestrator_client.Stop().Successful());
}

// Helper to simulate a response from the VM instance metadata endpoint.
ExecutionResult MockPerformRequestToInstanceMetadataEndpoint(
    AsyncContext<HttpRequest, HttpResponse>& context) {
  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer(std::string(kAuthenticationToken));
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

// Helper to simulate an orchestrator response with valid data.
// Captures and returns the request query parameters.
ExecutionResult MockPerformRequestToOrchestrator(
    std::string* request_query,
    AsyncContext<HttpRequest, HttpResponse>& context) {
  *request_query = *context.request->query;

  context.response = std::make_shared<HttpResponse>();

  DataExportInfo info;
  *info.mutable_data_export_id() = kDataExportId;
  *info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *info.mutable_shard_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kShardDirectory;
  *info.mutable_metadata_location()
       ->mutable_blob_storage_location()
       ->mutable_bucket_name() = kBucketName;
  *info.mutable_metadata_location()
       ->mutable_blob_storage_location()
       ->mutable_path() = kExportMetadataPath;
  info.mutable_sharding_scheme()->set_num_shards(kNumShards);
  *info.mutable_sharding_scheme()->mutable_type() = kShardingSchemeType;
  std::string serialized_metadata;
  EXPECT_TRUE(MessageToJsonString(info, &serialized_metadata).ok());

  context.response->body = BytesBuffer(serialized_metadata);
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

TEST_F(OrchestratorClientTest, GetDataExportInfoIsSuccessful) {
  std::string captured_request_query;
  mock_http1_client_->perform_request_mock =
      MockPerformRequestToInstanceMetadataEndpoint;
  mock_http2_client_->perform_request_mock =
      std::bind(MockPerformRequestToOrchestrator, &captured_request_query, _1);
  GetDataExportInfoRequest request = {
      .cluster_group_id = std::string(kClusterGroupId),
      .cluster_id = std::string(kClusterId)};

  ExecutionResultOr<GetDataExportInfoResponse> result_or =
      orchestrator_client_->GetDataExportInfo(request);

  EXPECT_THAT(result_or.result(), IsSuccessful());
  EXPECT_THAT(captured_request_query,
              HasSubstr(absl::StrFormat("clusterId=%s", kClusterId)));
  EXPECT_THAT(captured_request_query,
              HasSubstr(absl::StrFormat("clusterGroupId=%s", kClusterGroupId)));
  DataExportInfo& data_export_info = *result_or->data_export_info;
  EXPECT_EQ(
      data_export_info.shard_location().blob_storage_location().bucket_name(),
      kBucketName);
  EXPECT_EQ(data_export_info.shard_location().blob_storage_location().path(),
            kShardDirectory);
  EXPECT_EQ(data_export_info.metadata_location()
                .blob_storage_location()
                .bucket_name(),
            kBucketName);
  EXPECT_EQ(data_export_info.metadata_location().blob_storage_location().path(),
            kExportMetadataPath);
  EXPECT_EQ(data_export_info.sharding_scheme().type(), kShardingSchemeType);
  EXPECT_EQ(data_export_info.sharding_scheme().num_shards(), kNumShards);
}

TEST_F(OrchestratorClientTest,
       GetDataExportInfoWithAuthenticationErrorYieldsFailure) {
  std::string captured_request_query;
  mock_http1_client_->http_get_result_mock = FailureExecutionResult(SC_UNKNOWN);
  mock_http2_client_->perform_request_mock =
      std::bind(MockPerformRequestToOrchestrator, &captured_request_query, _1);
  GetDataExportInfoRequest request = {
      .cluster_group_id = std::string(kClusterGroupId),
      .cluster_id = std::string(kClusterId)};

  ExecutionResultOr<GetDataExportInfoResponse> result_or =
      orchestrator_client_->GetDataExportInfo(request);

  EXPECT_THAT(result_or.result(), ResultIs(FailureExecutionResult(SC_UNKNOWN)));
}

TEST_F(OrchestratorClientTest, GetDataExportInfoWithFetchErrorYieldsFailure) {
  std::string captured_request_query;
  mock_http1_client_->perform_request_mock =
      MockPerformRequestToInstanceMetadataEndpoint;
  mock_http2_client_->http_get_result_mock =
      FailureExecutionResult(ORCHESTRATOR_CLIENT_PARSE_ERROR);
  GetDataExportInfoRequest request = {
      .cluster_group_id = std::string(kClusterGroupId),
      .cluster_id = std::string(kClusterId)};

  ExecutionResultOr<GetDataExportInfoResponse> result_or =
      orchestrator_client_->GetDataExportInfo(request);

  EXPECT_THAT(
      result_or.result(),
      ResultIs(FailureExecutionResult(ORCHESTRATOR_CLIENT_PARSE_ERROR)));
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
