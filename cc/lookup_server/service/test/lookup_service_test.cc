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

#include "cc/lookup_server/service/src/lookup_service.h"

#include <vector>

#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/utils/metric_instance/mock/mock_aggregate_metric.h"
#include "core/async_executor/mock/mock_async_executor.h"
#include "core/http2_server/mock/mock_http2_server.h"
#include "core/interface/async_context.h"
#include "core/interface/http_server_interface.h"
#include "gtest/gtest.h"
#include "public/core/interface/execution_result.h"
#include "public/cpio/mock/metric_client/mock_metric_client.h"

#include "cc/core/test/src/parse_text_proto.h"
#include "cc/lookup_server/coordinator_client/src/error_codes.h"
#include "cc/lookup_server/crypto_client/mock/mock_crypto_client.h"
#include "cc/lookup_server/crypto_client/mock/mock_crypto_key.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/match_data_storage/mock/mock_match_data_storage.h"
#include "cc/lookup_server/metric_client/mock/mock_metric_client.h"
#include "cc/lookup_server/service/mock/pass_through_lookup_service.h"
#include "cc/lookup_server/service/src/error_codes.h"
#include "cc/lookup_server/service/src/json_serialization_functions.h"
#include "protos/lookup_server/api/healthcheck.pb.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/lookup_server/backend/service_status.pb.h"
#include "protos/shared/api/errors/code.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::ParseTextProtoOrDie;
using ::google::confidential_match::lookup_server::proto_api::DataRecord;
using ::google::confidential_match::lookup_server::proto_api::
    HealthcheckResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
using ::google::confidential_match::lookup_server::proto_backend::ServiceStatus;
using ::google::confidential_match::shared::api_errors::Code;
using ::google::confidential_match::shared::api_errors::ErrorResponse;
using ::google::scp::core::AsyncContext;
using ::google::scp::core::AsyncExecutorInterface;
using ::google::scp::core::Byte;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpHeaders;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::async_executor::mock::MockAsyncExecutor;
using ::google::scp::core::http2_server::mock::MockHttp2Server;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::ResultIs;
using google::scp::core::test::WaitUntil;
using ::google::scp::cpio::MockAggregateMetric;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::AtMost;
using ::testing::Eq;
using ::testing::Return;

using HashInfo = ::google::confidential_match::lookup_server::proto_api::
    LookupRequest::HashInfo;
using SerializableDataRecords = ::google::confidential_match::lookup_server::
    proto_api::LookupRequest::SerializableDataRecords;

constexpr char kRequestCountMetricLabel[] = "Count";
constexpr char kRequestErrorMetricLabel[] = "Error";
constexpr char kRequestInvalidSchemeMetricLabel[] = "InvalidScheme";
constexpr absl::string_view kKeyId = "testKeyId";
constexpr absl::string_view kKeyServiceEndpoint1 =
    "https://test-privatekeyservice-1.google.com/v1alpha";
constexpr absl::string_view kKeyServiceEndpoint2 =
    "https://test-privatekeyservice-2.google.com/v1alpha";
constexpr absl::string_view kAccountIdentity1 =
    "test-verified-user@coordinator1.iam.gserviceaccount.com";
constexpr absl::string_view kAccountIdentity2 =
    "test-verified-user@coordinator2.iam.gserviceaccount.com";
constexpr absl::string_view kWipProvider1 =
    "projects/1/locations/global/workloadIdentityPools/wip/providers/provider";
constexpr absl::string_view kWipProvider2 =
    "projects/2/locations/global/workloadIdentityPools/wip/providers/provider";
constexpr absl::string_view kKeyServiceAudienceUrl1 =
    "https://test-key-service-audience-url-1.a.run.app";
constexpr absl::string_view kKeyServiceAudienceUrl2 =
    "https://test-key-service-audience-url-2.a.run.app";

class LookupServiceTest : public testing::Test {
 protected:
  LookupServiceTest()
      : mock_match_data_storage_(std::make_shared<MockMatchDataStorage>()),
        mock_http2_server_(std::make_shared<MockHttp2Server>()),
        mock_aead_crypto_client_(std::make_shared<MockCryptoClient>()),
        mock_hpke_crypto_client_(std::make_shared<MockCryptoClient>()),
        mock_request_aggregate_metric_(std::make_shared<MockAggregateMetric>()),
        mock_error_aggregate_metric_(std::make_shared<MockAggregateMetric>()),
        mock_invalid_scheme_aggregate_metric_(
            std::make_shared<MockAggregateMetric>()),
        mock_metric_client_(std::make_shared<MockMetricClient>()),
        lookup_service_(
            mock_match_data_storage_, mock_http2_server_,
            mock_aead_crypto_client_, mock_hpke_crypto_client_,
            mock_request_aggregate_metric_, mock_error_aggregate_metric_,
            mock_invalid_scheme_aggregate_metric_, mock_metric_client_,
            absl::flat_hash_map<std::string,
                                std::shared_ptr<StatusProviderInterface>>()) {}

  void SetUp() {
    EXPECT_CALL(*mock_request_aggregate_metric_, Init)
        .Times(AtMost(1))
        .WillRepeatedly(Return(SuccessExecutionResult()));
    EXPECT_CALL(*mock_error_aggregate_metric_, Init)
        .Times(AtMost(1))
        .WillRepeatedly(Return(SuccessExecutionResult()));
    EXPECT_CALL(*mock_invalid_scheme_aggregate_metric_, Init)
        .Times(AtMost(1))
        .WillRepeatedly(Return(SuccessExecutionResult()));
  }

  std::shared_ptr<MockMatchDataStorage> mock_match_data_storage_;
  std::shared_ptr<MockHttp2Server> mock_http2_server_;
  std::shared_ptr<MockCryptoClient> mock_aead_crypto_client_;
  std::shared_ptr<MockCryptoClient> mock_hpke_crypto_client_;
  std::shared_ptr<MockAggregateMetric> mock_request_aggregate_metric_;
  std::shared_ptr<MockAggregateMetric> mock_error_aggregate_metric_;
  std::shared_ptr<MockAggregateMetric> mock_invalid_scheme_aggregate_metric_;
  std::shared_ptr<MockMetricClient> mock_metric_client_;
  PassThroughLookupService lookup_service_;
};

// Helper to simulate a successful response from the crypto client.
void MockGetCryptoKeyWithResponse(
    const std::shared_ptr<MockCryptoKey>& response,
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
        get_key_context) noexcept {
  get_key_context.result = SuccessExecutionResult();
  get_key_context.response = response;
  get_key_context.Finish();
}

// Helper to simulate a failure result from the crypto client.
void MockGetCryptoKeyWithFailure(
    ExecutionResult failure_result,
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
        get_key_context) noexcept {
  get_key_context.result = failure_result;
  get_key_context.Finish();
}

TEST_F(LookupServiceTest, GetHealthcheckSuccess) {
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.response = std::make_shared<HttpResponse>();

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    HealthcheckResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    EXPECT_FALSE(response.response_id().empty());
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.GetHealthcheck(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, GetHealthcheckWithOkServiceStatus) {
  std::string service_name = "MockMatchDataStorage";
  ServiceStatus service_status = ServiceStatus::OK;

  absl::flat_hash_map<std::string, std::shared_ptr<StatusProviderInterface>>
      service_status_providers;
  service_status_providers[service_name] = mock_match_data_storage_;
  EXPECT_CALL(*mock_match_data_storage_, GetStatus)
      .WillOnce(Return(service_status));
  PassThroughLookupService lookup_service(
      mock_match_data_storage_, mock_http2_server_, mock_aead_crypto_client_,
      mock_hpke_crypto_client_, mock_request_aggregate_metric_,
      mock_error_aggregate_metric_, mock_invalid_scheme_aggregate_metric_,
      mock_metric_client_, service_status_providers);

  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::GET;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.response = std::make_shared<HttpResponse>();

  std::atomic<bool> finished = false;
  http_context
      .callback = [&finished, &service_name, &service_status](
                      AsyncContext<HttpRequest, HttpResponse>& http_context) {
    EXPECT_SUCCESS(http_context.result);
    HealthcheckResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    EXPECT_EQ(response.services().size(), 1);
    EXPECT_EQ(response.services(0).name(), service_name);
    EXPECT_EQ(response.services(0).status(),
              ServiceStatus_Name(service_status));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service.GetHealthcheck(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithValidRequestNoMatches) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, Get)
      .WillOnce(Return(std::vector<MatchDataRow>()));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record {
          lookup_key { key: "+16505551234" }
          metadata { key: "customerId" string_value: "1" }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupWithValidRequestSingleMatchNoAssociatedDataKeys) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": []
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get).WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record {
          lookup_key { key: "+16505551234" }
          metadata { key: "customerId" string_value: "1" }
        }
        matched_data_records { lookup_key { key: "+16505551234" } }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithValidRequestSingleMatch) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get).WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record {
          lookup_key { key: "+16505551234" }
          metadata { key: "customerId" string_value: "1" }
        }
        matched_data_records {
          lookup_key { key: "+16505551234" }
          associated_data { key: "user_id" int_value: 100 }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithValidRequestMultipleRecords) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [
          {
              "lookupKey": {
                  "key": "+16505551234"
              },
              "metadata": [{
                  "key": "customerId",
                  "stringValue": "1"
              }]
          },
          {
              "lookupKey": {
                  "key": "test@email.com"
              },
              "metadata": [{
                  "key": "customerId",
                  "stringValue": "2"
              }]
          },
      ],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id", "type"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match1_1;
  *match1_1.mutable_key() = "+16505551234";
  *match1_1.add_associated_data()->mutable_key() = "type";
  *match1_1.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match1_1.add_associated_data()->mutable_key() = "user_id";
  match1_1.mutable_associated_data(1)->set_int_value(101);
  MatchDataRow match1_2;
  *match1_2.mutable_key() = "+16505551234";
  *match1_2.add_associated_data()->mutable_key() = "type";
  *match1_2.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match1_2.add_associated_data()->mutable_key() = "user_id";
  match1_2.mutable_associated_data(1)->set_int_value(102);
  MatchDataRow match2_1;
  *match2_1.mutable_key() = "test@email.com";
  *match2_1.add_associated_data()->mutable_key() = "type";
  *match2_1.mutable_associated_data(0)->mutable_string_value() = "email";
  *match2_1.add_associated_data()->mutable_key() = "user_id";
  match2_1.mutable_associated_data(1)->set_int_value(103);
  std::vector<MatchDataRow> matches1 = {match1_1, match1_2};
  std::vector<MatchDataRow> matches2 = {match2_1};
  EXPECT_CALL(*mock_match_data_storage_, Get)
      .WillOnce(Return(matches1))
      .WillOnce(Return(matches2));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record {
          lookup_key { key: "+16505551234" }
          metadata { key: "customerId" string_value: "1" }
        }
        matched_data_records {
          lookup_key { key: "+16505551234" }
          associated_data { key: "user_id" int_value: 101 }
          associated_data { key: "type" string_value: "phone" }
        }
        matched_data_records {
          lookup_key { key: "+16505551234" }
          associated_data { key: "user_id" int_value: 102 }
          associated_data { key: "type" string_value: "phone" }
        }
        status: STATUS_SUCCESS
      }
      lookup_results {
        client_data_record {
          lookup_key { key: "test@email.com" }
          metadata { key: "customerId" string_value: "2" }
        }
        matched_data_records {
          lookup_key { key: "test@email.com" }
          associated_data { key: "user_id" int_value: 103 }
          associated_data { key: "type" string_value: "email" }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithValidBinaryFormatRequestSingleMatch) {
  EXPECT_SUCCESS(lookup_service_.Init());
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    data_records { lookup_key { key: "+16505551234" } }
    key_format: KEY_FORMAT_HASHED
    sharding_scheme { type: "jch" num_shards: 50 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
  )pb");
  std::string request_body = request.SerializeAsString();

  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {"content-type", "application/octet-stream"});
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get).WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_SUCCESS(http_context.result);
        absl::string_view response_bytes(
            http_context.response->body.bytes->data(),
            http_context.response->body.length);
        LookupResponse response;
        response.ParseFromString(response_bytes);
        ASSERT_EQ(response.lookup_results_size(), 1);
        LookupResponse expected = ParseTextProtoOrDie(R"pb(
          lookup_results {
            client_data_record { lookup_key { key: "+16505551234" } }
            matched_data_records { lookup_key { key: "+16505551234" } }
            status: STATUS_SUCCESS
          }
        )pb");
        EXPECT_THAT(response, EqualsProto(expected));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithInvalidRequest) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({"invalid": "invalid"})";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, Get).Times(0);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupValidRequestWithOutdatedShardingScheme) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(false));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(0);
  EXPECT_CALL(*mock_invalid_scheme_aggregate_metric_,
              Increment(kRequestInvalidSchemeMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_THAT(http_context.result,
                ResultIs(FailureExecutionResult(
                    LOOKUP_SERVICE_INVALID_REQUEST_SCHEME)));
    ErrorResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    EXPECT_EQ(response.code(), Code::INVALID_ARGUMENT);
    EXPECT_EQ(response.details().size(), 1);
    EXPECT_EQ(response.details(0).reason(), "INVALID_SCHEME");
    EXPECT_EQ(response.details(0).domain(), "LookupService");
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithInvalidKeyFormat) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_UNSPECIFIED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, Get).Times(0);
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(http_context.result,
                    ResultIs(FailureExecutionResult(
                        LOOKUP_SERVICE_INVALID_KEY_FORMAT)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupWithMissingHashTypeReturnsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "shardingScheme": {
          "type": "jch",
          "numShards": 60
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupMultipleMetricCounterIncrements) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_match_data_storage_, Get)
      .Times(2)
      .WillRepeatedly(Return(std::vector<MatchDataRow>()));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .Times(2)
      .WillRepeatedly(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(2);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_SUCCESS(http_context.result);
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
  finished = false;
  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupBinaryFormatRequestMissingHeaderYieldsError) {
  EXPECT_SUCCESS(lookup_service_.Init());

  LookupRequest request;
  request.set_key_format(LookupRequest::KEY_FORMAT_HASHED);
  request.mutable_hash_info()->set_hash_type(HashInfo::HASH_TYPE_SHA_256);
  request.mutable_sharding_scheme()->set_type("jch");
  request.mutable_sharding_scheme()->set_num_shards(50);
  DataRecord* data_record = request.add_data_records();
  data_record->mutable_lookup_key()->set_key("+16505551234");
  std::string request_body = request.SerializeAsString();

  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupJsonRequestWithBinaryFormatHeaderYieldsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "+16505551234"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": []
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->headers->insert(
      {"content-type", "application/octet-stream"});
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupEncryptedWithValidRequestSingleMatch) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "encryptionKeyInfo": {
          "wrappedKeyInfo": {
              "encryptedDek": "testBase64EncryptedDek",
              "kekKmsResourceId": "test/kms/resource/id",
              "keyType": "KEY_TYPE_XCHACHA20_POLY1305",
              "kmsWipProvider": "test/kms/wip/provider"
          }
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt).WillOnce(Return("+16505551234"));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse, mock_crypto_key, _1));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record {
          lookup_key { key: "testEncryptedLookupKey" }
          metadata { key: "customerId" string_value: "1" }
        }
        matched_data_records {
          lookup_key { key: "testEncryptedLookupKey" }
          associated_data { key: "user_id" int_value: 100 }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupEncryptedWithValidGcpWrappedInfoSingleMatch) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "encryptionKeyInfo": {
          "wrappedKeyInfo": {
              "encryptedDek": "testBase64EncryptedDek",
              "kekKmsResourceId": "test/kms/resource/id",
              "keyType": "KEY_TYPE_XCHACHA20_POLY1305",
              "gcpWrappedKeyInfo": {
                  "wipProvider": "test/kms/wip/provider"
              }
          }
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt).WillOnce(Return("+16505551234"));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse, mock_crypto_key, _1));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record {
          lookup_key { key: "testEncryptedLookupKey" }
          metadata { key: "customerId" string_value: "1" }
        }
        matched_data_records {
          lookup_key { key: "testEncryptedLookupKey" }
          associated_data { key: "user_id" int_value: 100 }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupEncryptedWithPartialKeyDecryptionError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          }
      },
      {
          "lookupKey": {
              "key": "failedEncryptedLookupKey"
          }
      }],
      "encryptionKeyInfo": {
          "wrappedKeyInfo": {
              "encryptedDek": "testBase64EncryptedDek",
              "kekKmsResourceId": "test/kms/resource/id",
              "keyType": "KEY_TYPE_XCHACHA20_POLY1305",
              "kmsWipProvider": "test/kms/wip/provider"
          }
      },
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return("+16505551234"))
      .WillOnce(Return(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse, mock_crypto_key, _1));

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record { lookup_key { key: "testEncryptedLookupKey" } }
        matched_data_records {
          lookup_key { key: "testEncryptedLookupKey" }
          associated_data { key: "user_id" int_value: 100 }
        }
        status: STATUS_SUCCESS
      }
      lookup_results {
        client_data_record { lookup_key { key: "failedEncryptedLookupKey" } }
        status: STATUS_FAILED
        error_response {
          code: 3
          message: "An encryption/decryption error occurred while processing the request."
          details { reason: "2415853572" domain: "LookupService" }
        }
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupEncryptedWithMissingKeyInfoReturnsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_match_data_storage_, Get).Times(0);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey).Times(0);
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupEncryptedWithInvalidKmsReturnsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "encryptionKeyInfo": {
          "wrappedKeyInfo": {
              "encryptedDek": "testBase64EncryptedDek",
              "kekKmsResourceId": "test/kms/resource/id",
              "keyType": "KEY_TYPE_XCHACHA20_POLY1305",
              "kmsWipProvider": "test/kms/wip/provider"
          }
      },
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_match_data_storage_, Get).Times(0);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithFailure,
                          FailureExecutionResult(CRYPTO_CLIENT_GET_AEAD_ERROR),
                          _1));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_GET_AEAD_ERROR)));
        ErrorResponse error_response;
        EXPECT_TRUE(
            JsonBytesBufferToProto(http_context.response->body, &error_response)
                .ok());
        EXPECT_EQ(error_response.code(), 3);
        EXPECT_EQ(error_response.details(0).reason(), "2415853572");
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupEncryptedWithInvalidDekReturnsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "encryptionKeyInfo": {
          "wrappedKeyInfo": {
              "encryptedDek": "testBase64EncryptedDek",
              "kekKmsResourceId": "test/kms/resource/id",
              "keyType": "KEY_TYPE_XCHACHA20_POLY1305",
              "kmsWipProvider": "test/kms/wip/provider"
          }
      },
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_match_data_storage_, Get).Times(0);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(
          MockGetCryptoKeyWithFailure,
          FailureExecutionResult(CRYPTO_CLIENT_KEYSET_READ_ERROR), _1));

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_KEYSET_READ_ERROR)));
        ErrorResponse error_response;
        EXPECT_TRUE(
            JsonBytesBufferToProto(http_context.response->body, &error_response)
                .ok());
        EXPECT_EQ(error_response.code(), 3);
        EXPECT_EQ(error_response.details(0).reason(), "2415853572");
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest, PostLookupEncryptedWithInvalidKeyTypeReturnsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          },
          "metadata": [{
              "key": "customerId",
              "stringValue": "1"
          }]
      }],
      "encryptionKeyInfo": {
          "wrappedKeyInfo": {
              "encryptedDek": "testBase64EncryptedDek",
              "kekKmsResourceId": "test/kms/resource/id",
              "keyType": "KEY_TYPE_UNSPECIFIED",
              "kmsWipProvider": "test/kms/wip/provider"
          }
      },
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 50
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_match_data_storage_, Get).Times(0);
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKey).Times(0);

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(
            http_context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupCoordinatorEncryptedValidRequestSingleMatch) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "encryptedDataRecords": "testEncryptedDataRecords",
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "encryptionKeyInfo": {
          "coordinatorKeyInfo": {
              "coordinatorInfo": [
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-1.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-1.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator1.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
                  },
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-2.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-2.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator2.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
                  }
              ],
              "keyId": "testKeyId"
          }
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 60
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  SerializableDataRecords serializable_data_records;
  serializable_data_records.add_data_records()->mutable_lookup_key()->set_key(
      "+16505551234");
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(serializable_data_records.SerializeAsString()));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record { lookup_key { key: "+16505551234" } }
        matched_data_records {
          lookup_key { key: "+16505551234" }
          associated_data { key: "user_id" int_value: 100 }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupCoordinatorIndividuallyEncryptedValidRequestSingleMatch) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          }
      }],
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "encryptionKeyInfo": {
          "coordinatorKeyInfo": {
              "coordinatorInfo": [
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-1.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-1.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator1.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
                  },
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-2.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-2.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator2.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
                  }
              ],
              "keyId": "testKeyId"
          }
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 60
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt).WillOnce(Return("+16505551234"));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse, mock_crypto_key, _1));

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record { lookup_key { key: "testEncryptedLookupKey" } }
        matched_data_records {
          lookup_key { key: "testEncryptedLookupKey" }
          associated_data { key: "user_id" int_value: 100 }
        }
        status: STATUS_SUCCESS
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupCoordinatorEncryptedWithCoordinatorKeyErrorReturnsError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "testEncryptedLookupKey"
          }
      }],
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "encryptionKeyInfo": {
          "coordinatorKeyInfo": {
              "coordinatorInfo": [
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-1.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-1.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator1.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
                  },
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-2.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-2.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator2.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
                  }
              ],
              "keyId": "invalidKeyId"
          }
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 60
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_error_aggregate_metric_,
              Increment(kRequestErrorMetricLabel))
      .Times(1);
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(
          MockGetCryptoKeyWithFailure,
          FailureExecutionResult(COORDINATOR_CLIENT_KEY_FETCH_ERROR), _1));

  std::atomic<bool> finished = false;
  http_context.callback =
      [&finished](AsyncContext<HttpRequest, HttpResponse>& http_context) {
        EXPECT_THAT(http_context.result,
                    ResultIs(FailureExecutionResult(
                        COORDINATOR_CLIENT_KEY_FETCH_ERROR)));
        ErrorResponse error_response;
        EXPECT_TRUE(
            JsonBytesBufferToProto(http_context.response->body, &error_response)
                .ok());
        EXPECT_EQ(error_response.code(), 3);
        EXPECT_EQ(error_response.details(0).domain(), "LookupService");
        EXPECT_EQ(error_response.details(0).reason(), "2415853572");
        finished = true;
      };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(LookupServiceTest,
       PostLookupCoordinatorEncryptedWithSingleHashDecryptionError) {
  EXPECT_SUCCESS(lookup_service_.Init());
  std::string request_body = R"({
      "dataRecords": [{
          "lookupKey": {
              "key": "failedEncryptedLookupKey"
          }
      }],
      "keyFormat": "KEY_FORMAT_HASHED_ENCRYPTED",
      "hashInfo": {
        "hashType": "HASH_TYPE_SHA_256"
      },
      "encryptionKeyInfo": {
          "coordinatorKeyInfo": {
              "coordinatorInfo": [
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-1.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-1.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator1.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
                  },
                  {
                      "keyServiceEndpoint": "https://test-privatekeyservice-2.google.com/v1alpha",
                      "keyServiceAudienceUrl": "https://test-key-service-audience-url-2.a.run.app",
                      "kmsIdentity": "test-verified-user@coordinator2.iam.gserviceaccount.com",
                      "kmsWipProvider": "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
                  }
              ],
              "keyId": "testKeyId"
          }
      },
      "shardingScheme": {
          "type": "jch",
          "numShards": 60
      },
      "associatedDataKeys": ["user_id"]
  })";
  AsyncContext<HttpRequest, HttpResponse> http_context;
  http_context.request = std::make_shared<HttpRequest>();
  http_context.request->method = scp::core::HttpMethod::POST;
  http_context.request->headers = std::make_shared<HttpHeaders>();
  http_context.request->body.bytes = std::make_shared<std::vector<Byte>>(
      request_body.begin(), request_body.end());
  http_context.request->body.length = request_body.length();
  http_context.request->body.capacity = request_body.capacity();
  http_context.response = std::make_shared<HttpResponse>();

  EXPECT_CALL(*mock_match_data_storage_, IsValidRequestScheme)
      .WillOnce(Return(true));
  EXPECT_CALL(*mock_metric_client_, RecordMetric)
      .WillOnce(Return(SuccessExecutionResult()));
  EXPECT_CALL(*mock_request_aggregate_metric_,
              Increment(kRequestCountMetricLabel))
      .Times(1);
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  std::atomic<bool> finished = false;
  http_context.callback = [&finished](AsyncContext<HttpRequest, HttpResponse>&
                                          http_context) {
    EXPECT_SUCCESS(http_context.result);
    LookupResponse response;
    EXPECT_TRUE(
        JsonBytesBufferToProto(http_context.response->body, &response).ok());
    LookupResponse expected = ParseTextProtoOrDie(R"pb(
      lookup_results {
        client_data_record { lookup_key { key: "failedEncryptedLookupKey" } }
        status: STATUS_FAILED
        error_response {
          code: 3
          message: "An encryption/decryption error occurred while processing the request."
          details { reason: "2415853572" domain: "LookupService" }
        }
      }
    )pb");
    EXPECT_THAT(response, EqualsProto(expected));
    finished = true;
  };

  EXPECT_SUCCESS(lookup_service_.PostLookup(http_context));
  WaitUntil([&]() { return finished.load(); });
}

}  // namespace google::confidential_match::lookup_server
