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

#include "cc/match_service/lookup_service_shard_client/lookup_service_shard_client.h"

#include <memory>
#include <string>

#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "cc/core/http2_client/mock/mock_http_client.h"
#include "cc/core/interface/http_types.h"
#include "cc/core/interface/type_def.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/match_service/auth_token_client/mock_auth_token_client.h"
#include "cc/match_service/error/error.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "gmock/gmock.h"
#include "google/protobuf/util/json_util.h"
#include "gtest/gtest.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/match_service/backend/error.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::match_service::backend::Error;
using ::google::protobuf::util::JsonStringToMessage;
using ::google::protobuf::util::MessageToJsonString;
using ::google::scp::core::BytesBuffer;
using ::google::scp::core::ExecutionResult;
using ::google::scp::core::FailureExecutionResult;
using ::google::scp::core::HttpMethod;
using ::google::scp::core::HttpRequest;
using ::google::scp::core::HttpResponse;
using ::google::scp::core::SuccessExecutionResult;
using ::google::scp::core::http2_client::mock::MockHttpClient;
using ::google::scp::core::test::EqualsProto;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;

constexpr absl::string_view kLookupServiceShardAddress =
    "lookup-server-0.lookupservice-test.google.com/v1/lookup";
constexpr absl::string_view kAuthenticationToken = "token";
constexpr absl::string_view kEmail = "test@example.com";
constexpr absl::string_view kAudience = "lookupAudience";
constexpr absl::string_view kLookupRequestJson = R"({
  "dataRecords": [
    {
      "lookupKey": {
        "key": "12345"
      }
    }
  ],
  "associatedDataKeys": [
    "encrypted_gaia_id",
  ],
  "keyFormat": "KEY_FORMAT_HASHED",
  "hashInfo": {
    "hashType": "HASH_TYPE_SHA_256"
  },
  "shardingScheme": {
    "numShards": "2",
    "type": "jch"
  }
}
)";
constexpr absl::string_view kLookupResponseJson = R"({
  "lookupResults": [
    {
      "clientDataRecord": {
        "lookupKey": {
          "key": "12345"
        }
      },
     "matchedDataRecords": [
        {
          "lookupKey": {
            "key": "12345"
          },
          "associatedData": [
            {
              "key": "encrypted_gaia_id",
              "bytesValue":"0000"
            },
            {
              "key": "pii_type",
              "stringValue": "E"
            }
          ]
        }
      ],
      "status": "STATUS_SUCCESS"
    }
  ]
}
)";

class LookupServiceShardClientTest : public testing::Test {
 protected:
  LookupServiceShardClientTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        lookup_client_(std::make_unique<LookupServiceShardClient>(
            &mock_auth_token_client_, &mock_http_client_, kAudience)) {}

  std::shared_ptr<LoggerInterface> logger_;
  MockAuthTokenClient mock_auth_token_client_;
  MockHttpClient mock_http_client_;
  std::unique_ptr<LookupServiceShardClientInterface> lookup_client_;
};

TEST_F(LookupServiceShardClientTest, StartStop) {
  LookupServiceShardClient lookup_client(&mock_auth_token_client_,
                                         &mock_http_client_, kAudience);
  EXPECT_TRUE(lookup_client.Init().ok());
  EXPECT_TRUE(lookup_client.Run().ok());
  EXPECT_TRUE(lookup_client.Stop().ok());
}

// Helper to mock auth token
void MockGetAuthToken(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context) {
  EXPECT_EQ(context.request->audience(), kAudience);
  GetAuthTokenResponse response;
  response.set_token(kAuthenticationToken);
  context.response = std::make_shared<GetAuthTokenResponse>(response);
  context.Finish();
}

// Helper to mock auth token failure
void MockGetAuthTokenFailure(
    AsyncContext<GetAuthTokenRequest, GetAuthTokenResponse> context) {
  context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, "GetAuthToken error");
  context.Finish();
}

// Helper to mock email call
void MockGetServiceAccountEmail(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
        context) {
  GetServiceAccountEmailResponse response;
  response.set_email(kEmail);
  context.response = std::make_shared<GetServiceAccountEmailResponse>(response);
  context.Finish();
}

// Helper to mock email call failure
void MockGetServiceAccountEmailFailure(
    AsyncContext<GetServiceAccountEmailRequest, GetServiceAccountEmailResponse>
        context) {
  context.status = Status(Error::AUTH_TOKEN_CLIENT_ERROR, "GetSAEmail error");
  context.Finish();
}

// Helper to simulate a lookup service response with valid data.
// Captures and returns the request body.
ExecutionResult MockPerformRequestToLookupService(
    std::string* request_body, std::string* request_path,
    HttpMethod* request_method,
    scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
  *request_body = context.request->body.ToString();
  *request_path = *context.request->path;
  *request_method = context.request->method;
  context.response = std::make_shared<HttpResponse>();
  context.response->body = BytesBuffer(std::string(kLookupResponseJson));
  context.result = SuccessExecutionResult();
  context.Finish();
  return SuccessExecutionResult();
}

TEST_F(LookupServiceShardClientTest, LookupIsSuccessful) {
  LookupRequest request_proto;
  EXPECT_TRUE(JsonStringToMessage(kLookupRequestJson, &request_proto).ok());
  LookupResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kLookupResponseJson, &expected_response).ok());
  std::string captured_request_body;
  std::string captured_request_path;
  HttpMethod captured_request_method;
  mock_http_client_.perform_request_mock =
      std::bind(MockPerformRequestToLookupService, &captured_request_body,
                &captured_request_path, &captured_request_method, _1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(MockGetAuthToken);
  EXPECT_CALL(mock_auth_token_client_, GetServiceAccountEmail)
      .WillOnce(MockGetServiceAccountEmail);
  AsyncContext<LookupRequest, LookupResponse> lookup_context(
      std::make_shared<LookupRequest>(request_proto),
      [&expected_response](
          AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected_response));
        EXPECT_EQ(context.response->lookup_results_size(), 1);
      },
      logger_);

  lookup_client_->Lookup(lookup_context, kLookupServiceShardAddress);

  EXPECT_EQ(captured_request_method, HttpMethod::POST);
  EXPECT_EQ(captured_request_path, kLookupServiceShardAddress);
  LookupRequest sent_request;
  EXPECT_TRUE(JsonStringToMessage(captured_request_body, &sent_request).ok());
  EXPECT_THAT(sent_request, EqualsProto(request_proto));
}

TEST_F(LookupServiceShardClientTest, LookupWithFetchErrorYieldsFailure) {
  LookupRequest request_proto;
  EXPECT_TRUE(JsonStringToMessage(kLookupRequestJson, &request_proto).ok());
  mock_http_client_.perform_request_mock =
      [](scp::core::AsyncContext<HttpRequest, HttpResponse>& context) {
        return FailureExecutionResult(1);
      };
  EXPECT_CALL(mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(MockGetAuthToken);
  EXPECT_CALL(mock_auth_token_client_, GetServiceAccountEmail)
      .WillOnce(MockGetServiceAccountEmail);
  AsyncContext<LookupRequest, LookupResponse> lookup_context(
      std::make_shared<LookupRequest>(request_proto),
      [](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_EQ(GetBackendErrorReason(context.status),
                  Error::LOOKUP_SERVICE_SHARD_CLIENT_ERROR);
        EXPECT_EQ(context.response, nullptr);
      },
      logger_);

  lookup_client_->Lookup(lookup_context, kLookupServiceShardAddress);
}

TEST_F(LookupServiceShardClientTest, LookupWithAuthTokenErrorYieldsFailure) {
  LookupRequest request_proto;
  EXPECT_TRUE(JsonStringToMessage(kLookupRequestJson, &request_proto).ok());
  EXPECT_CALL(mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(MockGetAuthTokenFailure);
  AsyncContext<LookupRequest, LookupResponse> lookup_context(
      std::make_shared<LookupRequest>(request_proto),
      [](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_EQ(GetBackendErrorReason(context.status),
                  Error::AUTH_TOKEN_CLIENT_ERROR);
        EXPECT_EQ(context.response, nullptr);
      },
      logger_);

  lookup_client_->Lookup(lookup_context, kLookupServiceShardAddress);
}

TEST_F(LookupServiceShardClientTest, LookupWithEmailErrorYieldsFailure) {
  LookupRequest request_proto;
  EXPECT_TRUE(JsonStringToMessage(kLookupRequestJson, &request_proto).ok());
  EXPECT_CALL(mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(MockGetAuthToken);
  EXPECT_CALL(mock_auth_token_client_, GetServiceAccountEmail)
      .WillOnce(MockGetServiceAccountEmailFailure);
  AsyncContext<LookupRequest, LookupResponse> lookup_context(
      std::make_shared<LookupRequest>(request_proto),
      [](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_EQ(GetBackendErrorReason(context.status),
                  Error::AUTH_TOKEN_CLIENT_ERROR);
        EXPECT_EQ(context.response, nullptr);
      },
      logger_);

  lookup_client_->Lookup(lookup_context, kLookupServiceShardAddress);
}

TEST_F(LookupServiceShardClientTest, LookupIsSuccessfulWithPreconfiguredEmail) {
  LookupServiceShardClient client_with_email(
      &mock_auth_token_client_, &mock_http_client_, kAudience,
      /*disable_service_account_email_retrieval=*/true,
      /*service_account_email=*/kEmail);

  LookupRequest request_proto;
  EXPECT_TRUE(JsonStringToMessage(kLookupRequestJson, &request_proto).ok());
  LookupResponse expected_response;
  EXPECT_TRUE(
      JsonStringToMessage(kLookupResponseJson, &expected_response).ok());
  std::string captured_request_body;
  std::string captured_request_path;
  HttpMethod captured_request_method;
  mock_http_client_.perform_request_mock = std::bind(
      MockPerformRequestToLookupService, &captured_request_body,
      &captured_request_path, &captured_request_method, std::placeholders::_1);
  EXPECT_CALL(mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(MockGetAuthToken);
  // GetServiceAccountEmail should NOT be called.
  EXPECT_CALL(mock_auth_token_client_, GetServiceAccountEmail).Times(0);

  AsyncContext<LookupRequest, LookupResponse> lookup_context(
      std::make_shared<LookupRequest>(request_proto),
      [&expected_response](
          AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_THAT(context.status, IsOk());
        EXPECT_THAT(*context.response, EqualsProto(expected_response));
        EXPECT_EQ(context.response->lookup_results_size(), 1);
      },
      logger_);

  client_with_email.Lookup(lookup_context, kLookupServiceShardAddress);

  EXPECT_EQ(captured_request_method, HttpMethod::POST);
  EXPECT_EQ(captured_request_path, kLookupServiceShardAddress);
  LookupRequest sent_request;
  EXPECT_TRUE(JsonStringToMessage(captured_request_body, &sent_request).ok());
  EXPECT_THAT(sent_request, EqualsProto(request_proto));
}

TEST_F(LookupServiceShardClientTest,
       LookupWithAuthTokenErrorAndPreconfiguredEmailYieldsFailure) {
  LookupServiceShardClient client_with_email(
      &mock_auth_token_client_, &mock_http_client_, kAudience,
      /*disable_service_account_email_retrieval=*/true,
      /*service_account_email=*/kEmail);

  LookupRequest request_proto;
  EXPECT_TRUE(JsonStringToMessage(kLookupRequestJson, &request_proto).ok());
  EXPECT_CALL(mock_auth_token_client_, GetAuthTokenFullFormat)
      .WillOnce(MockGetAuthTokenFailure);
  // GetServiceAccountEmail should NOT be called.
  EXPECT_CALL(mock_auth_token_client_, GetServiceAccountEmail).Times(0);

  AsyncContext<LookupRequest, LookupResponse> lookup_context(
      std::make_shared<LookupRequest>(request_proto),
      [](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_EQ(GetBackendErrorReason(context.status),
                  Error::AUTH_TOKEN_CLIENT_ERROR);
        EXPECT_EQ(context.response, nullptr);
      },
      logger_);

  client_with_email.Lookup(lookup_context, kLookupServiceShardAddress);
}

}  // namespace
}  // namespace google::confidential_match::match_service
