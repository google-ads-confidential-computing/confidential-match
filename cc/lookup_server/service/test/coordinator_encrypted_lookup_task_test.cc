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

#include "cc/lookup_server/service/src/coordinator_encrypted_lookup_task.h"

#include "cc/core/async_executor/mock/mock_async_executor.h"
#include "cc/core/http2_server/mock/mock_http2_server.h"
#include "cc/core/interface/async_context.h"
#include "cc/core/interface/async_executor_interface.h"
#include "cc/core/interface/http_server_interface.h"
#include "cc/core/test/utils/conditional_wait.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/public/core/interface/execution_result.h"
#include "cc/public/core/test/interface/execution_result_matchers.h"
#include "cc/public/cpio/utils/metric_instance/mock/mock_aggregate_metric.h"
#include "googlemock/include/gmock/gmock.h"
#include "googletest/include/gtest/gtest.h"

#include "cc/core/test/src/parse_text_proto.h"
#include "cc/lookup_server/crypto_client/mock/mock_crypto_client.h"
#include "cc/lookup_server/crypto_client/mock/mock_crypto_key.h"
#include "cc/lookup_server/crypto_client/src/error_codes.h"
#include "cc/lookup_server/match_data_storage/mock/mock_match_data_storage.h"
#include "cc/lookup_server/service/src/error_codes.h"
#include "protos/lookup_server/backend/encryption_key_info.pb.h"
#include "protos/lookup_server/backend/match_data_row.pb.h"
#include "protos/shared/api/errors/code.pb.h"
#include "protos/shared/api/errors/error_response.pb.h"

namespace google::confidential_match::lookup_server {
namespace {

using ::google::confidential_match::ParseTextProtoOrDie;
using ::google::confidential_match::lookup_server::proto_api::DataRecord;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::confidential_match::lookup_server::proto_backend::
    EncryptionKeyInfo;
using ::google::confidential_match::lookup_server::proto_backend::MatchDataRow;
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
using ::google::scp::core::test::WaitUntil;
using ::google::scp::cpio::MockAggregateMetric;
using ::std::placeholders::_1;
using ::testing::_;
using ::testing::AtMost;
using ::testing::Eq;
using ::testing::Return;

using SerializableDataRecords = ::google::confidential_match::lookup_server::
    proto_api::LookupRequest::SerializableDataRecords;

class CoordinatorEncryptedLookupTaskTest : public testing::Test {
 protected:
  CoordinatorEncryptedLookupTaskTest()
      : mock_match_data_storage_(std::make_shared<MockMatchDataStorage>()),
        mock_hpke_crypto_client_(std::make_shared<MockCryptoClient>()),
        coordinator_encrypted_lookup_task_(mock_match_data_storage_,
                                           mock_hpke_crypto_client_) {}

  std::shared_ptr<MockMatchDataStorage> mock_match_data_storage_;
  std::shared_ptr<MockCryptoClient> mock_hpke_crypto_client_;
  CoordinatorEncryptedLookupTask coordinator_encrypted_lookup_task_;
};

// Helper to simulate a successful response from a crypto key fetch.
void MockGetCryptoKeyWithResponse(
    std::shared_ptr<CryptoKeyInterface> response,
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface>
        get_key_context) noexcept {
  get_key_context.result = SuccessExecutionResult();
  get_key_context.response = response;
  get_key_context.Finish();
}

TEST_F(CoordinatorEncryptedLookupTaskTest, HandleRequestValidSingleMatch) {
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    encrypted_data_records: "testEncryptedDataRecords"
    key_format: KEY_FORMAT_HASHED_ENCRYPTED
    sharding_scheme { type: "jch" num_shards: 60 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
    encryption_key_info {
      coordinator_key_info {
        key_id: "testKeyId"
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
          kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
        }
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
          kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
        }
      }
    }
    associated_data_keys: "user_id"
  )pb");

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));

  SerializableDataRecords serializable_data_records;
  serializable_data_records.add_data_records()->mutable_lookup_key()->set_key(
      "+16505551234");
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(serializable_data_records.SerializeAsString()));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = std::make_shared<LookupRequest>(request);
  std::atomic<bool> finished = false;
  lookup_context.callback =
      [&finished](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_SUCCESS(context.result);
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
        EXPECT_THAT(*context.response, EqualsProto(expected));
        finished = true;
      };

  coordinator_encrypted_lookup_task_.HandleRequest(lookup_context);
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(CoordinatorEncryptedLookupTaskTest,
       HandleRequestValidOneMatchOneNonMatch) {
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    encrypted_data_records: "testEncryptedDataRecords"
    key_format: KEY_FORMAT_HASHED_ENCRYPTED
    sharding_scheme { type: "jch" num_shards: 60 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
    encryption_key_info {
      coordinator_key_info {
        key_id: "testKeyId"
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
          kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
        }
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
          kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
        }
      }
    }
    associated_data_keys: "user_id"
  )pb");

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551235")))
      .WillOnce(Return(std::vector<MatchDataRow>()));

  SerializableDataRecords serializable_data_records;
  serializable_data_records.add_data_records()->mutable_lookup_key()->set_key(
      "+16505551234");
  serializable_data_records.add_data_records()->mutable_lookup_key()->set_key(
      "+16505551235");
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(serializable_data_records.SerializeAsString()));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = std::make_shared<LookupRequest>(request);
  std::atomic<bool> finished = false;
  lookup_context.callback =
      [&finished](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_SUCCESS(context.result);
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
        EXPECT_THAT(*context.response, EqualsProto(expected));
        finished = true;
      };

  coordinator_encrypted_lookup_task_.HandleRequest(lookup_context);
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(CoordinatorEncryptedLookupTaskTest, HandleRequestValidNoMatch) {
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    encrypted_data_records: "testEncryptedDataRecords"
    key_format: KEY_FORMAT_HASHED_ENCRYPTED
    sharding_scheme { type: "jch" num_shards: 60 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
    encryption_key_info {
      coordinator_key_info {
        key_id: "testKeyId"
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
          kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
        }
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
          kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
        }
      }
    }
    associated_data_keys: "user_id"
  )pb");

  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551235")))
      .WillOnce(Return(std::vector<MatchDataRow>()));

  SerializableDataRecords serializable_data_records;
  serializable_data_records.add_data_records()->mutable_lookup_key()->set_key(
      "+16505551235");
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(serializable_data_records.SerializeAsString()));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = std::make_shared<LookupRequest>(request);
  std::atomic<bool> finished = false;
  lookup_context.callback =
      [&finished](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_SUCCESS(context.result);
        EXPECT_THAT(*context.response, EqualsProto(LookupResponse()));
        finished = true;
      };

  coordinator_encrypted_lookup_task_.HandleRequest(lookup_context);
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(CoordinatorEncryptedLookupTaskTest,
       HandleRequestValidIndividuallyEncryptedSingleMatch) {
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    data_records { lookup_key { key: "testEncryptedLookupKey" } }
    key_format: KEY_FORMAT_HASHED_ENCRYPTED
    sharding_scheme { type: "jch" num_shards: 60 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
    encryption_key_info {
      coordinator_key_info {
        key_id: "testKeyId"
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
          kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
        }
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
          kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
        }
      }
    }
    associated_data_keys: "user_id"
  )pb");

  MatchDataRow match;
  *match.mutable_key() = "+16505551234";
  *match.add_associated_data()->mutable_key() = "type";
  *match.mutable_associated_data(0)->mutable_string_value() = "phone";
  *match.add_associated_data()->mutable_key() = "user_id";
  match.mutable_associated_data(1)->set_int_value(100);
  std::vector<MatchDataRow> matches = {match};
  EXPECT_CALL(*mock_match_data_storage_, Get(Eq("+16505551234")))
      .WillOnce(Return(matches));
  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt).WillOnce(Return("+16505551234"));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = std::make_shared<LookupRequest>(request);
  std::atomic<bool> finished = false;
  lookup_context.callback =
      [&finished](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_SUCCESS(context.result);
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
        EXPECT_THAT(*context.response, EqualsProto(expected));
        finished = true;
      };

  coordinator_encrypted_lookup_task_.HandleRequest(lookup_context);
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(CoordinatorEncryptedLookupTaskTest,
       HandleRequestWithDecryptErrorReturnsError) {
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    encrypted_data_records: "invalidEncryptedDataRecords"
    key_format: KEY_FORMAT_HASHED_ENCRYPTED
    sharding_scheme { type: "jch" num_shards: 60 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
    encryption_key_info {
      coordinator_key_info {
        key_id: "testKeyId"
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
          kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
        }
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
          kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
        }
      }
    }
    associated_data_keys: "user_id"
  )pb");

  auto mock_crypto_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_crypto_key, Decrypt)
      .WillOnce(Return(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::move(mock_crypto_key), _1));

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = std::make_shared<LookupRequest>(request);
  std::atomic<bool> finished = false;
  lookup_context.callback =
      [&finished](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_THAT(
            context.result,
            ResultIs(FailureExecutionResult(CRYPTO_CLIENT_DECRYPT_ERROR)));
        finished = true;
      };

  coordinator_encrypted_lookup_task_.HandleRequest(lookup_context);
  WaitUntil([&]() { return finished.load(); });
}

TEST_F(CoordinatorEncryptedLookupTaskTest,
       HandleRequestWithInvalidBase64ReturnsError) {
  LookupRequest request = ParseTextProtoOrDie(R"pb(
    encrypted_data_records: "Invalid base-64!"
    key_format: KEY_FORMAT_HASHED_ENCRYPTED
    sharding_scheme { type: "jch" num_shards: 60 }
    hash_info { hash_type: HASH_TYPE_SHA_256 }
    encryption_key_info {
      coordinator_key_info {
        key_id: "testKeyId"
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-1.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator1.iam.gserviceaccount.com"
          kms_wip_provider: "projects/1/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-1.a.run.app"
        }
        coordinator_info {
          key_service_endpoint: "https://test-privatekeyservice-2.google.com/v1alpha"
          kms_identity: "test-verified-user@coordinator2.iam.gserviceaccount.com"
          kms_wip_provider: "projects/2/locations/global/workloadIdentityPools/wip/providers/provider"
          key_service_audience_url: "https://test-key-service-audience-url-2.a.run.app"
        }
      }
    }
    associated_data_keys: "user_id"
  )pb");

  EXPECT_CALL(*mock_hpke_crypto_client_, GetCryptoKey)
      .WillOnce(std::bind(MockGetCryptoKeyWithResponse,
                          std::make_shared<MockCryptoKey>(), _1));

  AsyncContext<LookupRequest, LookupResponse> lookup_context;
  lookup_context.request = std::make_shared<LookupRequest>(request);
  std::atomic<bool> finished = false;
  lookup_context.callback =
      [&finished](AsyncContext<LookupRequest, LookupResponse>& context) {
        EXPECT_THAT(
            context.result,
            ResultIs(FailureExecutionResult(LOOKUP_SERVICE_INVALID_REQUEST)));
        finished = true;
      };

  coordinator_encrypted_lookup_task_.HandleRequest(lookup_context);
  WaitUntil([&]() { return finished.load(); });
}

}  // namespace
}  // namespace google::confidential_match::lookup_server
