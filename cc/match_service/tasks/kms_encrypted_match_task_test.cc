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

#include "cc/match_service/tasks/kms_encrypted_match_task.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/numbers.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "cc/core/async/async_context.h"
#include "cc/core/hash/mock_hasher.h"
#include "cc/core/logger/logger_interface.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "cc/match_service/crypto_client/crypto_key_interface.h"
#include "cc/match_service/crypto_client/mock_crypto_client.h"
#include "cc/match_service/crypto_client/mock_crypto_key.h"
#include "cc/match_service/error/error.h"
#include "cc/match_service/lookup_service_client/mock_lookup_service_client.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/match_service.pb.h"
#include "protos/match_service/backend/private_key_endpoints.pb.h"
#include "public/cpio/mock/metric_client/mock_metric_client.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::EncryptionKeyInfo;
using ::google::confidential_match::MockHasher;
using ::google::confidential_match::match_service::MockLookupServiceClient;
using ::google::confidential_match::match_service::backend::Error;
using ::google::confidential_match::match_service::backend::
    LookupServiceRequest;
using ::google::confidential_match::match_service::backend::
    LookupServiceResponse;
using ::google::confidential_match::match_service::backend::MatchRequest;
using ::google::confidential_match::match_service::backend::MatchResponse;
using ::google::confidential_match::match_service::backend::PrivateKeyEndpoints;
using ::google::protobuf::TextFormat;
using ::google::scp::core::test::EqualsProto;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::UnorderedPointwise;

class KmsEncryptedMatchTaskTest : public ::testing::Test {
 protected:
  KmsEncryptedMatchTaskTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_lookup_service_client_(
            std::make_unique<MockLookupServiceClient>()),
        mock_aead_crypto_client_(std::make_unique<MockCryptoClient>()),
        mock_hybrid_crypto_client_(std::make_unique<MockCryptoClient>()),
        mock_hasher_(std::make_unique<MockHasher>()),
        kms_encrypted_match_task_(
            mock_lookup_service_client_.get(), mock_aead_crypto_client_.get(),
            mock_hasher_.get(), mock_hybrid_crypto_client_.get(),
            PrivateKeyEndpoints()) {}

  std::shared_ptr<LoggerInterface> logger_;
  std::unique_ptr<MockLookupServiceClient> mock_lookup_service_client_;
  std::unique_ptr<MockCryptoClient> mock_aead_crypto_client_,
      mock_hybrid_crypto_client_;
  std::unique_ptr<MockHasher> mock_hasher_;
  KmsEncryptedMatchTask kms_encrypted_match_task_;
};

// Helper to mock the CryptoClient returning a specific mock key.
void MockGetCryptoKeySuccess(
    std::shared_ptr<MockCryptoKey> mock_key,
    AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context) noexcept {
  context.response = mock_key;
  context.Finish();
}

// Helper to mock a successful lookup call.
void MockLookupSuccess(const LookupServiceResponse& response,
                       AsyncContext<LookupServiceRequest,
                                    LookupServiceResponse>& context) noexcept {
  context.response = std::make_shared<LookupServiceResponse>(response);
  context.status = absl::OkStatus();
  context.Finish();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchSingleMatchedRecordSuccess) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));
  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2VtYWls"
            decrypted_key: "decrypted_email"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));
  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" } }
        }
      )pb",
      &mock_lookup_resp));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        EXPECT_EQ(ctx.response->matched_data_records(0)
                      .matched_keys(0)
                      .field()
                      .matched_field_info()
                      .field_value(),
                  "decrypted_email");
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchSingleMatchedRecordHexSuccess) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_HEX
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "656e637279707465645f656d61696c"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));
  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2VtYWls"
            decrypted_key: "decrypted_email"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));
  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" } }
        }
      )pb",
      &mock_lookup_resp));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        EXPECT_EQ(ctx.response->matched_data_records(0)
                      .matched_keys(0)
                      .field()
                      .matched_field_info()
                      .field_value(),
                  "decrypted_email");
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest,
       MatchSingleMatchedRecordInvalidEncodingReturnsError) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_HEX
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_fail_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_fail_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key.field().error_reason(),
                  backend::ERROR_REASON_DECODING_ERROR);
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest,
       MatchSingleMatchedRecordAssociatedDataSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
        associated_data_types: ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER
      )pb",
      context.request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            associated_data {
              key: "encrypted_gaia_id"
              bytes_value: "encrypted_bytes"
            }

          }
        }
      )pb",
      &mock_lookup_response));
  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2VtYWls"
            decrypted_key: "decrypted_email"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        associated_data_keys: "encrypted_gaia_id"
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_request));
    MockLookupSuccess(mock_lookup_response, ctx);
  });
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "decrypted_email"
              }
              matched_associated_data_indices: 0
            }
          }
        }
        matched_associated_data {
          first_party_identifier { id: "ZW5jcnlwdGVkX2J5dGVz" }
        }
      )pb",
      &expected_match_response));
  absl::Notification finished;
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
    finished.Notify();
  };

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchMultipleRecordsGroupedByEncryptionKey) {
  auto request = std::make_shared<MatchRequest>();
  // request contains 3 records, 2 from one KEK and 1 using another
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "key_A_dek"
              kek_kms_resource_id: "key_A_kek"
              gcp_wrapped_key_info { wip_provider: "key_A_wip" }
            }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzBfa2V5MA=="  # enc_rec0_key0
            }
          }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "key_A_dek"
              kek_kms_resource_id: "key_A_kek"
              gcp_wrapped_key_info { wip_provider: "key_A_wip" }
            }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzFfa2V5MA=="  # enc_rec1_key0
            }
          }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "key_B_dek"
              kek_kms_resource_id: "key_B_kek"
              gcp_wrapped_key_info { wip_provider: "key_B_wip" }
            }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzJfa2V5MA=="  # enc_rec2_key0
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key_a = std::make_shared<MockCryptoKey>();
  auto mock_key_b = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .Times(2)
      .WillRepeatedly(Invoke([&](auto ctx) {
        if (ctx.request->wrapped_key_info().encrypted_dek() == "key_A_dek") {
          MockGetCryptoKeySuccess(mock_key_a, ctx);
        } else {
          MockGetCryptoKeySuccess(mock_key_b, ctx);
        }
      }));
  EXPECT_CALL(*mock_key_a, Decrypt("enc_rec0_key0"))
      .WillOnce(Return("dec_rec0"));
  EXPECT_CALL(*mock_key_a, Decrypt("enc_rec1_key0"))
      .WillOnce(Return("dec_rec1"));
  EXPECT_CALL(*mock_key_b, Decrypt("enc_rec2_key0"))
      .WillOnce(Return("dec_rec2"));
  LookupServiceResponse resp_group_a;  // Records 0 and 1
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" } }
        }
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzFfa2V5MA==" }
            metadata { key: "d" int_value: 1 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzFfa2V5MA==" } }
        }
      )pb",
      &resp_group_a));
  LookupServiceResponse resp_group_b;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzJfa2V5MA==" }
            metadata { key: "d" int_value: 2 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzJfa2V5MA==" } }
        }
      )pb",
      &resp_group_b));
  // Lookup Service: Expect 2 batched calls
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .Times(2)
      .WillRepeatedly(Invoke([&](auto& ctx) {
        // Distinguish based on the encryption key in the LookupRequest
        if (ctx.request->encryption_key().wrapped_key().encrypted_dek() ==
            "key_A_dek") {
          // Verify Group A has 2 records
          EXPECT_EQ(ctx.request->data_records_size(), 2);
          MockLookupSuccess(resp_group_a, ctx);
        } else if (ctx.request->encryption_key()
                       .wrapped_key()
                       .encrypted_dek() == "key_B_dek") {
          // Verify Group B has 1 record
          EXPECT_EQ(ctx.request->data_records_size(), 1);
          MockLookupSuccess(resp_group_b, ctx);
        } else {
          FAIL() << "Should be one of the two";
        }
      }));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        ASSERT_EQ(ctx.response->matched_data_records_size(), 3);
        // Verify Record 0 (Group A)
        EXPECT_EQ(ctx.response->matched_data_records(0)
                      .matched_keys(0)
                      .field()
                      .matched_field_info()
                      .field_value(),
                  "dec_rec0");
        // Verify Record 1 (Group A)
        EXPECT_EQ(ctx.response->matched_data_records(1)
                      .matched_keys(0)
                      .field()
                      .matched_field_info()
                      .field_value(),
                  "dec_rec1");
        // Verify Record 2 (Group B)
        EXPECT_EQ(ctx.response->matched_data_records(2)
                      .matched_keys(0)
                      .field()
                      .matched_field_info()
                      .field_value(),
                  "dec_rec2");
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchCompositeAddressSuccess) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values {
                type: FIELD_TYPE_FIRST_NAME
                value: "ZW5jX2ZpcnN0"  # "enc_first"
              }
              values {
                type: FIELD_TYPE_LAST_NAME
                value: "ZW5jX2xhc3Q="  # "enc_last"
              }
              values {
                type: FIELD_TYPE_COUNTRY_CODE
                value: "US"  # Plaintext
              }
              values {
                type: FIELD_TYPE_ZIP_CODE
                value: "90210"  # Plaintext
              }
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  // Expect decryption of First and Last Name.
  EXPECT_CALL(*mock_key, Decrypt("enc_first")).WillOnce(Return("Sm9obg=="));
  EXPECT_CALL(*mock_key, Decrypt("enc_last")).WillOnce(Return("RG9l"));
  // Hashed concatenated Address.
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("Sm9obg==RG9lus90210"))
      .WillOnce(Return("hashed_address"));
  EXPECT_CALL(*mock_key, Encrypt("hashed_address"))
      .WillOnce(Return("encrypted_hashed_address"));
  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz"
            decrypted_key: "hashed_address"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));
  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
          }
        }
      )pb",
      &mock_lookup_resp));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_FIRST_NAME
                field_value: "Sm9obg=="
              }
              matched_field_info {
                field_type: FIELD_TYPE_LAST_NAME
                field_value: "RG9l"
              }
              matched_field_info {
                field_type: FIELD_TYPE_COUNTRY_CODE
                field_value: "US"
              }
              matched_field_info {
                field_type: FIELD_TYPE_ZIP_CODE
                field_value: "90210"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest,
       MatchCompositeAddressPartialEncryptionFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "ZW5jX2ZpcnN0XzA=" }
              values { type: FIELD_TYPE_LAST_NAME value: "ZW5jX2xhc3RfMA==" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "90210" }
            }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "ZW5jX2ZpcnN0XzE=" }
              values { type: FIELD_TYPE_LAST_NAME value: "ZW5jX2xhc3RfMQ==" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "90211" }
            }
          }
        }
      )pb",
      request.get()));

  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));

  EXPECT_CALL(*mock_key, Decrypt("enc_first_0"))
      .WillOnce(Return("dec_first_0"));
  EXPECT_CALL(*mock_key, Decrypt("enc_last_0")).WillOnce(Return("dec_last_0"));
  EXPECT_CALL(*mock_key, Decrypt("enc_first_1"))
      .WillOnce(Return("dec_first_1"));
  EXPECT_CALL(*mock_key, Decrypt("enc_last_1")).WillOnce(Return("dec_last_1"));

  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("dec_first_0dec_last_0us90210"))
      .WillOnce(Return("hashed_address_0"));
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("dec_first_1dec_last_1us90211"))
      .WillOnce(Return("hashed_address_1"));

  EXPECT_CALL(*mock_key, Encrypt("hashed_address_0"))
      .WillOnce(Return(Status(Error::ENCRYPTION_ERROR, "Encryption failed")));
  EXPECT_CALL(*mock_key, Encrypt("hashed_address_1"))
      .WillOnce(Return("enc_hash_1"));

  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jX2hhc2hfMQ=="
            decrypted_key: "hashed_address_1"
          }
          metadata { key: "d" int_value: 1 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));

  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX2hhc2hfMQ==" }
            metadata { key: "d" int_value: 1 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX2hhc2hfMQ==" } }
        }
      )pb",
      &mock_lookup_resp));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_ENCRYPTION_ERROR
              matched_field_info { field_type: FIELD_TYPE_FIRST_NAME }
              matched_field_info { field_type: FIELD_TYPE_LAST_NAME }
              matched_field_info { field_type: FIELD_TYPE_COUNTRY_CODE }
              matched_field_info { field_type: FIELD_TYPE_ZIP_CODE }
            }
          }
        }
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_FIRST_NAME
                field_value: "dec_first_1"
              }
              matched_field_info {
                field_type: FIELD_TYPE_LAST_NAME
                field_value: "dec_last_1"
              }
              matched_field_info {
                field_type: FIELD_TYPE_COUNTRY_CODE
                field_value: "US"
              }
              matched_field_info {
                field_type: FIELD_TYPE_ZIP_CODE
                field_value: "90211"
              }
            }
          }
        }
      )pb",
      &expected_match_response));

  bool done = false;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
        done = true;
      },
      logger_);

  kms_encrypted_match_task_.Match(context);

  EXPECT_TRUE(done);
}

TEST_F(KmsEncryptedMatchTaskTest, MatchCompositeAddressHexSuccess) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_HEX
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values {
                type: FIELD_TYPE_FIRST_NAME
                value: "456e637279707465644669727374"  # "EncryptedFirst"
              }
              values {
                type: FIELD_TYPE_LAST_NAME
                value: "456e637279707465644c617374"  # "EncryptedLast"
              }
              values {
                type: FIELD_TYPE_COUNTRY_CODE
                value: "US"  # Plaintext
              }
              values {
                type: FIELD_TYPE_ZIP_CODE
                value: "90210"  # Plaintext
              }
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  // Expect decryption of First and Last Name.
  EXPECT_CALL(*mock_key, Decrypt("EncryptedFirst"))
      .WillOnce(Return("Sm9obg=="));
  EXPECT_CALL(*mock_key, Decrypt("EncryptedLast")).WillOnce(Return("RG9l"));
  // Hashed concatenated Address.
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("Sm9obg==RG9lus90210"))
      .WillOnce(Return("hashed_address"));
  EXPECT_CALL(*mock_key, Encrypt("hashed_address"))
      .WillOnce(Return("encrypted_hashed_address"));
  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz"
            decrypted_key: "hashed_address"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));
  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
          }
        }
      )pb",
      &mock_lookup_resp));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_FIRST_NAME
                field_value: "Sm9obg=="
              }
              matched_field_info {
                field_type: FIELD_TYPE_LAST_NAME
                field_value: "RG9l"
              }
              matched_field_info {
                field_type: FIELD_TYPE_COUNTRY_CODE
                field_value: "US"
              }
              matched_field_info {
                field_type: FIELD_TYPE_ZIP_CODE
                field_value: "90210"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithCryptoKeyRetrievalFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "fail_dek"
            kek_kms_resource_id: "fail_kek"
            gcp_wrapped_key_info { wip_provider: "fail_wip" }
          }
        }
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
      )pb",
      request.get()));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce([](auto ctx) {
        ctx.status = Status(Error::WRAPPED_KEY_FETCHING_ERROR,
                            "Received error from KMS client when trying to "
                            "decrypt the wrapped DEK.");
        ctx.Finish();
      });
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_fail_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_fail_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key.field().error_reason(),
                  backend::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR);
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithCryptoKeyRetrievalPartialFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "fail_dek"
              kek_kms_resource_id: "fail_kek"
              gcp_wrapped_key_info { wip_provider: "fail_wip" }
            }
          }
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "test_dek"
              kek_kms_resource_id: "test_kek_id"
              gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
            }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzBfa2V5MA=="  # enc_rec0_key0
            }
          }
        }
      )pb",
      request.get()));

  auto mock_valid_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .Times(2)
      .WillRepeatedly(Invoke([&](auto ctx) {
        if (ctx.request->wrapped_key_info().encrypted_dek() == "fail_dek") {
          ctx.status = Status(Error::DEK_DECRYPTION_ERROR,
                              "Failed to decrypt the wrapped DEK.");
          ctx.Finish();
        } else {
          MockGetCryptoKeySuccess(mock_valid_key, ctx);
        }
      }));
  EXPECT_CALL(*mock_valid_key, Decrypt("enc_rec0_key0"))
      .WillOnce(Return("dec_rec0"));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" }
            metadata { key: "d" int_value: 1 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" } }
        }
      )pb",
      &mock_lookup_response));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(Invoke([&](auto& ctx) {
        EXPECT_EQ(ctx.request->data_records_size(), 1);
        MockLookupSuccess(mock_lookup_response, ctx);
      }));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_fail_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_fail_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key.field().error_reason(),
                  backend::ERROR_REASON_DEK_DECRYPTION_ERROR);
        const auto& matched_pass_key =
            ctx.response->matched_data_records(1).matched_keys(0);
        EXPECT_EQ(matched_pass_key.field().status(),
                  backend::STATUS_SUCCESS_MATCHED);
        EXPECT_EQ(matched_pass_key.field().matched_field_info().field_value(),
                  "dec_rec0");
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchInputDecodingPartialFailureFieldLevel) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "Pl8-"  # Invalid Base64 for MATCH_KEY_ENCODING_BASE64
            }
          }
        }
      )pb",
      request.get()));

  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  // Decryption should only be called for the valid one
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));

  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2VtYWls"
            decrypted_key: "decrypted_email"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));

  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" } }
        }
      )pb",
      &mock_lookup_resp));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  bool done = false;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        // Verify Record 0 is valid
        const auto& valid_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(valid_key.field().matched_field_info().field_value(),
                  "decrypted_email");
        EXPECT_NE(valid_key.field().status(), backend::STATUS_FAILED);

        // Verify Record 1 is failed with decoding error
        const auto& failed_key =
            ctx.response->matched_data_records(1).matched_keys(0);
        EXPECT_EQ(failed_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(failed_key.field().error_reason(),
                  backend::ERROR_REASON_DECODING_ERROR);
        done = true;
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  EXPECT_TRUE(done);
}

TEST_F(KmsEncryptedMatchTaskTest,
       MatchInputDecodingCompositePartialFailureFieldLevel) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values {
                type: FIELD_TYPE_FIRST_NAME
                value: "Pl8-"  # Invalid Base64
              }
              values {
                type: FIELD_TYPE_LAST_NAME
                value: "ZW5jcnlwdGVkX2VtYWls"  # Valid Base64
              }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "90210" }
            }
          }
        }
      )pb",
      request.get()));

  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));

  LookupServiceRequest expected_lookup_req;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          lookup_key {
            key: "ZW5jcnlwdGVkX2VtYWls"
            decrypted_key: "decrypted_email"
          }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
      )pb",
      &expected_lookup_req));

  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" } }
        }
      )pb",
      &mock_lookup_resp));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    EXPECT_THAT(*ctx.request, EqualsProto(expected_lookup_req));
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  bool done = false;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        // Verify Record 0 is valid
        const auto& valid_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(valid_key.field().matched_field_info().field_value(),
                  "decrypted_email");
        EXPECT_NE(valid_key.field().status(), backend::STATUS_FAILED);

        // Verify Record 1 (Composite) is failed with decoding error
        const auto& failed_key =
            ctx.response->matched_data_records(1).matched_keys(0);
        EXPECT_EQ(failed_key.composite_field().status(),
                  backend::STATUS_FAILED);
        EXPECT_EQ(failed_key.composite_field().error_reason(),
                  backend::ERROR_REASON_DECODING_ERROR);
        done = true;
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  EXPECT_TRUE(done);
}

TEST_F(KmsEncryptedMatchTaskTest,
       MatchWithCryptoKeyRetrievalNewErrorsPartialFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "fail_dek_permission"
              kek_kms_resource_id: "fail_kek"
              gcp_wrapped_key_info { wip_provider: "fail_wip" }
            }
          }
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "fail_dek_quota"
              kek_kms_resource_id: "fail_kek"
              gcp_wrapped_key_info { wip_provider: "fail_wip" }
            }
          }
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "fail_dek_wrapped"
              kek_kms_resource_id: "fail_kek"
              gcp_wrapped_key_info { wip_provider: "fail_wip" }
            }
          }
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "test_dek"
              kek_kms_resource_id: "test_kek_id"
              gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
            }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzBfa2V5MA=="  # enc_rec0_key0
            }
          }
        }
      )pb",
      request.get()));

  auto mock_valid_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .Times(4)
      .WillRepeatedly(Invoke([&](auto ctx) {
        if (ctx.request->wrapped_key_info().encrypted_dek() ==
            "fail_dek_permission") {
          ctx.status = Status(Error::CUSTOMER_KEY_PERMISSION_DENIED,
                              "Permission Denied");
          ctx.Finish();
        } else if (ctx.request->wrapped_key_info().encrypted_dek() ==
                   "fail_dek_quota") {
          ctx.status = Status(Error::CUSTOMER_QUOTA_EXCEEDED, "Quota Exceeded");
          ctx.Finish();
        } else if (ctx.request->wrapped_key_info().encrypted_dek() ==
                   "fail_dek_wrapped") {
          ctx.status =
              Status(Error::WRAPPED_KEY_FETCHING_ERROR, "Fetching Error");
          ctx.Finish();
        } else {
          MockGetCryptoKeySuccess(mock_valid_key, ctx);
        }
      }));

  EXPECT_CALL(*mock_valid_key, Decrypt("enc_rec0_key0"))
      .WillOnce(Return("dec_rec0"));

  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" }
            metadata { key: "d" int_value: 3 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" } }
        }
      )pb",
      &mock_lookup_response));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(Invoke([&](auto& ctx) {
        EXPECT_EQ(ctx.request->data_records_size(), 1);
        MockLookupSuccess(mock_lookup_response, ctx);
      }));

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());

        // Record 0: Permission Denied
        const auto& matched_fail_key_0 =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_fail_key_0.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key_0.field().error_reason(),
                  backend::ERROR_REASON_CUSTOMER_KEY_PERMISSION_DENIED);

        // Record 1: Quota Exceeded
        const auto& matched_fail_key_1 =
            ctx.response->matched_data_records(1).matched_keys(0);
        EXPECT_EQ(matched_fail_key_1.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key_1.field().error_reason(),
                  backend::ERROR_REASON_CUSTOMER_QUOTA_EXCEEDED);

        // Record 2: Wrapped Key Fetching Error
        const auto& matched_fail_key_2 =
            ctx.response->matched_data_records(2).matched_keys(0);
        EXPECT_EQ(matched_fail_key_2.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key_2.field().error_reason(),
                  backend::ERROR_REASON_WRAPPED_KEY_FETCHING_ERROR);

        // Record 3: Success
        const auto& matched_pass_key =
            ctx.response->matched_data_records(3).matched_keys(0);
        EXPECT_EQ(matched_pass_key.field().status(),
                  backend::STATUS_SUCCESS_MATCHED);
        EXPECT_EQ(matched_pass_key.field().matched_field_info().field_value(),
                  "dec_rec0");
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithDecryptionFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "fail_dek"
            kek_kms_resource_id: "fail_kek"
            gcp_wrapped_key_info { wip_provider: "fail_wip" }
          }
        }
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt(_))
      .WillOnce(Return(Status(Error::DECRYPTION_ERROR,
                              "Tink AEAD failed to decrypt ciphertext.")));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_fail_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_fail_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key.field().error_reason(),
                  backend::ERROR_REASON_DECRYPTION_ERROR);
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithDecryptionPartialFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "fail_dek"
            kek_kms_resource_id: "fail_kek"
            gcp_wrapped_key_info { wip_provider: "fail_wip" }
          }
        }
        data_records {
          encryption_key {
            wrapped_key {
              key_type: KEY_TYPE_XCHACHA20_POLY1305
              encrypted_dek: "test_dek"
              kek_kms_resource_id: "test_kek_id"
              gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
            }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzBfa2V5MA=="  # enc_rec0_key0
            }
          }
        }
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "ZW5j" } }
        }
      )pb",
      request.get()));
  auto mock_valid_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_valid_key, Decrypt("enc_rec0_key0"))
      .WillOnce(Return("dec_rec0"));
  auto mock_fail_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_fail_key, Decrypt(_))
      .WillOnce(Return(Status(Error::DECRYPTION_ERROR,
                              "Tink AEAD failed to decrypt ciphertext.")));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .Times(2)
      .WillRepeatedly(Invoke([&](auto ctx) {
        if (ctx.request->wrapped_key_info().encrypted_dek() == "fail_dek") {
          MockGetCryptoKeySuccess(mock_fail_key, ctx);
        } else {
          MockGetCryptoKeySuccess(mock_valid_key, ctx);
        }
      }));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" } }
        }
      )pb",
      &mock_lookup_response));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(Invoke([&](auto& ctx) {
        EXPECT_EQ(ctx.request->data_records_size(), 1);
        MockLookupSuccess(mock_lookup_response, ctx);
      }));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_pass_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_pass_key.field().status(),
                  backend::STATUS_SUCCESS_MATCHED);
        EXPECT_EQ(matched_pass_key.field().matched_field_info().field_value(),
                  "dec_rec0");
        const auto& matched_fail_key =
            ctx.response->matched_data_records(1).matched_keys(0);
        EXPECT_EQ(matched_fail_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key.field().error_reason(),
                  backend::ERROR_REASON_DECRYPTION_ERROR);
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithDecryptionPartialCompositeFailure) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzBfa2V5MA=="  # enc_rec0_key0
            }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values {
                type: FIELD_TYPE_FIRST_NAME
                value: "ZW5jX2ZpcnN0"  # "enc_first"
              }
              values {
                type: FIELD_TYPE_LAST_NAME
                value: "ZW5jX2xhc3Q="  # "enc_last"
              }
              values {
                type: FIELD_TYPE_COUNTRY_CODE
                value: "US"  # Plaintext
              }
              values {
                type: FIELD_TYPE_ZIP_CODE
                value: "90210"  # Plaintext
              }
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_key, Decrypt("enc_rec0_key0")).WillOnce(Return("dec_rec0"));
  EXPECT_CALL(*mock_key, Decrypt("enc_first")).WillOnce(Return("Sm9obg=="));
  EXPECT_CALL(*mock_key, Decrypt("enc_last"))
      .WillOnce(Return(Status(Error::DECRYPTION_ERROR,
                              "Tink AEAD failed to decrypt ciphertext.")));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" } }
        }
      )pb",
      &mock_lookup_response));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(Invoke([&](auto& ctx) {
        EXPECT_EQ(ctx.request->data_records_size(), 1);
        MockLookupSuccess(mock_lookup_response, ctx);
      }));
  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_pass_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_pass_key.field().status(),
                  backend::STATUS_SUCCESS_MATCHED);
        EXPECT_EQ(matched_pass_key.field().matched_field_info().field_value(),
                  "dec_rec0");
        const auto& matched_fail_key =
            ctx.response->matched_data_records(1).matched_keys(0);
        EXPECT_EQ(matched_fail_key.composite_field().status(),
                  backend::STATUS_FAILED);
        EXPECT_EQ(matched_fail_key.composite_field().error_reason(),
                  backend::ERROR_REASON_DECRYPTION_ERROR);
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchCoordinatorKeyUsesHybridClient) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key { coordinator_key { key_id: "test_key_id" } }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));

  auto mock_key = std::make_shared<MockCryptoKey>();
  // Should call mock_hybrid_crypto_client, NOT mock_aead_crypto_client_
  EXPECT_CALL(*mock_hybrid_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync).Times(0);

  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));

  LookupServiceResponse mock_lookup_resp;
  auto* lookup_result = mock_lookup_resp.add_lookup_results();
  lookup_result->set_status(backend::LookupResult::STATUS_SUCCESS);
  auto* client_record = lookup_result->mutable_client_data_record();
  auto* meta_d = client_record->add_metadata();
  meta_d->set_key("d");
  meta_d->set_int_value(0);
  auto* meta_m = client_record->add_metadata();
  meta_m->set_key("m");
  meta_m->set_int_value(0);

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchCoordinatorKeyPopulatesCoordinatorInfo) {
  PrivateKeyEndpoints endpoints;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        endpoints {
          endpoint: "endpoint_1"
          account_identity: "identity_1"
          gcp_wip_provider: "wip_1"
          gcp_cloud_function_url: "url_1"
        }
        endpoints {
          endpoint: "endpoint_2"
          account_identity: "identity_2"
          gcp_wip_provider: "wip_2"
          gcp_cloud_function_url: "url_2"
        }
      )pb",
      &endpoints));

  KmsEncryptedMatchTask task(mock_lookup_service_client_.get(),
                             mock_aead_crypto_client_.get(), mock_hasher_.get(),
                             mock_hybrid_crypto_client_.get(), endpoints);

  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key { coordinator_key { key_id: "test_key_id" } }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));

  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_hybrid_crypto_client_, GetCryptoKeyAsync)
      .WillOnce([&](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> ctx) {
        EXPECT_EQ(ctx.request->coordinator_key_info().key_id(), "test_key_id");
        MockGetCryptoKeySuccess(mock_key, ctx);
      });

  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    // Verify LookupRequest has coordinator info too
    EXPECT_EQ(ctx.request->encryption_key().coordinator_key().key_id(),
              "test_key_id");
    ASSERT_EQ(
        ctx.request->encryption_key().coordinator_key().coordinator_info_size(),
        2);
    EXPECT_EQ(ctx.request->encryption_key()
                  .coordinator_key()
                  .coordinator_info(0)
                  .key_service_endpoint(),
              "endpoint_1");
    EXPECT_EQ(ctx.request->encryption_key()
                  .coordinator_key()
                  .coordinator_info(0)
                  .kms_wip_provider(),
              "wip_1");
    EXPECT_EQ(ctx.request->encryption_key()
                  .coordinator_key()
                  .coordinator_info(0)
                  .key_service_audience_url(),
              "url_1");

    LookupServiceResponse mock_lookup_resp;
    auto* lookup_result = mock_lookup_resp.add_lookup_results();
    lookup_result->set_status(backend::LookupResult::STATUS_SUCCESS);
    auto* client_record = lookup_result->mutable_client_data_record();
    auto* meta_d = client_record->add_metadata();
    meta_d->set_key("d");
    meta_d->set_int_value(0);
    auto* meta_m = client_record->add_metadata();
    meta_m->set_key("m");
    meta_m->set_int_value(0);
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        finished.Notify();
      },
      logger_);

  task.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchCompositeAddressPreservesCasingSuccess) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "ZW5jX2ZpcnN0" }
              values { type: FIELD_TYPE_LAST_NAME value: "ZW5jX2xhc3Q=" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "uS" }
              values { type: FIELD_TYPE_ZIP_CODE value: "9021" }
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("enc_first")).WillOnce(Return("Sm9obg=="));
  EXPECT_CALL(*mock_key, Decrypt("enc_last")).WillOnce(Return("RG9l"));

  // Both country code and zip code should be lowercased in the hash input,
  // and US zip code should be zero-padded to 5 digits (09021).
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("Sm9obg==RG9lus09021"))
      .WillOnce(Return("hashed_address"));
  EXPECT_CALL(*mock_key, Encrypt("hashed_address"))
      .WillOnce(Return("encrypted_hashed_address"));

  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
          }
        }
      )pb",
      &mock_lookup_response));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    MockLookupSuccess(mock_lookup_response, ctx);
  });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& info = ctx.response->matched_data_records(0)
                               .matched_keys(0)
                               .composite_field()
                               .matched_field_info();
        // Values in the response should match the original request casing.
        EXPECT_EQ(info[2].field_value(), "uS");
        EXPECT_EQ(info[3].field_value(), "9021");
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, RecordsKeyFetchingMetrics) {
  google::scp::cpio::MockMetricClient mock_metric_client;
  KmsEncryptedMatchTask task_with_metrics(
      mock_lookup_service_client_.get(), mock_aead_crypto_client_.get(),
      mock_hasher_.get(), mock_hybrid_crypto_client_.get(),
      PrivateKeyEndpoints(), &mock_metric_client, "test_namespace");

  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          encryption_key {
            coordinator_key { key_id: "key_A_id" }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzBfa2V5MA=="  # enc_rec0_key0
            }
          }
        }
        data_records {
          encryption_key {
            coordinator_key { key_id: "key_B_id" }
          }
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jX3JlYzFfa2V5MA=="  # enc_rec1_key0
            }
          }
        }
      )pb",
      request.get()));

  auto mock_key_a = std::make_shared<MockCryptoKey>();
  auto mock_key_b = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_hybrid_crypto_client_, GetCryptoKeyAsync)
      .Times(2)
      .WillRepeatedly(Invoke([&](auto ctx) {
        if (ctx.request->coordinator_key_info().key_id() == "key_A_id") {
          absl::SleepFor(absl::Milliseconds(10));  // Simulate latency
          MockGetCryptoKeySuccess(mock_key_a, ctx);
        } else {
          absl::SleepFor(absl::Milliseconds(50));  // Simulate latency
          MockGetCryptoKeySuccess(mock_key_b, ctx);
        }
      }));
  EXPECT_CALL(*mock_key_a, Decrypt("enc_rec0_key0"))
      .WillOnce(Return("dec_rec0"));
  EXPECT_CALL(*mock_key_b, Decrypt("enc_rec1_key0"))
      .WillOnce(Return("dec_rec1"));

  LookupServiceResponse resp_group_a_and_b;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzBfa2V5MA==" } }
        }
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "ZW5jX3JlYzFfa2V5MA==" }
            metadata { key: "d" int_value: 1 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "ZW5jX3JlYzFfa2V5MA==" } }
        }
      )pb",
      &resp_group_a_and_b));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .Times(2)
      .WillRepeatedly(Invoke([&](auto& ctx) {
        LookupServiceResponse response;
        if (ctx.request->encryption_key().coordinator_key().key_id() ==
            "key_A_id") {
          *response.add_lookup_results() = resp_group_a_and_b.lookup_results(0);
        } else {
          *response.add_lookup_results() = resp_group_a_and_b.lookup_results(1);
        }
        MockLookupSuccess(response, ctx);
      }));

  int count_metrics_received = 0;
  std::vector<int64_t> latencies_recorded;
  EXPECT_CALL(mock_metric_client, PutMetrics)
      .Times(4)
      .WillRepeatedly([&](auto& ctx) {
        ctx.result = scp::core::SuccessExecutionResult();
        const auto& metrics = ctx.request->metrics();
        ASSERT_EQ(metrics.size(), 1);
        const auto& metric = metrics.at(0);
        EXPECT_EQ(metric.labels().at("EncryptionType"), "COORDINATOR_KEY");
        EXPECT_EQ(metric.labels().at("Application"), "APPLICATION_ECL");
        EXPECT_EQ(metric.labels().at("MatchKeyFormat"),
                  "MATCH_KEY_FORMAT_HASHED_ENCRYPTED");
        if (metric.name() == "KeyFetchingRequestCount") {
          EXPECT_EQ(metric.value(), "1");
          count_metrics_received++;
        } else if (metric.name() == "KeyFetchingRequestLatency") {
          EXPECT_FALSE(metric.value().empty());
          int64_t latency_ms = 0;
          EXPECT_TRUE(absl::SimpleAtoi(metric.value(), &latency_ms));
          latencies_recorded.push_back(latency_ms);
        } else {
          FAIL() << "Unexpected metric: " << metric.name();
        }
      });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      std::move(request),
      [&](AsyncContext<MatchRequest, MatchResponse>& context) {
        EXPECT_TRUE(context.status.ok());
        finished.Notify();
      },
      logger_);

  task_with_metrics.Match(context);
  finished.WaitForNotification();

  EXPECT_EQ(count_metrics_received, 2);
  ASSERT_EQ(latencies_recorded.size(), 2);
  // Key Group A sleeps for 10ms -> latency >= 10ms.
  // Key Group B sleeps for 50ms -> latency >= 50ms.
  EXPECT_GE(latencies_recorded[0], 10);
  EXPECT_GE(latencies_recorded[1], 50);
  EXPECT_GT(latencies_recorded[1], latencies_recorded[0]);
}

TEST_F(KmsEncryptedMatchTaskTest, RecordsKeyFetchingErrorMetrics) {
  google::scp::cpio::MockMetricClient mock_metric_client;
  KmsEncryptedMatchTask task_with_metrics(
      mock_lookup_service_client_.get(), mock_aead_crypto_client_.get(),
      mock_hasher_.get(), mock_hybrid_crypto_client_.get(),
      PrivateKeyEndpoints(), &mock_metric_client, "test_namespace");

  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));

  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(
          [](AsyncContext<EncryptionKeyInfo, CryptoKeyInterface> context) {
            context.status = absl::InternalError("fetching fail");
            context.Finish();
          });

  bool has_count = false;
  bool has_latency = false;
  bool has_error = false;
  EXPECT_CALL(mock_metric_client, PutMetrics)
      .Times(3)
      .WillRepeatedly([&has_count, &has_latency, &has_error](auto& ctx) {
        ctx.result = scp::core::SuccessExecutionResult();
        const auto& metrics = ctx.request->metrics();
        ASSERT_EQ(metrics.size(), 1);
        const auto& metric = metrics.at(0);
        if (metric.name() == "KeyFetchingRequestCount") {
          EXPECT_EQ(metric.value(), "1");
          has_count = true;
        } else if (metric.name() == "KeyFetchingRequestLatency") {
          EXPECT_FALSE(metric.value().empty());
          has_latency = true;
        } else if (metric.name() == "KeyFetchingRequestErrorCount") {
          EXPECT_EQ(metric.value(), "1");
          EXPECT_EQ(metric.labels().at("BackendError"), "UNKNOWN_ERROR");
          has_error = true;
        } else {
          FAIL() << "Unexpected metric: " << metric.name();
        }
      });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      std::move(request),
      [&](AsyncContext<MatchRequest, MatchResponse>& context) {
        finished.Notify();
      },
      logger_);

  task_with_metrics.Match(context);
  finished.WaitForNotification();

  EXPECT_TRUE(has_count);
  EXPECT_TRUE(has_latency);
  EXPECT_TRUE(has_error);
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithLookupServiceCryptoError) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));

  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_FAILED
          error_response: "Crypto error occurred Code: 2415853572"
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
        }
      )pb",
      &mock_lookup_resp));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_key.field().error_reason(),
                  backend::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR);
        EXPECT_TRUE(
            matched_key.field().matched_field_info().field_value().empty());
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchCompositeWithLookupServiceCryptoError) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "ZW5jX2ZpcnN0" }
              values { type: FIELD_TYPE_LAST_NAME value: "ZW5jX2xhc3Q=" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "90210" }
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("enc_first")).WillOnce(Return("John"));
  EXPECT_CALL(*mock_key, Decrypt("enc_last")).WillOnce(Return("Doe"));

  // Country code should be lower-cased in the hash input.
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("JohnDoeus90210"))
      .WillOnce(Return("hashed_address"));
  EXPECT_CALL(*mock_key, Encrypt("hashed_address"))
      .WillOnce(Return("encrypted_hashed_address"));

  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_FAILED
          error_response: "Crypto error occurred Code: 2415853572"
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2hhc2hlZF9hZGRyZXNz" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
        }
      )pb",
      &mock_lookup_response));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    MockLookupSuccess(mock_lookup_response, ctx);
  });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_key.composite_field().status(),
                  backend::STATUS_FAILED);
        EXPECT_EQ(matched_key.composite_field().error_reason(),
                  backend::ERROR_REASON_LOOKUP_SERVICE_CRYPTO_ERROR);
        for (const auto& info :
             matched_key.composite_field().matched_field_info()) {
          EXPECT_TRUE(info.field_value().empty());
        }
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

TEST_F(KmsEncryptedMatchTaskTest, MatchWithLookupServiceGenericError) {
  auto request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED_ENCRYPTED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        encryption_key {
          wrapped_key {
            key_type: KEY_TYPE_XCHACHA20_POLY1305
            encrypted_dek: "test_dek"
            kek_kms_resource_id: "test_kek_id"
            gcp_wrapped_key_info { wip_provider: "test_wip_provider" }
          }
        }
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "ZW5jcnlwdGVkX2VtYWls"  # "encrypted_email"
            }
          }
        }
      )pb",
      request.get()));
  auto mock_key = std::make_shared<MockCryptoKey>();
  EXPECT_CALL(*mock_aead_crypto_client_, GetCryptoKeyAsync)
      .WillOnce(absl::bind_front(MockGetCryptoKeySuccess, mock_key));
  EXPECT_CALL(*mock_key, Decrypt("encrypted_email"))
      .WillOnce(Return("decrypted_email"));

  LookupServiceResponse mock_lookup_resp;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_FAILED
          error_response: "Some generic error"
          client_data_record {
            lookup_key { key: "ZW5jcnlwdGVkX2VtYWls" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
        }
      )pb",
      &mock_lookup_resp));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).WillOnce([&](auto& ctx) {
    MockLookupSuccess(mock_lookup_resp, ctx);
  });

  absl::Notification finished;
  AsyncContext<MatchRequest, MatchResponse> context(
      request,
      [&](auto& ctx) {
        ASSERT_THAT(ctx.status, IsOk());
        const auto& matched_key =
            ctx.response->matched_data_records(0).matched_keys(0);
        EXPECT_EQ(matched_key.field().status(), backend::STATUS_FAILED);
        EXPECT_EQ(matched_key.field().error_reason(),
                  backend::ERROR_REASON_INTERNAL_ERROR);
        finished.Notify();
      },
      logger_);

  kms_encrypted_match_task_.Match(context);
  finished.WaitForNotification();
}

}  // namespace
}  // namespace google::confidential_match::match_service
