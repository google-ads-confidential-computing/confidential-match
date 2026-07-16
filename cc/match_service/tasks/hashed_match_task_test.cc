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

#include "cc/match_service/tasks/hashed_match_task.h"

#include <memory>

#include "absl/functional/bind_front.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"

#include "cc/core/async/async_context.h"
#include "cc/core/hash/mock_hasher.h"
#include "cc/core/logger/logger_interface.h"
#include "cc/match_service/lookup_service_client/mock_lookup_service_client.h"
#include "protos/match_service/backend/match_service.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::MockHasher;
using ::google::confidential_match::match_service::MockLookupServiceClient;
using ::google::confidential_match::match_service::backend::Application;
using ::google::confidential_match::match_service::backend::DataRecord;
using ::google::confidential_match::match_service::backend::Field;
using ::google::confidential_match::match_service::backend::FieldType;
using ::google::confidential_match::match_service::backend::KeyValue;
using ::google::confidential_match::match_service::backend::LookupDataRecord;
using ::google::confidential_match::match_service::backend::LookupResult;
using ::google::confidential_match::match_service::backend::
    LookupServiceRequest;
using ::google::confidential_match::match_service::backend::
    LookupServiceResponse;
using ::google::confidential_match::match_service::backend::MatchKey;
using ::google::confidential_match::match_service::backend::MatchKeyEncoding;
using ::google::confidential_match::match_service::backend::MatchKeyFormat;
using ::google::confidential_match::match_service::backend::MatchRequest;
using ::google::confidential_match::match_service::backend::MatchResponse;
using ::google::protobuf::TextFormat;
using ::google::scp::core::test::EqualsProto;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Eq;
using ::testing::Invoke;
using ::testing::Return;

class HashedMatchTaskTest : public ::testing::Test {
 protected:
  HashedMatchTaskTest()
      : logger_(std::make_shared<Logger>(GlobalLogger())),
        mock_lookup_service_client_(
            std::make_unique<MockLookupServiceClient>()),
        mock_hasher_(std::make_unique<MockHasher>()),
        hashed_match_task_(mock_lookup_service_client_.get(),
                           mock_hasher_.get()) {}

  std::shared_ptr<LoggerInterface> logger_;
  std::unique_ptr<MockLookupServiceClient> mock_lookup_service_client_;
  std::unique_ptr<MockHasher> mock_hasher_;
  HashedMatchTask hashed_match_task_;
};

// Helper to mock a successful lookup call with the provided response, while
// also validating that the request matches the provided expected request.
void MockLookupSuccessValidatingRequest(
    const LookupServiceRequest& expected_request,
    const LookupServiceResponse& response,
    AsyncContext<LookupServiceRequest, LookupServiceResponse>&
        context) noexcept {
  EXPECT_THAT(*context.request, EqualsProto(expected_request));
  context.response = std::make_shared<LookupServiceResponse>(response);
  context.Finish();
}

TEST_F(HashedMatchTaskTest, MatchSingleUnmatchedRecordSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "nomatch" } }
        }
      )pb",
      context.request.get()));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "nomatch" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "nomatch" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_UNMATCHED
              matched_field_info { field_type: FIELD_TYPE_EMAIL }
            }
          }
        }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchSingleMatchedRecordSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "matched" } }
        }
      )pb",
      context.request.get()));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "matched" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "matched" } }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "matched" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "matched"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchSingleMatchedRecordAssociatedDataSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "matched" } }
        }
        associated_data_types: ASSOCIATED_DATA_TYPE_FIRST_PARTY_IDENTIFIER
      )pb",
      context.request.get()));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "matched" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "matched" }
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
        data_records {
          lookup_key { key: "matched" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        associated_data_keys: "encrypted_gaia_id"
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "matched"
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
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchSingleMatchedRecordHexSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_HEX
        data_records {
          match_keys {
            field {
              type: FIELD_TYPE_EMAIL
              value: "6d61746368656440676f6f676c652e636f6d"
            }
          }
        }
      )pb",
      context.request.get()));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "bWF0Y2hlZEBnb29nbGUuY29t" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "bWF0Y2hlZEBnb29nbGUuY29t" }
          }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "bWF0Y2hlZEBnb29nbGUuY29t" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "bWF0Y2hlZEBnb29nbGUuY29t"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchSingleMatchedRecordWebSafeSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64_WEB_SAFE
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "Pl8-" } }
        }
      )pb",
      context.request.get()));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "Pl8+" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "Pl8+" } }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "Pl8+" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "Pl8+"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchSingleMatchedCompositeRecordHexSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_HEX
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "4c61727279" }
              values { type: FIELD_TYPE_LAST_NAME value: "50616765" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("TGFycnk=UGFnZQ==us94043"))
      .WillOnce(Return("SergeyCombinedAddressHash"));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "SergeyCombinedAddressHash" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "SergeyCombinedAddressHash" }
          }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "SergeyCombinedAddressHash" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_FIRST_NAME
                field_value: "TGFycnk="
              }
              matched_field_info {
                field_type: FIELD_TYPE_LAST_NAME
                field_value: "UGFnZQ=="
              }
              matched_field_info {
                field_type: FIELD_TYPE_COUNTRY_CODE
                field_value: "US"
              }
              matched_field_info {
                field_type: FIELD_TYPE_ZIP_CODE
                field_value: "94043"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchSingleMatchedCompositeRecordSuccess) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "SergeyHash" }
              values { type: FIELD_TYPE_LAST_NAME value: "BrinHash" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));
  EXPECT_CALL(*mock_hasher_, Base64EncodedHash("SergeyHashBrinHashus94043"))
      .WillOnce(Return("SergeyCombinedAddressHash"));
  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "SergeyCombinedAddressHash" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records {
            lookup_key { key: "SergeyCombinedAddressHash" }
          }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "SergeyCombinedAddressHash" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_FIRST_NAME
                field_value: "SergeyHash"
              }
              matched_field_info {
                field_type: FIELD_TYPE_LAST_NAME
                field_value: "BrinHash"
              }
              matched_field_info {
                field_type: FIELD_TYPE_COUNTRY_CODE
                field_value: "US"
              }
              matched_field_info {
                field_type: FIELD_TYPE_ZIP_CODE
                field_value: "94043"
              }
            }
          }
        }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchMultipleWithMetadataAllLevels) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        metadata { key: "top_level_metadata_key" string_value: "a" }
        data_records {
          metadata { key: "data_record_level_metadata_key" int_value: 0 }
          match_keys {
            metadata { key: "match_key_level_metadata_key" string_value: "0_0" }
            field { type: FIELD_TYPE_PHONE value: "+16505551234" }
          }
          match_keys {
            metadata { key: "match_key_level_metadata_key" string_value: "0_1" }
            field { type: FIELD_TYPE_EMAIL value: "matched" }
          }
        }
        data_records {
          metadata { key: "data_record_level_metadata_key" int_value: 1 }
          match_keys {
            metadata { key: "match_key_level_metadata_key" string_value: "1_0" }
            field { type: FIELD_TYPE_PHONE value: "+16505551235" }
          }
        }
      )pb",
      context.request.get()));
  LookupServiceResponse mock_lookup_response;
  // Ensure the code functions even if lookup result order is not preserved
  // from the lookup request order.
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "+16505551234" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
        }
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "+16505551235" }
            metadata { key: "d" int_value: 1 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "+6505551235" } }
        }
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "matched" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 1 }
          }
          matched_data_records { lookup_key { key: "matched" } }
        }
      )pb",
      &mock_lookup_response));

  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "+16505551234" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        data_records {
          lookup_key { key: "matched" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 1 }
        }
        data_records {
          lookup_key { key: "+16505551235" }
          metadata { key: "d" int_value: 1 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        application: APPLICATION_ECL
        hash_info { hash_type: HASH_TYPE_SHA_256 }
      )pb",
      &expected_lookup_request));
  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));
  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_UNMATCHED
              matched_field_info { field_type: FIELD_TYPE_PHONE }
            }
            metadata { key: "match_key_level_metadata_key" string_value: "0_0" }
          }
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "matched"
              }
            }
            metadata { key: "match_key_level_metadata_key" string_value: "0_1" }
          }
          metadata { key: "data_record_level_metadata_key" int_value: 0 }
        }
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_PHONE
                field_value: "+16505551235"
              }
            }
            metadata { key: "match_key_level_metadata_key" string_value: "1_0" }
          }
          metadata { key: "data_record_level_metadata_key" int_value: 1 }
        }
        metadata { key: "top_level_metadata_key" string_value: "a" }
      )pb",
      &expected_match_response));
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithUnspecifiedApplication) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_UNSPECIFIED
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "nomatch" } }
        }
      )pb",
      context.request.get()));

  context.callback = [&](auto& ctx) {
    EXPECT_THAT(ctx.status, StatusIs(absl::StatusCode::kInvalidArgument));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithUnspecifiedMatchKeyFormat) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_UNSPECIFIED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "nomatch" } }
        }
      )pb",
      context.request.get()));

  context.callback = [&](auto& ctx) {
    EXPECT_THAT(ctx.status, StatusIs(absl::StatusCode::kInvalidArgument));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchPartialFailureEncodingError) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "matched" } }
          match_keys {
            field { type: FIELD_TYPE_EMAIL value: "invalid-base-64" }
          }
        }
      )pb",
      context.request.get()));

  // Only one record should be in the lookup request.
  LookupServiceRequest expected_lookup_request;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        data_records {
          lookup_key { key: "matched" }
          metadata { key: "d" int_value: 0 }
          metadata { key: "m" int_value: 0 }
        }
        key_format: KEY_FORMAT_HASHED
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        application: APPLICATION_ECL
      )pb",
      &expected_lookup_request));

  LookupServiceResponse mock_lookup_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        lookup_results {
          status: STATUS_SUCCESS
          client_data_record {
            lookup_key { key: "matched" }
            metadata { key: "d" int_value: 0 }
            metadata { key: "m" int_value: 0 }
          }
          matched_data_records { lookup_key { key: "matched" } }
        }
      )pb",
      &mock_lookup_response));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(absl::bind_front(MockLookupSuccessValidatingRequest,
                                 expected_lookup_request,
                                 mock_lookup_response));

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_SUCCESS_MATCHED
              matched_field_info {
                field_type: FIELD_TYPE_EMAIL
                field_value: "matched"
              }
            }
          }
          matched_keys {
            field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type: FIELD_TYPE_EMAIL }
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithUnspecifiedKeyEncoding) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_UNSPECIFIED
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "nomatch" } }
        }
      )pb",
      context.request.get()));

  context.callback = [&](auto& ctx) {
    EXPECT_THAT(ctx.status, StatusIs(absl::StatusCode::kInvalidArgument));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithInvalidBase64KeyEncoding) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            field { type: FIELD_TYPE_EMAIL value: "invalid-base-64" }
          }
        }
      )pb",
      context.request.get()));

  // No Lookup call expected since all records failed validation.
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type: FIELD_TYPE_EMAIL }
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithInvalidBase64WebSafeKeyEncoding) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64_WEB_SAFE
        data_records {
          match_keys {
            field { type: FIELD_TYPE_EMAIL value: "invalid/web-safe" }
          }
        }
      )pb",
      context.request.get()));

  // No Lookup call expected since all records failed validation.
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type: FIELD_TYPE_EMAIL }
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithInvalidHexKeyEncoding) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_HEX
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "nothex" } }
        }
      )pb",
      context.request.get()));

  // No Lookup call expected since all records failed validation.
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type: FIELD_TYPE_EMAIL }
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithLookupServiceError) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_EMAIL value: "nomatch" } }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup)
      .WillOnce(Invoke([](auto& ctx) {
        ctx.status = absl::InternalError("Lookup failed");
        ctx.Finish();
      }));

  context.callback = [&](auto& ctx) {
    EXPECT_THAT(ctx.status, StatusIs(absl::StatusCode::kInternal));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchWithZeroDataRecordsShortCircuits) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        # Zero data_records here
      )pb",
      context.request.get()));
  // Lookup should be short-circuited (0 calls).
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);
  MatchResponse expected_match_response;  // Empty response expected
  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };
  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchUnsupportedSimpleFieldType) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys { field { type: FIELD_TYPE_FIRST_NAME value: "Larry" } }
        }
      )pb",
      context.request.get()));

  // Lookup should be short-circuited (0 calls) because the only record failed
  // validation.
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_INVALID_MATCH_KEY_FIELD
              matched_field_info { field_type: FIELD_TYPE_FIRST_NAME }
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchFanOutFieldUnsupported) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            fan_out_key_field {
              type: FAN_OUT_FIELD_TYPE_GTAG
              value: "gtag_value"
            }
          }
        }
      )pb",
      context.request.get()));

  // Lookup should be short-circuited (0 calls).
  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            fan_out_key_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_INTERNAL_ERROR
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };
  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchCompositeFieldInvalidEncoding) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "invalid-base-64" }
              values { type: FIELD_TYPE_LAST_NAME value: "last" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type:
                                       FIELD_TYPE_FIRST_NAME }  # Redacted
              matched_field_info { field_type:
                                       FIELD_TYPE_LAST_NAME }  # Redacted
              matched_field_info { field_type:
                                       FIELD_TYPE_COUNTRY_CODE }      # Redacted
              matched_field_info { field_type: FIELD_TYPE_ZIP_CODE }  # Redacted
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchCompositeFieldInvalidEncodingLaterField) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "name" }  # valid
              values { type: FIELD_TYPE_LAST_NAME
                       value: "invalid-base-64" }  # invalid
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type:
                                       FIELD_TYPE_FIRST_NAME }  # Redacted
              matched_field_info { field_type:
                                       FIELD_TYPE_LAST_NAME }  # Redacted
              matched_field_info { field_type:
                                       FIELD_TYPE_COUNTRY_CODE }      # Redacted
              matched_field_info { field_type: FIELD_TYPE_ZIP_CODE }  # Redacted
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchCompositeFieldInvalidHexEncoding) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_HEX
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME
                       value: "nothex" }                         # invalid hex
              values { type: FIELD_TYPE_LAST_NAME value: "73" }  # valid hex
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_DECODING_ERROR
              matched_field_info { field_type:
                                       FIELD_TYPE_FIRST_NAME }  # Redacted
              matched_field_info { field_type:
                                       FIELD_TYPE_LAST_NAME }  # Redacted
              matched_field_info { field_type:
                                       FIELD_TYPE_COUNTRY_CODE }      # Redacted
              matched_field_info { field_type: FIELD_TYPE_ZIP_CODE }  # Redacted
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchUnsupportedCompositeFieldType) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_UNSPECIFIED
              values { type: FIELD_TYPE_FIRST_NAME value: "Larry" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_INVALID_MATCH_KEY_FIELD
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchCompositeFieldInvalidSize) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "Larry" }
              values { type: FIELD_TYPE_LAST_NAME value: "Page" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_INVALID_MATCH_KEY_FIELD
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchCompositeFieldDuplicateFields) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "Larry" }
              values { type: FIELD_TYPE_FIRST_NAME value: "Sergey" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_INVALID_MATCH_KEY_FIELD
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

TEST_F(HashedMatchTaskTest, MatchCompositeFieldUnsupportedSubFieldType) {
  AsyncContext<MatchRequest, MatchResponse> context(logger_);
  context.request = std::make_shared<MatchRequest>();
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        application: APPLICATION_ECL
        match_key_format: MATCH_KEY_FORMAT_HASHED
        key_encoding: MATCH_KEY_ENCODING_BASE64
        data_records {
          match_keys {
            composite_field {
              type: COMPOSITE_FIELD_TYPE_ADDRESS
              values { type: FIELD_TYPE_FIRST_NAME value: "Larry" }
              values { type: FIELD_TYPE_EMAIL value: "page@google.com" }
              values { type: FIELD_TYPE_COUNTRY_CODE value: "US" }
              values { type: FIELD_TYPE_ZIP_CODE value: "94043" }
            }
          }
        }
      )pb",
      context.request.get()));

  EXPECT_CALL(*mock_lookup_service_client_, Lookup).Times(0);

  MatchResponse expected_match_response;
  ASSERT_TRUE(TextFormat::ParseFromString(
      R"pb(
        matched_data_records {
          matched_keys {
            composite_field {
              status: STATUS_FAILED
              error_reason: ERROR_REASON_INVALID_MATCH_KEY_FIELD
            }
          }
        }
      )pb",
      &expected_match_response));

  context.callback = [&](auto& ctx) {
    ASSERT_THAT(ctx.status, IsOk());
    EXPECT_THAT(*ctx.response, EqualsProto(expected_match_response));
  };

  hashed_match_task_.Match(context);
}

}  // namespace
}  // namespace google::confidential_match::match_service
