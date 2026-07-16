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

#include "cc/match_service/converters/lookup_request_converter.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "cc/core/test/utils/proto_test_utils.h"
#include "gtest/gtest.h"

#include "cc/match_service/error/error.h"
#include "protos/lookup_server/api/lookup.pb.h"
#include "protos/match_service/backend/error.pb.h"
#include "protos/match_service/backend/lookup.pb.h"

namespace google::confidential_match::match_service {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::google::confidential_match::match_service::backend::Error;
using ::google::scp::core::test::EqualsProto;
using ::google::scp::core::test::SubstituteAndParseTextToProto;

class LookupRequestConverterTest : public ::testing::Test {
 protected:
  static backend::LookupServiceRequest CreateValidBackendRequest() {
    backend::LookupServiceRequest request;
    request.set_key_format(backend::LookupServiceRequest::KEY_FORMAT_HASHED);
    auto* record = request.add_data_records();
    record->mutable_lookup_key()->set_key("lookup_key");
    request.mutable_sharding_scheme()->set_type("jch");
    request.mutable_sharding_scheme()->set_num_shards(1);
    request.mutable_hash_info()->set_hash_type(
        backend::LookupServiceRequest::HashInfo::HASH_TYPE_SHA_256);
    return request;
  }
};

TEST_F(LookupRequestConverterTest, ConvertBackendRequestToApiSuccess) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  auto* key_value = input_request.mutable_data_records(0)->add_metadata();
  key_value->set_key("meta_key");
  key_value->set_string_value("meta_value");
  lookup_server::proto_api::LookupRequest expected_request;
  expected_request.set_key_format(
      lookup_server::proto_api::LookupRequest::KEY_FORMAT_HASHED);
  auto* api_record = expected_request.add_data_records();
  api_record->mutable_lookup_key()->set_key("lookup_key");
  auto* api_key_value = api_record->add_metadata();
  api_key_value->set_key("meta_key");
  api_key_value->set_string_value("meta_value");
  expected_request.mutable_sharding_scheme()->set_type("jch");
  expected_request.mutable_sharding_scheme()->set_num_shards(1);
  expected_request.mutable_hash_info()->set_hash_type(
      lookup_server::proto_api::LookupRequest::HashInfo::HASH_TYPE_SHA_256);
  lookup_server::proto_api::LookupRequest actual_request;

  EXPECT_THAT(ToLookupApi(input_request, actual_request), IsOk());

  EXPECT_THAT(actual_request, EqualsProto(expected_request));
}

TEST_F(LookupRequestConverterTest, ConvertApiResponseToBackendSuccess) {
  lookup_server::proto_api::LookupResponse input_response;
  auto* result = input_response.add_lookup_results();
  result->set_status(lookup_server::proto_api::LookupResult::STATUS_SUCCESS);
  result->mutable_client_data_record()->mutable_lookup_key()->set_key(
      "lookup_key");
  auto* matched_record = result->add_matched_data_records();
  matched_record->mutable_lookup_key()->set_key("lookup_key");
  auto* associated_key_value = matched_record->add_associated_data();
  associated_key_value->set_key("data_key");
  associated_key_value->set_string_value("data_value");
  backend::LookupServiceResponse expected_response;
  auto* backend_result = expected_response.add_lookup_results();
  backend_result->set_status(backend::LookupResult::STATUS_SUCCESS);
  backend_result->mutable_client_data_record()->mutable_lookup_key()->set_key(
      "lookup_key");
  auto* backend_matched_record = backend_result->add_matched_data_records();
  backend_matched_record->mutable_lookup_key()->set_key("lookup_key");
  auto* backend_associated_key_value =
      backend_matched_record->add_associated_data();
  backend_associated_key_value->set_key("data_key");
  backend_associated_key_value->set_string_value("data_value");
  backend::LookupServiceResponse actual_response;

  EXPECT_THAT(ToBackend(input_response, actual_response), IsOk());

  EXPECT_THAT(actual_response, EqualsProto(expected_response));
}

TEST_F(LookupRequestConverterTest, ToLookupApiWithFullWrappedKeyFields) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED);
  auto* wrapped = input_request.mutable_encryption_key()->mutable_wrapped_key();
  wrapped->set_key_type(backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("dek");
  wrapped->set_kek_kms_resource_id("kek");
  auto* gcp = wrapped->mutable_gcp_wrapped_key_info();
  gcp->set_wip_provider("wip");
  lookup_server::proto_api::LookupRequest output_request;

  EXPECT_THAT(ToLookupApi(input_request, output_request), IsOk());

  const auto& out_wrapped =
      output_request.encryption_key_info().wrapped_key_info();
  EXPECT_EQ(out_wrapped.encrypted_dek(), "dek");
  EXPECT_EQ(out_wrapped.kek_kms_resource_id(), "kek");
  EXPECT_EQ(out_wrapped.gcp_wrapped_key_info().wip_provider(), "wip");

  // GCP missing WIP
  wrapped->clear_gcp_wrapped_key_info();
  auto* aws = wrapped->mutable_aws_wrapped_key_info();
  aws->set_role_arn("role");

  EXPECT_THAT(ToLookupApi(input_request, output_request), IsOk());

  const auto& aws_wrapped =
      output_request.encryption_key_info().wrapped_key_info();
  EXPECT_EQ(aws_wrapped.encrypted_dek(), "dek");
  EXPECT_EQ(aws_wrapped.kek_kms_resource_id(), "kek");
  EXPECT_EQ(aws_wrapped.aws_wrapped_key_info().role_arn(), "role");
}

TEST_F(LookupRequestConverterTest, ToLookupApiWithFullCoordinatorKeyFields) {
  backend::LookupServiceRequest input_request =
      SubstituteAndParseTextToProto<backend::LookupServiceRequest>(R"pb(
        key_format: KEY_FORMAT_HASHED_ENCRYPTED
        data_records { lookup_key { key: "lookup_key" } }
        sharding_scheme { type: "jch" num_shards: 1 }
        hash_info { hash_type: HASH_TYPE_SHA_256 }
        encryption_key {
          coordinator_key {
            key_id: "test_key_id"
            coordinator_info {
              key_service_endpoint: "endpoint1"
              kms_wip_provider: "wip1"
              key_service_audience_url: "url1"
            }
            coordinator_info {
              key_service_endpoint: "endpoint2"
              kms_wip_provider: "wip2"
              key_service_audience_url: "url2"
            }
          }
        }
      )pb");

  lookup_server::proto_api::LookupRequest expected_request =
      SubstituteAndParseTextToProto<lookup_server::proto_api::LookupRequest>(
          R"pb(
            key_format: KEY_FORMAT_HASHED_ENCRYPTED
            data_records { lookup_key { key: "lookup_key" } }
            sharding_scheme { type: "jch" num_shards: 1 }
            hash_info { hash_type: HASH_TYPE_SHA_256 }
            encryption_key_info {
              coordinator_key_info {
                key_id: "test_key_id"
                coordinator_info {
                  key_service_endpoint: "endpoint1"
                  kms_wip_provider: "wip1"
                  key_service_audience_url: "url1"
                }
                coordinator_info {
                  key_service_endpoint: "endpoint2"
                  kms_wip_provider: "wip2"
                  key_service_audience_url: "url2"
                }
              }
            }
          )pb");

  lookup_server::proto_api::LookupRequest output_request;
  EXPECT_THAT(ToLookupApi(input_request, output_request), IsOk());
  EXPECT_THAT(output_request, EqualsProto(expected_request));
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingKeyFormat) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.clear_key_format();
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Invalid backend key format."));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingShardingScheme) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.clear_sharding_scheme();
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Backend lookup request missing sharding scheme."));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingHashInfo) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.clear_hash_info();
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Backend lookup request missing hash info."));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingEncryptionKey) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED);
  input_request.clear_encryption_key();
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Backend lookup request missing encryption_key for "
                       "HASHED_ENCRYPTED format"));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingDataRecordLookupKey) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.mutable_data_records(0)->clear_lookup_key();
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Backend input does not have lookup key."));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingKeyValueKey) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  auto* key_value = input_request.mutable_data_records(0)->add_metadata();
  key_value->clear_key();  // Key is missing
  key_value->set_string_value("some-val");
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Backend keyValue does not contain key."));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingEncryptedDek) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED);
  auto* wrapped = input_request.mutable_encryption_key()->mutable_wrapped_key();
  wrapped->set_key_type(backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  // Missing encrypted_dek
  wrapped->set_kek_kms_resource_id("kek-id");
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Backend WrappedKey missing encrypted dek"));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingKek) {
  backend::LookupServiceRequest input_request = CreateValidBackendRequest();
  input_request.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED);
  auto* wrapped = input_request.mutable_encryption_key()->mutable_wrapped_key();
  wrapped->set_key_type(backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("dek");
  lookup_server::proto_api::LookupRequest output_request;

  absl::Status result = ToLookupApi(input_request, output_request);

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Backend WrappedKey missing kek"));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToLookupApiMissingCloudWrappedKeyDetails) {
  backend::LookupServiceRequest input = CreateValidBackendRequest();
  input.set_key_format(
      backend::LookupServiceRequest::KEY_FORMAT_HASHED_ENCRYPTED);

  auto* wrapped = input.mutable_encryption_key()->mutable_wrapped_key();
  wrapped->set_key_type(backend::KeyType::KEY_TYPE_XCHACHA20_POLY1305);
  wrapped->set_encrypted_dek("dek");
  wrapped->set_kek_kms_resource_id("kek-id");

  // AWS missing role
  auto* aws = wrapped->mutable_aws_wrapped_key_info();
  aws->clear_role_arn();
  lookup_server::proto_api::LookupRequest output_request;
  absl::Status result = ToLookupApi(input, output_request);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               "Backend AwsWrappedKeyInfo missing role arn"));
  EXPECT_EQ(GetBackendErrorReason(result), Error::CONVERTER_PARSE_ERROR);

  // GCP missing WIP
  wrapped->clear_aws_wrapped_key_info();
  auto* gcp = wrapped->mutable_gcp_wrapped_key_info();
  gcp->clear_wip_provider();
  absl::Status result2 = ToLookupApi(input, output_request);
  EXPECT_THAT(result2,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Backend GcpWrappedKeyInfo missing wip provider"));
  EXPECT_EQ(GetBackendErrorReason(result2), Error::CONVERTER_PARSE_ERROR);
}

TEST_F(LookupRequestConverterTest, ToBackendMissingLookupKeyInDataRecord) {
  lookup_server::proto_api::LookupResponse input_response;
  auto* result = input_response.add_lookup_results();
  result->set_status(lookup_server::proto_api::LookupResult::STATUS_SUCCESS);
  // Client data record exists but has empty/missing lookup key
  result->mutable_client_data_record()->mutable_lookup_key()->clear_key();
  backend::LookupServiceResponse output_response;

  absl::Status status = ToBackend(input_response, output_response);

  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInternal,
          "LookupService response client data record missing lookup key."));
  EXPECT_EQ(GetBackendErrorReason(status), Error::INTERNAL_ERROR);
}

TEST_F(LookupRequestConverterTest, ToBackendMissingLookupKeyInMatchedRecord) {
  lookup_server::proto_api::LookupResponse input_response;
  auto* result = input_response.add_lookup_results();
  result->set_status(lookup_server::proto_api::LookupResult::STATUS_SUCCESS);
  result->mutable_client_data_record()->mutable_lookup_key()->set_key(
      "lookup_key");
  auto* matched_record = result->add_matched_data_records();
  matched_record->mutable_lookup_key()->clear_key();  // Missing key
  backend::LookupServiceResponse output_response;

  absl::Status status = ToBackend(input_response, output_response);

  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInternal,
          "LookupService InternalError MatchedDataRecord missing lookup key."));
  EXPECT_EQ(GetBackendErrorReason(status), Error::INTERNAL_ERROR);
}

TEST_F(LookupRequestConverterTest, ToBackendMissingKeyValueKey) {
  lookup_server::proto_api::LookupResponse input_response;
  auto* result = input_response.add_lookup_results();
  result->set_status(lookup_server::proto_api::LookupResult::STATUS_SUCCESS);
  auto* record = result->mutable_client_data_record();
  record->mutable_lookup_key()->set_key("k");
  auto* key_value = record->add_metadata();
  key_value->clear_key();  // Missing key
  key_value->set_string_value("v");
  backend::LookupServiceResponse output_response;

  absl::Status status = ToBackend(input_response, output_response);

  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kInternal,
                       "LookupService response missing key_value_pair key."));
  EXPECT_EQ(GetBackendErrorReason(status), Error::INTERNAL_ERROR);
}

}  // namespace
}  // namespace google::confidential_match::match_service
