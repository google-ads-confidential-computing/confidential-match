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

#include "cc/lookup_server/service/src/json_serialization_functions.h"

#include <string>

#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"

#include "protos/lookup_server/api/lookup.pb.h"

namespace google::confidential_match::lookup_server {

using ::google::confidential_match::lookup_server::proto_api::DataRecord;
using ::google::confidential_match::lookup_server::proto_api::KeyValue;
using ::google::confidential_match::lookup_server::proto_api::LookupRequest;
using ::google::confidential_match::lookup_server::proto_api::LookupResponse;
using ::google::confidential_match::lookup_server::proto_api::LookupResult;
using ::google::protobuf::util::MessageDifferencer;
using ::google::scp::core::BytesBuffer;

TEST(JsonSerializationFunctionsTest, ProtoToJsonBytesBufferSuccess) {
  LookupRequest request;
  request.add_associated_data_keys("userId");
  DataRecord* record = request.add_data_records();
  *record->mutable_lookup_key()->mutable_key() = "+16505551234";
  KeyValue* customer_id_field = record->add_metadata();
  *customer_id_field->mutable_key() = "customerId";
  *customer_id_field->mutable_string_value() = "1";
  request.set_key_format(LookupRequest::KEY_FORMAT_HASHED);

  BytesBuffer bytes_buffer;
  EXPECT_TRUE(ProtoToJsonBytesBuffer(request, &bytes_buffer).ok());

  EXPECT_EQ(
      bytes_buffer.ToString(),
      R"({"dataRecords":[{"lookupKey":{"key":"+16505551234"},"metadata":[{"key":"customerId","stringValue":"1"}]}],"keyFormat":"KEY_FORMAT_HASHED","associatedDataKeys":["userId"]})");
}

TEST(JsonSerializationFunctionsTest, JsonBytesBufferToProtoSuccess) {
  std::string serialized = R"({
      "lookupResults": [
          {
              "clientDataRecord": {
                  "lookupKey": {"key":"sample@gmail.com"},
                  "metadata": [{
                      "key": "customerId",
                      "stringValue": "1"
                  }]
              },
              "matchedDataRecords": [
                  {"associatedData": {"key":"userId", "stringValue":"1"}},
                  {"associatedData": {"key":"userId", "stringValue":"2"}},
              ]
          }
      ]
  })";
  BytesBuffer bytes_buffer(serialized);

  LookupResponse expected;
  LookupResult* match_result = expected.add_lookup_results();
  DataRecord* client_record = match_result->mutable_client_data_record();
  *client_record->mutable_lookup_key()->mutable_key() = "sample@gmail.com";
  KeyValue* customer_id_field = client_record->add_metadata();
  *customer_id_field->mutable_key() = "customerId";
  *customer_id_field->mutable_string_value() = "1";
  KeyValue* user1 =
      match_result->add_matched_data_records()->add_associated_data();
  *user1->mutable_key() = "userId";
  *user1->mutable_string_value() = "1";
  KeyValue* user2 =
      match_result->add_matched_data_records()->add_associated_data();
  *user2->mutable_key() = "userId";
  *user2->mutable_string_value() = "2";

  LookupResponse actual;
  EXPECT_TRUE(JsonBytesBufferToProto(bytes_buffer, &actual).ok());

  EXPECT_TRUE(MessageDifferencer::Equals(expected, actual));
}

}  // namespace google::confidential_match::lookup_server
