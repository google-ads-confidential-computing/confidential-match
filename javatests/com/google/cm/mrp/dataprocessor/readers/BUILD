# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

package(default_visibility = ["//visibility:public"])

java_test(
    name = "CsvDataReaderTest",
    srcs = [
        "CsvDataReaderTest.java",
    ],
    resources = [
        "testdata/gtag_coordinator_schema.json",
        "testdata/input_data.csv",
        "testdata/invalid_quotes_in_records.csv",
        "testdata/mic_ecw_encoded_schema.json",
        "testdata/too_few_columns_in_record_row.csv",
        "testdata/wrapped_key_encrypted_schema.json",
        "testdata/wrapped_key_with_wip_schema.json",
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/util",
        "//java/external:clients_cryptoclient",
        "//java/external:gcp_storage",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//java/external:protobuf_java",
        "//java/external:shared_util",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "ConfidentialMatchDataRecordParserImplTest",
    srcs = [
        "ConfidentialMatchDataRecordParserImplTest.java",
    ],
    resources = [
        "testdata/mic_proto_schema_encrypted.json",
        "testdata/mic_proto_schema_unencrypted.json",
        "testdata/mic_proto_schema_wrapped_encrypted.json",
        "testdata/mic_proto_schema_wrapped_with_wip_encrypted.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/common",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/util",
        "//java/external:clients_cryptoclient",
        "//java/external:gcp_storage",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//java/external:protobuf_java",
        "//java/external:shared_util",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "SerializedProtoDataReaderTest",
    srcs = [
        "SerializedProtoDataReaderTest.java",
    ],
    resources = [
        "testdata/input_proto.txt",
        "testdata/mic_proto_schema_encrypted.json",
        "testdata/mic_proto_schema_unencrypted.json",
        "testdata/mic_proto_schema_wrapped_encrypted.json",
        "testdata/mic_proto_schema_wrapped_with_wip_encrypted.json",
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/dataprocessor",
        "//java/com/google/cm/mrp/dataprocessor/common",
        "//java/com/google/cm/mrp/dataprocessor/converters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/readers",
        "//java/com/google/cm/util",
        "//java/external:clients_cryptoclient",
        "//java/external:gcp_storage",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//java/external:protobuf_java",
        "//java/external:shared_util",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)
