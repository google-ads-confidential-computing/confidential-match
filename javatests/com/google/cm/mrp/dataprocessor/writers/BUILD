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
    name = "CsvDataWriterTest",
    srcs = ["CsvDataWriterTest.java"],
    resources = [
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/dataprocessor/destinations",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/writers",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:guava",
        "//java/external:mockito",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "SerializedProtoDataWriterTest",
    srcs = ["SerializedProtoDataWriterTest.java"],
    resources = [
        "testdata/match_config_multiple_column_groups.json",
        "testdata/proto_schema_aws_wrapped_key.json",
        "testdata/proto_schema_data_types.json",
        "testdata/proto_schema_hashed.json",
        "testdata/proto_schema_multiple_column_groups.json",
        "testdata/proto_schema_wrapped_key.json",
        "//javatests/com/google/cm/mrp/dataprocessor/testdata",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:match_config_provider",
        "//java/com/google/cm/mrp/dataprocessor/common",
        "//java/com/google/cm/mrp/dataprocessor/destinations",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/writers",
        "//java/com/google/cm/mrp/models",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:guava",
        "//java/external:mockito",
        "//protos/mrp/api:java_proto",
        "//protos/mrp/backend:java_proto",
    ],
)
