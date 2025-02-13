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
    name = "DataOutputPreparerTest",
    srcs = ["DataOutputPreparerTest.java"],
    resources = [
        "testdata/mic_ecw_parsed_schema.json",
    ],
    deps = [
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/preparers",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "NestedDataSourcePreparerTest",
    srcs = ["NestedDataSourcePreparerTest.java"],
    resources = [
        "testdata/mic_ecw_parsed_schema.json",
        "testdata/mic_ecw_unencrypted_grouped_pii_schema.json",
        "testdata/mic_undefined_column_format_schema.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/mrp/dataprocessor/preparers",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//javatests/com/google/cm/mrp/dataprocessor/formatters:EnhancedMatchMapperTest",
        "//protos/mrp/backend:java_proto",
    ],
)
