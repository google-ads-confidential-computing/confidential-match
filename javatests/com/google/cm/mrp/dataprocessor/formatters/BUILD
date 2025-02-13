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
    name = "DataErrorMatcherTest",
    srcs = ["DataErrorMatcherTest.java"],
    deps = [
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/external:google_truth",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "DataOutputFormatterImplTest",
    srcs = ["DataOutputFormatterImplTest.java"],
    resources = [
        "testdata/fanned_out_flat_data_output_formatter_schema.json",
        "testdata/flat_data_output_formatter_schema.json",
        "testdata/flat_input_with_condensing_data_output_formatter_schema.json",
        "testdata/nested_data_output_formatter_schema.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/dataprocessor/common",
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/mrp/dataprocessor/models",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "DataOutputSchemaValidatorTest",
    srcs = ["DataOutputSchemaValidatorTest.java"],
    resources = [
        "testdata/fanned_out_flat_data_output_formatter_schema.json",
        "testdata/flat_data_output_formatter_schema.json",
        "testdata/flat_input_with_condensing_data_output_formatter_schema.json",
        "testdata/invalid_output_schema_duplicate_column_names.json",
        "testdata/invalid_output_schema_missing_condense_mode.json",
        "testdata/invalid_output_schema_missing_condense_subcolumns.json",
        "testdata/invalid_output_schema_multiple_condense_columns.json",
        "testdata/invalid_output_schema_nested_column_missing_output_columns.json",
        "testdata/invalid_output_schema_nesting_requires_condensing.json",
        "testdata/nested_data_output_formatter_schema.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "DataSourceFormatterImplTest",
    srcs = ["DataSourceFormatterImplTest.java"],
    resources = [
        "testdata/duplicate_single_column_schema.json",
        "testdata/encryption_match_config.json",
        "testdata/formatter_match_config.json",
        "testdata/formatter_schema.json",
        "testdata/incomplete_composite_columns_schema.json",
        "testdata/invalid_schema_invalid_gtag_schema.json",
        "testdata/invalid_schema_no_column_format.json",
        "testdata/multi_composite_columns_schema.json",
        "testdata/no_encryption_key_schema.json",
        "testdata/wrapped_key_formatter_schema.json",
    ],
    deps = [
        "//java/com/google/cm/mrp:exceptions",
        "//java/com/google/cm/mrp:featureFlags",
        "//java/com/google/cm/mrp/clients/cryptoclient",
        "//java/com/google/cm/mrp/dataprocessor/common",
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//java/external:mockito",
        "//javatests/com/google/cm/mrp/testutils",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "EnhancedMatchMapperTest",
    srcs = ["EnhancedMatchMapperTest.java"],
    deps = [
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/external:google_truth",
        "//protos/mrp/backend:java_proto",
    ],
)

java_test(
    name = "ProtoDataOutputCondenserTest",
    srcs = ["ProtoDataOutputCondenserTest.java"],
    resources = [
        "testdata/flat_schema.json",
        "testdata/nested_data_output_condenser_schema.json",
    ],
    deps = [
        "//java/com/google/cm/mrp/dataprocessor/formatters",
        "//java/com/google/cm/util",
        "//java/external:google_truth",
        "//protos/mrp/backend:java_proto",
    ],
)
