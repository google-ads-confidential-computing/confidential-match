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

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//cc/match_service:__subpackages__"])

cc_library(
    name = "match_request_converters",
    srcs = ["match_request_converters.cc"],
    hdrs = ["match_request_converters.h"],
    deps = [
        ":application_converters",
        ":associated_data_type_converters",
        ":data_record_converters",
        ":encryption_key_converters",
        ":key_value_converters",
        ":match_key_encoding_converters",
        ":match_key_format_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_request_converters_test",
    size = "small",
    srcs = ["match_request_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":match_request_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "match_response_converters",
    srcs = ["match_response_converters.cc"],
    hdrs = ["match_response_converters.h"],
    deps = [
        ":associated_data_converter",
        ":key_value_converters",
        ":matched_data_record_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_response_converters_test",
    size = "small",
    srcs = ["match_response_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":match_response_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "lookup_associated_data_converter",
    srcs = ["lookup_associated_data_converter.cc"],
    hdrs = ["lookup_associated_data_converter.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:lookup_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "lookup_associated_data_converter_test",
    size = "small",
    srcs = ["lookup_associated_data_converter_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":lookup_associated_data_converter",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:lookup_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "lookup_request_converter",
    srcs = ["lookup_request_converter.cc"],
    hdrs = ["lookup_request_converter.h"],
    deps = [
        "//cc/common/proto_utils",
        "//cc/core/error:status_macros",
        "//cc/external:absl_log",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/match_service/error",
        "//protos/lookup_server/api:cc_proto",
        "//protos/match_service/backend:lookup_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "lookup_request_converter_test",
    srcs = ["lookup_request_converter_test.cc"],
    deps = [
        ":lookup_request_converter",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//cc/match_service/error",
        "//protos/lookup_server/api:cc_proto",
        "//protos/match_service/backend:lookup_cc_proto",
    ],
)

cc_library(
    name = "associated_data_converter",
    srcs = ["associated_data_converter.cc"],
    hdrs = ["associated_data_converter.h"],
    deps = [
        ":first_party_identifier_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "associated_data_converter_test",
    size = "small",
    srcs = ["associated_data_converter_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":associated_data_converter",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "application_converters",
    srcs = ["application_converters.cc"],
    hdrs = ["application_converters.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "application_converters_test",
    size = "small",
    srcs = ["application_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":application_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "coordinator_key_converters",
    srcs = ["coordinator_key_converters.cc"],
    hdrs = ["coordinator_key_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "coordinator_key_converters_test",
    size = "small",
    srcs = ["coordinator_key_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":coordinator_key_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "data_record_associated_data_converter",
    srcs = ["data_record_associated_data_converter.cc"],
    hdrs = ["data_record_associated_data_converter.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "data_record_associated_data_converter_test",
    size = "small",
    srcs = ["data_record_associated_data_converter_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":data_record_associated_data_converter",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "data_record_converters",
    srcs = ["data_record_converters.cc"],
    hdrs = ["data_record_converters.h"],
    deps = [
        ":encryption_key_converters",
        ":key_value_converters",
        ":match_key_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "data_record_converters_test",
    size = "small",
    srcs = ["data_record_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":data_record_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "encryption_key_converters",
    srcs = ["encryption_key_converters.cc"],
    hdrs = ["encryption_key_converters.h"],
    deps = [
        ":coordinator_key_converters",
        ":wrapped_key_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "encryption_key_converters_test",
    size = "small",
    srcs = ["encryption_key_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":encryption_key_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/core:cc_proto",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "field_type_converters",
    srcs = ["field_type_converters.cc"],
    hdrs = ["field_type_converters.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "field_type_converters_test",
    size = "small",
    srcs = ["field_type_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":field_type_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "field_converters",
    srcs = ["field_converters.cc"],
    hdrs = ["field_converters.h"],
    deps = [
        ":field_type_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "field_converters_test",
    size = "small",
    srcs = ["field_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":field_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "first_party_identifier_converters",
    srcs = ["first_party_identifier_converters.cc"],
    hdrs = ["first_party_identifier_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "first_party_identifier_converters_test",
    size = "small",
    srcs = ["first_party_identifier_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":first_party_identifier_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "key_value_converters",
    srcs = ["key_value_converters.cc"],
    hdrs = ["key_value_converters.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "key_value_converters_test",
    size = "small",
    srcs = ["key_value_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":key_value_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "match_key_converters",
    srcs = ["match_key_converters.cc"],
    hdrs = ["match_key_converters.h"],
    deps = [
        ":encryption_key_converters",
        ":field_converters",
        ":key_value_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_key_converters_test",
    size = "small",
    srcs = ["match_key_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":match_key_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "matched_key_converters",
    srcs = ["matched_key_converters.cc"],
    hdrs = ["matched_key_converters.h"],
    deps = [
        ":key_value_converters",
        ":matched_field_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "matched_key_converters_test",
    size = "small",
    srcs = ["matched_key_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":matched_key_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "wrapped_key_converters",
    srcs = ["wrapped_key_converters.cc"],
    hdrs = ["wrapped_key_converters.h"],
    deps = [
        ":key_type_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "wrapped_key_converters_test",
    size = "small",
    srcs = ["wrapped_key_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":wrapped_key_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "match_key_format_converters",
    srcs = ["match_key_format_converters.cc"],
    hdrs = ["match_key_format_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_key_format_converters_test",
    size = "small",
    srcs = ["match_key_format_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":match_key_format_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "match_key_encoding_converters",
    srcs = ["match_key_encoding_converters.cc"],
    hdrs = ["match_key_encoding_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_key_encoding_converters_test",
    size = "small",
    srcs = ["match_key_encoding_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":match_key_encoding_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "key_type_converters",
    srcs = ["key_type_converters.cc"],
    hdrs = ["key_type_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "key_type_converters_test",
    size = "small",
    srcs = ["key_type_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":key_type_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "associated_data_type_converters",
    srcs = ["associated_data_type_converters.cc"],
    hdrs = ["associated_data_type_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "associated_data_type_converters_test",
    size = "small",
    srcs = ["associated_data_type_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":associated_data_type_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "status_converters",
    srcs = ["status_converters.cc"],
    hdrs = ["status_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "status_converters_test",
    size = "small",
    srcs = ["status_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":status_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "error_reason_converters",
    srcs = ["error_reason_converters.cc"],
    hdrs = ["error_reason_converters.h"],
    deps = [
        "//cc/external:absl_status",
        "//cc/match_service/error",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "error_reason_converters_test",
    size = "small",
    srcs = ["error_reason_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":error_reason_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "matched_field_converters",
    srcs = ["matched_field_converters.cc"],
    hdrs = ["matched_field_converters.h"],
    deps = [
        ":error_reason_converters",
        ":field_type_converters",
        ":status_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "matched_field_converters_test",
    size = "small",
    srcs = ["matched_field_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":matched_field_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "matched_data_record_converters",
    srcs = ["matched_data_record_converters.cc"],
    hdrs = ["matched_data_record_converters.h"],
    deps = [
        ":data_record_associated_data_converter",
        ":key_value_converters",
        ":matched_key_converters",
        "//cc/core/error:status_macros",
        "//cc/external:absl_status",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "matched_data_record_converters_test",
    size = "small",
    srcs = ["matched_data_record_converters_test.cc"],
    copts = [
        "-std=c++17",
    ],
    deps = [
        ":matched_data_record_converters",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)
