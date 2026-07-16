# Copyright 2026 Google LLC
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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//cc:__subpackages__"])

cc_library(
    name = "kms_client",
    srcs = ["kms_client.cc"],
    hdrs = ["kms_client.h"],
    deps = [
        ":kms_client_interface",
        "//cc/core/async",
        "//cc/core/error:status_macros",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_kms_client_interface",
        "//cc/external:scp_cpio_kms_proto",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_library(
    name = "cached_kms_client",
    srcs = ["cached_kms_client.cc"],
    hdrs = ["cached_kms_client.h"],
    deps = [
        ":kms_client_interface",
        "//cc/core/async",
        "//cc/core/error:status_macros",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_str_format",
        "//cc/external:absl_strings",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_auto_expiry_concurrent_map_lib",
        "//cc/external:scp_core_interface_lib",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_test(
    name = "cached_kms_client_test",
    size = "small",
    srcs = ["cached_kms_client_test.cc"],
    deps = [
        ":cached_kms_client",
        ":kms_client",
        ":mock_kms_client",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_str_format",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_async_executor_mock",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)

cc_library(
    name = "fake_kms_client",
    hdrs = ["fake_kms_client.h"],
    deps = [
        ":kms_client_interface",
        "//cc/core/async",
        "//cc/external:absl_status",
        "//cc/external:scp_cpio_kms_proto",
    ],
)

cc_library(
    name = "mock_kms_client",
    hdrs = ["mock_kms_client.h"],
    deps = [
        ":kms_client_interface",
        "//cc/core/async",
        "//cc/external:absl_status",
        "//cc/external:google_gtest",
        "//cc/external:scp_cpio_kms_proto",
    ],
)

cc_library(
    name = "kms_client_interface",
    hdrs = ["kms_client_interface.h"],
    deps = [
        "//cc/core/async",
        "//cc/external:absl_status",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_kms_proto",
    ],
)

cc_test(
    name = "kms_client_test",
    size = "small",
    srcs = ["kms_client_test.cc"],
    deps = [
        ":kms_client",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_str_format",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_http2_client_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/external:scp_cpio_kms_client_interface",
        "//cc/external:scp_cpio_kms_client_mock",
        "//cc/external:scp_cpio_kms_proto",
        "//cc/match_service/error",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:error_cc_proto",
    ],
)
