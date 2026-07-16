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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//cc/match_service:__subpackages__"])

cc_library(
    name = "match_task_interface",
    hdrs = ["match_task_interface.h"],
    deps = [
        "//cc/core/async",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "match_task",
    srcs = ["match_task.cc"],
    hdrs = ["match_task.h"],
    deps = [
        ":hashed_match_task",
        ":kms_encrypted_match_task",
        ":match_task_interface",
        "//cc/core/async",
        "//cc/external:absl_log",
        "//cc/external:absl_status",
        "//cc/external:scp_core_async_executor",
        "//cc/match_service/error",
        "//protos/match_service/backend:error_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_task_test",
    srcs = ["match_task_test.cc"],
    deps = [
        ":match_task",
        ":mock_match_task",
        "//cc/core/async",
        "//cc/core/logger",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "mock_match_task",
    testonly = True,
    hdrs = ["mock_match_task.h"],
    deps = [
        ":match_task_interface",
        "//cc/core/async",
        "//cc/external:google_gtest",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "hashed_match_task",
    srcs = ["hashed_match_task.cc"],
    hdrs = ["hashed_match_task.h"],
    deps = [
        ":associated_data_collector",
        ":match_task_interface",
        ":normalization_utils",
        "//cc/common/proto_utils",
        "//cc/core/async",
        "//cc/core/error:status_macros",
        "//cc/core/hash:hasher_interface",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/match_service/converters:lookup_associated_data_converter",
        "//cc/match_service/error",
        "//cc/match_service/lookup_service_client:lookup_service_client_interface",
        "//cc/match_service/validators:match_key_encoding_validator",
        "//protos/match_service/backend:lookup_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "hashed_match_task_test",
    srcs = ["hashed_match_task_test.cc"],
    deps = [
        ":hashed_match_task",
        "//cc/core/async",
        "//cc/core/hash:mock_hasher",
        "//cc/core/logger",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_test_utils",
        "//cc/match_service/lookup_service_client:mock_lookup_service_client",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_library(
    name = "normalization_utils",
    srcs = ["normalization_utils.cc"],
    hdrs = ["normalization_utils.h"],
    deps = [
        "//cc/external:absl_strings",
    ],
)

cc_test(
    name = "normalization_utils_test",
    srcs = ["normalization_utils_test.cc"],
    deps = [
        ":normalization_utils",
        "//cc/external:absl_status",
        "//cc/external:google_gtest_main",
    ],
)

cc_library(
    name = "kms_encrypted_match_task",
    srcs = ["kms_encrypted_match_task.cc"],
    hdrs = ["kms_encrypted_match_task.h"],
    deps = [
        ":associated_data_collector",
        ":match_task_interface",
        ":normalization_utils",
        "//cc/core/async",
        "//cc/core/error:status_macros",
        "//cc/core/hash:hasher_interface",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_strings",
        "//cc/external:scp_cpio_metric_client_interface",
        "//cc/match_service/converters:encryption_key_converters",
        "//cc/match_service/converters:lookup_associated_data_converter",
        "//cc/match_service/crypto_client:crypto_client_interface",
        "//cc/match_service/error",
        "//cc/match_service/lookup_service_client:lookup_service_client_interface",
        "//cc/match_service/metrics:metrics_util",
        "//cc/match_service/validators:match_key_encoding_validator",
        "//protos/core:cc_proto",
        "//protos/match_service/backend:lookup_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
        "//protos/match_service/backend:private_key_endpoints_cc_proto",
    ],
)

cc_test(
    name = "kms_encrypted_match_task_test",
    srcs = ["kms_encrypted_match_task_test.cc"],
    deps = [
        ":kms_encrypted_match_task",
        "//cc/core/async",
        "//cc/core/hash:mock_hasher",
        "//cc/core/logger",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_bind_front",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_test_utils",
        "//cc/external:scp_cpio_metric_client_mock",
        "//cc/match_service/crypto_client:crypto_client_interface",
        "//cc/match_service/crypto_client:mock_crypto_client",
        "//cc/match_service/crypto_client:mock_crypto_key",
        "//cc/match_service/lookup_service_client:mock_lookup_service_client",
        "//protos/match_service/backend:match_service_cc_proto",
        "//protos/match_service/backend:private_key_endpoints_cc_proto",
    ],
)

cc_library(
    name = "associated_data_collector",
    srcs = ["associated_data_collector.cc"],
    hdrs = ["associated_data_collector.h"],
    deps = [
        "//cc/core/error:status_macros",
        "//cc/external:absl_statusor",
        "//cc/match_service/error",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "associated_data_collector_test",
    srcs = ["associated_data_collector_test.cc"],
    deps = [
        ":associated_data_collector",
        "//cc/core/logger",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:absl_status_matchers",
        "//cc/external:google_gtest_main",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_test_utils",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)
