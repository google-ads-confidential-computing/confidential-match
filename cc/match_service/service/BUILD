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
    name = "match_service",
    srcs = ["match_service.cc"],
    hdrs = ["match_service.h"],
    deps = [
        "//cc/core/async",
        "//cc/core/logger",
        "//cc/external:absl_flat_hash_map",
        "//cc/external:absl_random",
        "//cc/external:absl_strings",
        "//cc/external:absl_time",
        "//cc/external:grpc++",
        "//cc/external:grpc++_alts",
        "//cc/external:scp_core_async_executor",
        "//cc/external:scp_core_errors_lib",
        "//cc/external:scp_core_execution_result",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_metric_client_interface",
        "//cc/match_service/converters:match_request_converters",
        "//cc/match_service/converters:match_response_converters",
        "//cc/match_service/error",
        "//cc/match_service/metrics:metrics_util",
        "//cc/match_service/tasks:match_task_interface",
        "//protos/match_service/api/v1:match_service_cc_grpc",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)

cc_test(
    name = "match_service_test",
    srcs = ["match_service_test.cc"],
    deps = [
        ":match_service",
        "//cc/core/async",
        "//cc/core/test/src:test_lib",
        "//cc/external:absl_status",
        "//cc/external:google_gtest_main",
        "//cc/external:grpc++",
        "//cc/external:scp_core_async_executor_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/external:scp_cpio_metric_client_mock",
        "//cc/match_service/tasks:mock_match_task",
        "//protos/match_service/api/v1:match_service_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
    ],
)
