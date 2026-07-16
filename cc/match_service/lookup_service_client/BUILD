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

package(default_visibility = ["//cc:__subpackages__"])

cc_library(
    name = "lookup_service_client_interface",
    hdrs = [
        "lookup_service_client_interface.h",
    ],
    deps = [
        "//cc/core/async",
        "//cc/external:absl_status",
        "//protos/match_service/backend:lookup_cc_proto",
    ],
)

cc_library(
    name = "lookup_service_client",
    srcs = ["lookup_service_client.cc"],
    hdrs = ["lookup_service_client.h"],
    deps = [
        ":lookup_service_client_interface",
        "//cc/core/async",
        "//cc/core/util:jump_consistent_hasher",
        "//cc/external:absl_status",
        "//cc/external:absl_time",
        "//cc/external:scp_cpio_metric_client_interface",
        "//cc/match_service/converters:lookup_request_converter",
        "//cc/match_service/lookup_service_shard_client",
        "//cc/match_service/metrics:metrics_util",
        "//cc/match_service/orchestrator_client",
        "//protos/core:cc_proto",
        "//protos/lookup_server/api:cc_proto",
        "//protos/match_service/backend:lookup_cc_proto",
        "//protos/match_service/backend:match_service_cc_proto",
        "//protos/orchestrator/api:cc_proto",
    ],
)

cc_test(
    name = "lookup_service_client_test",
    size = "small",
    srcs = ["lookup_service_client_test.cc"],
    deps = [
        ":lookup_service_client",
        "//cc/core/async",
        "//cc/external:absl_status_matchers",
        "//cc/external:absl_time",
        "//cc/external:google_gtest_main",
        "//cc/external:scp_core_async_executor_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_core_test_utils",
        "//cc/external:scp_cpio_metric_client_mock",
        "//cc/match_service/lookup_service_shard_client:mock_lookup_service_shard_client",
        "//cc/match_service/orchestrator_client:mock_orchestrator_client",
        "//protos/match_service/backend:lookup_cc_proto",
    ],
)

cc_library(
    name = "mock_lookup_service_client",
    hdrs = ["mock_lookup_service_client.h"],
    deps = [
        ":lookup_service_client_interface",
        "//cc/core/async",
        "//cc/external:absl_status",
        "//cc/external:google_gtest",
        "//protos/match_service/backend:lookup_cc_proto",
    ],
)

cc_library(
    name = "fake_lookup_service_client",
    hdrs = ["fake_lookup_service_client.h"],
    deps = [
        ":lookup_service_client_interface",
        "//cc/core/async",
        "//cc/external:absl_status",
        "//protos/match_service/backend:lookup_cc_proto",
    ],
)
