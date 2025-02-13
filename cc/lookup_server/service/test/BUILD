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

load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//cc/lookup_server:__subpackages__"])

cc_test(
    name = "lookup_service_test",
    size = "small",
    srcs = glob(["*.cc"]),
    copts = [
        "-std=c++17",
    ],
    linkopts = [
        "-latomic",
    ],
    deps = [
        "//cc/core/test/src:test_lib",
        "//cc/external:google_gtest_main",
        "//cc/external:google_protobuf",
        "//cc/external:scp_core_async_executor_mock",
        "//cc/external:scp_core_execution_result_matchers",
        "//cc/external:scp_core_http2_server_mock",
        "//cc/external:scp_core_interface_lib",
        "//cc/external:scp_cpio_metric_client_mock",
        "//cc/external:scp_cpio_metric_instance_mock",
        "//cc/lookup_server/auth/src:auth",
        "//cc/lookup_server/coordinator_client/mock:coordinator_client_mock",
        "//cc/lookup_server/coordinator_client/src:coordinator_client",
        "//cc/lookup_server/crypto_client/mock:crypto_client_mock",
        "//cc/lookup_server/crypto_client/src:crypto_client",
        "//cc/lookup_server/interface:lookup_server_interface_lib",
        "//cc/lookup_server/kms_client/mock:kms_client_mock",
        "//cc/lookup_server/kms_client/src:kms_client",
        "//cc/lookup_server/match_data_storage/mock:match_data_storage_mock",
        "//cc/lookup_server/metric_client/mock:metric_client_mock",
        "//cc/lookup_server/service/mock:lookup_service_mock",
        "//cc/lookup_server/service/src:lookup_service_lib",
        "//protos/lookup_server/api:cc_proto",
    ],
)
